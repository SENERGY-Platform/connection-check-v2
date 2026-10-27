/*
 * Copyright (c) 2023 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vernemq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
)

// initial row limit of a snapshot request; doubled when an answer fills it
const initialSnapshotLimit = 100000

// how often a truncated snapshot is requested again with a doubled limit before the refresh fails
const maxTruncationRetries = 3

const minRetryWait = time.Second

// answers subscription and client checks from a periodically refreshed snapshot of all online sessions,
// instead of one management api request per check: a filtered session/show scans every session and
// drops rows that exceed the broker's row timeout, which reads as a missing subscription
func New(config configuration.Config, metrics *prometheus.Metrics) (*Vernemq, error) {
	refreshInterval, err := time.ParseDuration(config.VerneSnapshotRefreshInterval)
	if err != nil {
		return nil, fmt.Errorf("parsing verne snapshot refresh interval failed: %w", err)
	}
	if refreshInterval <= 0 {
		return nil, errors.New("verne snapshot refresh interval must be positive")
	}
	maxAge, err := time.ParseDuration(config.VerneSnapshotMaxAge)
	if err != nil {
		return nil, fmt.Errorf("parsing verne snapshot max age failed: %w", err)
	}
	if maxAge < refreshInterval {
		return nil, errors.New("verne snapshot max age must not be shorter than the refresh interval")
	}
	rowTimeout, err := time.ParseDuration(config.VerneSnapshotRowTimeout)
	if err != nil {
		return nil, fmt.Errorf("parsing verne snapshot row timeout failed: %w", err)
	}
	requestTimeout, err := time.ParseDuration(config.HttpRequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("parsing http request timeout failed: %w", err)
	}
	return &Vernemq{
		apiUrl:          config.VerneManagementUrl,
		metrics:         metrics,
		config:          config,
		client:          http.Client{Timeout: requestTimeout},
		refreshInterval: refreshInterval,
		maxAge:          maxAge,
		rowTimeout:      rowTimeout,
		limit:           initialSnapshotLimit,
		now:             time.Now,
	}, nil
}

type Vernemq struct {
	apiUrl          string
	metrics         *prometheus.Metrics
	config          configuration.Config
	client          http.Client
	refreshInterval time.Duration
	maxAge          time.Duration
	rowTimeout      time.Duration
	limit           int
	now             func() time.Time

	mux      sync.RWMutex
	snapshot *snapshot
}

type snapshot struct {
	topics  map[string]bool
	clients map[string]bool
	created time.Time // when the request was sent; the broker state in the answer is not older than this
}

// fetches the first snapshot and keeps refreshing it in the background until ctx is done
func (this *Vernemq) Start(ctx context.Context, wg *sync.WaitGroup) error {
	err := this.refresh()
	if err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryWait := minRetryWait
		timer := time.NewTimer(this.refreshInterval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				err := this.refresh()
				if err != nil {
					this.config.GetLogger().Warn("unable to refresh vernemq subscription snapshot", "error", err, "retryIn", retryWait.String())
					timer.Reset(retryWait)
					retryWait = min(retryWait*2, this.refreshInterval)
					continue
				}
				retryWait = minRetryWait
				timer.Reset(this.refreshInterval)
			}
		}
	}()
	return nil
}

func (this *Vernemq) refresh() error {
	for attempt := 0; ; attempt++ {
		requested := this.now()
		table, err := this.fetch(this.limit)
		if err != nil {
			return err
		}
		if len(table) < this.limit {
			this.store(table, requested)
			return nil
		}
		if attempt >= maxTruncationRetries {
			return fmt.Errorf("vernemq subscription snapshot still truncated at limit %v", this.limit)
		}
		this.config.GetLogger().Warn("vernemq subscription snapshot truncated, retry with higher limit", "limit", this.limit, "newLimit", this.limit*2)
		this.limit = this.limit * 2
	}
}

func (this *Vernemq) fetch(limit int) (table []Subscription, err error) {
	return this.query("--topic&--client_id&--limit=" + strconv.Itoa(limit))
}

// sends one session/show request for online sessions; selection is the query part between is_online and rowtimeout
func (this *Vernemq) query(selection string) (table []Subscription, err error) {
	path := "/api/v1/session/show?--is_online=true&" + selection + "&--rowtimeout=" + strconv.FormatInt(this.rowTimeout.Milliseconds(), 10)
	req, err := http.NewRequest("GET", this.apiUrl+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := this.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		buf, _ := io.ReadAll(resp.Body)
		return nil, errors.New(resp.Status + ":" + string(buf))
	}
	temp := SubscriptionWrapper{}
	err = json.NewDecoder(resp.Body).Decode(&temp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal vernemq session list: %w", err)
	}
	return temp.Table, nil
}

func (this *Vernemq) store(table []Subscription, requested time.Time) {
	next := &snapshot{
		topics:  map[string]bool{},
		clients: map[string]bool{},
		created: requested,
	}
	for _, row := range table {
		if row.Topic != "" {
			next.topics[row.Topic] = true
		}
		if row.ClientId != "" {
			next.clients[row.ClientId] = true
		}
	}
	this.mux.Lock()
	defer this.mux.Unlock()
	this.snapshot = next
}

func (this *Vernemq) getSnapshot() (*snapshot, error) {
	this.mux.RLock()
	defer this.mux.RUnlock()
	if this.snapshot == nil {
		return nil, errors.New("no vernemq subscription snapshot available")
	}
	age := this.now().Sub(this.snapshot.created)
	if age > this.maxAge {
		return nil, fmt.Errorf("vernemq subscription snapshot is %v old, exceeds max age of %v", age.Round(time.Second), this.maxAge)
	}
	return this.snapshot, nil
}

func (this *Vernemq) CheckTopic(topic string) (result bool, err error) {
	if this.config.Debug {
		defer func() {
			this.config.GetLogger().Debug("check topic", "topic", topic, "result", result, "error", err)
		}()
	}
	this.metrics.TopicsChecked.Inc()
	start := time.Now()
	s, err := this.getSnapshot()
	this.metrics.TopicCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	if err != nil {
		return false, err
	}
	return s.topics[topic], nil
}

type SubscriptionWrapper struct {
	Table []Subscription `json:"table"`
}

type Subscription struct {
	ClientId string `json:"client_id"`
	User     string `json:"user"`
	Topic    string `json:"topic"`
	IsOnline bool   `json:"is_online"`
}

func (this *Vernemq) CheckClient(clientId string) (onlineClientExists bool, err error) {
	this.metrics.ClientsChecked.Inc()
	start := time.Now()
	s, err := this.getSnapshot()
	this.metrics.ClientCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	if err != nil {
		return false, err
	}
	return s.clients[clientId], nil
}

// asks the broker directly whether an online session subscribes to topic; leaves the snapshot untouched
func (this *Vernemq) ConfirmTopic(topic string) (bool, error) {
	this.metrics.TopicsChecked.Inc()
	table, err := this.query("--topic=" + url.QueryEscape(topic) + "&--limit=1")
	if err != nil {
		return false, err
	}
	return len(table) > 0, nil
}

// asks the broker directly whether clientId has an online session; leaves the snapshot untouched
func (this *Vernemq) ConfirmClient(clientId string) (bool, error) {
	this.metrics.ClientsChecked.Inc()
	table, err := this.query("--client_id=" + url.QueryEscape(clientId) + "&--limit=1")
	if err != nil {
		return false, err
	}
	return len(table) > 0, nil
}
