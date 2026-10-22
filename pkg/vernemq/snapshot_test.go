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
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
)

type fakeBroker struct {
	mux      sync.Mutex
	table    []Subscription
	fail     bool
	requests []http.Request
}

func (this *fakeBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.requests = append(this.requests, *r)
	if this.fail {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	table := this.table
	limit, err := strconv.Atoi(r.URL.Query().Get("--limit"))
	if err == nil && len(table) > limit {
		table = table[:limit]
	}
	_ = json.NewEncoder(w).Encode(SubscriptionWrapper{Table: table})
}

func (this *fakeBroker) set(table []Subscription, fail bool) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.table = table
	this.fail = fail
}

func newTestVerne(t *testing.T, broker *fakeBroker) (*Vernemq, *time.Time) {
	server := httptest.NewServer(broker)
	t.Cleanup(server.Close)
	verne, err := New(configuration.Config{
		VerneManagementUrl:           server.URL,
		VerneSnapshotRefreshInterval: "30s",
		VerneSnapshotMaxAge:          "2m",
		VerneSnapshotRowTimeout:      "5s",
		HttpRequestTimeout:           "10s",
	}, prometheus.NewMetrics("test"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)
	verne.now = func() time.Time { return now }
	return verne, &now
}

func TestSnapshotAnswersTopicAndClientChecksFromOneRequest(t *testing.T) {
	broker := &fakeBroker{table: []Subscription{
		{ClientId: "gateway-1", Topic: "command/owner/zigbee:0x1/+"},
		{ClientId: "gateway-1", Topic: "command/zigbee:0x1/+"},
		{ClientId: "gateway-2", Topic: "command/owner/zigbee:0x2/+"},
	}}
	verne, _ := newTestVerne(t, broker)
	if err := verne.refresh(); err != nil {
		t.Fatal(err)
	}

	for topic, expected := range map[string]bool{
		"command/owner/zigbee:0x1/+": true,
		"command/owner/zigbee:0x2/+": true,
		"command/owner/zigbee:0x3/+": false,
		"command/owner/zigbee:0x1/#": false,
	} {
		result, err := verne.CheckTopic(topic)
		if err != nil {
			t.Fatal(err)
		}
		if result != expected {
			t.Errorf("CheckTopic(%q) = %v, expected %v", topic, result, expected)
		}
	}
	for clientId, expected := range map[string]bool{"gateway-1": true, "gateway-2": true, "gateway-3": false} {
		result, err := verne.CheckClient(clientId)
		if err != nil {
			t.Fatal(err)
		}
		if result != expected {
			t.Errorf("CheckClient(%q) = %v, expected %v", clientId, result, expected)
		}
	}
	if len(broker.requests) != 1 {
		t.Errorf("expected one request to the broker, got %v", len(broker.requests))
	}
}

func TestSnapshotRequestSelectsOnlineSubscriptionsWithRowTimeout(t *testing.T) {
	broker := &fakeBroker{}
	verne, _ := newTestVerne(t, broker)
	if err := verne.refresh(); err != nil {
		t.Fatal(err)
	}
	query := broker.requests[0].URL.Query()
	for key, expected := range map[string]string{
		"--is_online":  "true",
		"--topic":      "",
		"--client_id":  "",
		"--limit":      strconv.Itoa(initialSnapshotLimit),
		"--rowtimeout": "5000",
	} {
		if !query.Has(key) || query.Get(key) != expected {
			t.Errorf("query parameter %v = %q, expected %q (query: %v)", key, query.Get(key), expected, broker.requests[0].URL.RawQuery)
		}
	}
}

func TestSnapshotRetriesTruncatedAnswerWithHigherLimit(t *testing.T) {
	broker := &fakeBroker{table: []Subscription{
		{ClientId: "c1", Topic: "t1"},
		{ClientId: "c2", Topic: "t2"},
		{ClientId: "c3", Topic: "t3"},
	}}
	verne, _ := newTestVerne(t, broker)
	verne.limit = 2
	if err := verne.refresh(); err != nil {
		t.Fatal(err)
	}
	result, err := verne.CheckTopic("t3")
	if err != nil {
		t.Fatal(err)
	}
	if !result {
		t.Error("expected the row beyond the first limit to be found after the retry")
	}
	if len(broker.requests) != 2 {
		t.Errorf("expected two requests, got %v", len(broker.requests))
	}
	if verne.limit != 4 {
		t.Errorf("expected the raised limit to be kept for later refreshes, got %v", verne.limit)
	}
}

func TestSnapshotFailsWhenStillTruncatedAfterRetries(t *testing.T) {
	table := []Subscription{}
	for i := 0; i < 100; i++ {
		table = append(table, Subscription{ClientId: "c", Topic: "t" + strconv.Itoa(i)})
	}
	broker := &fakeBroker{table: table}
	verne, _ := newTestVerne(t, broker)
	verne.limit = 2
	if err := verne.refresh(); err == nil {
		t.Fatal("expected an error")
	}
	if len(broker.requests) != maxTruncationRetries+1 {
		t.Errorf("expected %v requests, got %v", maxTruncationRetries+1, len(broker.requests))
	}
	if _, err := verne.CheckTopic("t0"); err == nil {
		t.Error("expected checks to fail without a complete snapshot")
	}
}

func TestSnapshotFailedRefreshKeepsPreviousSnapshotUntilMaxAge(t *testing.T) {
	broker := &fakeBroker{table: []Subscription{{ClientId: "c1", Topic: "t1"}}}
	verne, now := newTestVerne(t, broker)
	if err := verne.refresh(); err != nil {
		t.Fatal(err)
	}
	broker.set(nil, true)
	if err := verne.refresh(); err == nil {
		t.Fatal("expected the refresh to fail")
	}

	*now = now.Add(2 * time.Minute)
	result, err := verne.CheckTopic("t1")
	if err != nil {
		t.Fatal(err)
	}
	if !result {
		t.Error("expected the previous snapshot to answer within max age")
	}

	*now = now.Add(time.Second)
	if _, err := verne.CheckTopic("t1"); err == nil {
		t.Error("expected an error once the snapshot exceeds max age")
	}
	if _, err := verne.CheckClient("c1"); err == nil {
		t.Error("expected an error once the snapshot exceeds max age")
	}
}

func TestSnapshotChecksFailBeforeFirstSnapshot(t *testing.T) {
	verne, _ := newTestVerne(t, &fakeBroker{})
	if _, err := verne.CheckTopic("t1"); err == nil {
		t.Error("expected an error")
	}
	if _, err := verne.CheckClient("c1"); err == nil {
		t.Error("expected an error")
	}
}

func TestStartFailsWhenFirstSnapshotFails(t *testing.T) {
	broker := &fakeBroker{fail: true}
	verne, _ := newTestVerne(t, broker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := verne.Start(ctx, &sync.WaitGroup{}); err == nil {
		t.Error("expected an error")
	}
}

func TestNewRejectsMaxAgeShorterThanRefreshInterval(t *testing.T) {
	_, err := New(configuration.Config{
		VerneSnapshotRefreshInterval: "1m",
		VerneSnapshotMaxAge:          "30s",
		VerneSnapshotRowTimeout:      "5s",
		HttpRequestTimeout:           "10s",
	}, prometheus.NewMetrics("test"))
	if err == nil {
		t.Error("expected an error")
	}
}
