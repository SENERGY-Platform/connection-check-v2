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
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"time"
)

func New(config configuration.Config, metrics *prometheus.Metrics) *Vernemq {
	return &Vernemq{apiUrl: config.VerneManagementUrl, metrics: metrics, config: config}
}

type Vernemq struct {
	apiUrl  string
	metrics *prometheus.Metrics
	config  configuration.Config
}

func (this *Vernemq) CheckTopic(topic string) (result bool, err error) {
	if this.config.Debug {
		defer func() {
			log.Println("DEBUG: check topic", topic, result, err)
		}()
	}
	this.metrics.TopicsChecked.Inc()
	path := "/api/v1/session/show?--is_online=true&--topic=" + url.QueryEscape(topic) + "&--limit=1"
	req, err := http.NewRequest("GET", this.apiUrl+path, nil)
	if err != nil {
		debug.PrintStack()
		return false, err
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	this.metrics.TopicCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	if err != nil {
		debug.PrintStack()
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		buf, _ := io.ReadAll(resp.Body)
		err = errors.New(resp.Status + ":" + string(buf))
		log.Println("ERROR: unable to get result from vernemq for device connection check", err)
		return false, err
	}
	temp := SubscriptionWrapper{}
	err = json.NewDecoder(resp.Body).Decode(&temp)
	if err != nil {
		log.Println("ERROR: unable to unmarshal result of device connection check")
		return false, err
	}
	return len(temp.Table) > 0, nil
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
	path := "/api/v1/session/show?--is_online=true&--client_id=" + url.QueryEscape(clientId) + "&--limit=1"
	req, err := http.NewRequest("GET", this.apiUrl+path, nil)
	if err != nil {
		debug.PrintStack()
		return false, err
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	this.metrics.ClientCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	if err != nil {
		debug.PrintStack()
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		buf, _ := io.ReadAll(resp.Body)
		err = errors.New(resp.Status + ":" + string(buf))
		log.Println("ERROR: unable to get result from vernemq", err)
		return false, err
	}
	temp := SubscriptionWrapper{}
	err = json.NewDecoder(resp.Body).Decode(&temp)
	if err != nil {
		log.Println("ERROR: unable to unmarshal result of", this.apiUrl+path)
		return false, err
	}
	return len(temp.Table) > 0, nil
}
