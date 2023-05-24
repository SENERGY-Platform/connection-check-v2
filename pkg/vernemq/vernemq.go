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
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
)

func New(config configuration.Config) *Vernemq {
	return &Vernemq{apiUrl: config.VerneManagementUrl}
}

type Vernemq struct {
	apiUrl string
}

func (this *Vernemq) CheckTopic(topic string) (result bool, err error) {
	return checkTopic(this.apiUrl, topic)
}

func checkTopic(apiUrl string, topic string) (result bool, err error) {
	path := "/api/v1/session/show?--is_online=true&--topic=" + url.QueryEscape(topic) + "&--limit=1"
	req, err := http.NewRequest("GET", apiUrl+path, nil)
	if err != nil {
		debug.PrintStack()
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
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
		log.Println("ERROR: unable to unmarshal result of", apiUrl+path)
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
