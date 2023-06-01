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
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/tests/docker"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestVernemqApi(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokerUrl, managementUrl, err := docker.VernemqttErlio(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	knownTopics, err := connectClient(ctx, wg, brokerUrl, "testclient", 2)
	if err != nil {
		t.Error(err)
		return
	}

	metrics := prometheus.NewMetrics("test")

	verne := New(configuration.Config{
		Debug:              true,
		VerneManagementUrl: managementUrl,
	}, metrics)

	t.Run("check known", func(t *testing.T) {
		for _, topic := range knownTopics {
			result, err := verne.CheckTopic(topic)
			if err != nil {
				t.Error(err)
				return
			}
			if !result {
				t.Error(result)
				return
			}
		}
	})

	t.Run("check unknown", func(t *testing.T) {
		result, err := verne.CheckTopic("unknown")
		if err != nil {
			t.Error(err)
			return
		}
		if result {
			t.Error(result)
			return
		}
	})
}

func connectClient(ctx context.Context, wg *sync.WaitGroup, brokerUrl string, clientId string, subscriptionCount int) (knownTopics []string, err error) {
	log.Println("connect client")
	options := paho.NewClientOptions().
		SetClientID(clientId).
		SetUsername("test").
		SetPassword("test").
		SetAutoReconnect(true).
		SetCleanSession(false).
		AddBroker(brokerUrl)

	mqtt := paho.NewClient(options)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Client.Connect(): ", token.Error())
		return nil, token.Error()
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		time.Sleep(1 * time.Second)
		mqtt.Disconnect(0)
		wg.Done()
	}()

	for i := 0; i < subscriptionCount; i++ {
		topic := clientId + "/" + strconv.Itoa(i)
		token := mqtt.Subscribe(topic, 2, func(client paho.Client, message paho.Message) {})
		if token.Wait() && token.Error() != nil {
			return nil, token.Error()
		}
		knownTopics = append(knownTopics, topic)
	}

	return knownTopics, nil
}
