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

package tests

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/connectionlog"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/tests/docker"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/worker"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/permission-search/lib/client"
	permmodel "github.com/SENERGY-Platform/permission-search/lib/model"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMqttDeviceLoop(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := configuration.Config{
		Debug:                             true,
		TopicGenerator:                    "mqtt",
		HandledProtocols:                  []string{"urn:infai:ses:protocol:0"},
		DeviceTypeCacheExpiration:         "30m",
		MaxDeviceAge:                      "10s",
		PermissionsRequestDeviceBatchSize: 50,
		DeviceCheckInterval:               "100ms",
		DeviceConnectionLogTopic:          "device_log",
		HubConnectionLogTopic:             "gateway_log",
		HubCheckInterval:                  "-",
		MaxHubAge:                         "10s",
		PermissionsRequestHubBatchSize:    11,
		HubProtocolCheckCacheExpiration:   "1h",
	}

	var err error

	config.DeviceRepositoryUrl, config.PermissionSearchUrl, config.KafkaUrl, err = docker.DeviceRepoWithDependencies(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	err = createDummyMqttLikeDeviceTypes(config)
	if err != nil {
		t.Error(err)
		return
	}

	err = createDummyMqttDevices(config)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	logger, err := connectionlog.New(ctx, wg, config)
	if err != nil {
		t.Error(err)
		return
	}

	mock := &Mock{
		CheckTopicF: func(topic string) bool {
			return strings.HasSuffix(topic, "/+")
		},
	}
	deviceTypeProvider, err := providers.NewDeviceTypeProvider(config, mock)
	if err != nil {
		t.Error(err)
		return
	}
	deviceProvider, err := providers.NewDeviceProvider(config, mock, deviceTypeProvider)
	if err != nil {
		t.Error(err)
		return
	}
	hubProvider, err := providers.NewHubProvider(config, mock, deviceTypeProvider)
	if err != nil {
		t.Error(err)
		return
	}
	w, err := worker.New(config, logger, deviceProvider, hubProvider, deviceTypeProvider, mock)
	if err != nil {
		t.Error(err)
		return
	}
	err = w.RunDeviceLoop(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(2 * time.Minute)

	mock.Mutex.Lock()
	defer mock.Mutex.Unlock()

	if len(mock.CheckTopicCalls) < 200 {
		t.Errorf("%#v", len(mock.CheckTopicCalls))
		return
	}

	permissions := client.NewClient(config.PermissionSearchUrl)
	senergyLikeDevices, err := client.List[[]model.PermDevice](permissions, TestToken, "devices", permmodel.ListOptions{
		QueryListCommons: permmodel.QueryListCommons{
			Limit:  200,
			Rights: "r",
			SortBy: "local_id",
		},
		Selection: &permmodel.FeatureSelection{
			Feature: "device_type_id",
			Value:   "urn:infai:ses:device-type:0",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if len(senergyLikeDevices) != 200 {
		t.Errorf("%#v", senergyLikeDevices)
		return
	}
	for _, device := range senergyLikeDevices {
		if !reflect.DeepEqual(device.Annotations[worker.ConnectionStateAnnotation], true) {
			t.Errorf("%#v", device)
			return
		}
	}

	noneSenergyLikeDevices, err := client.List[[]model.PermDevice](permissions, TestToken, "devices", permmodel.ListOptions{
		QueryListCommons: permmodel.QueryListCommons{
			Limit:  200,
			Rights: "r",
			SortBy: "local_id",
		},
		Selection: &permmodel.FeatureSelection{
			Feature: "device_type_id",
			Value:   "urn:infai:ses:device-type:1",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if len(noneSenergyLikeDevices) != 200 {
		t.Errorf("%#v", noneSenergyLikeDevices)
		return
	}
	for _, device := range noneSenergyLikeDevices {
		if _, ok := device.Annotations[worker.ConnectionStateAnnotation]; ok {
			t.Errorf("expected no anotation: %#v", device)
			return
		}
	}
}

func createDummyMqttDevices(config configuration.Config) error {
	writer := kafka.Writer{
		Addr:        kafka.TCP(config.KafkaUrl),
		Topic:       "devices",
		MaxAttempts: 10,
		BatchSize:   1,
		Balancer:    &kafka.Hash{},
	}
	defer writer.Close()
	for i := 0; i < 2; i++ {
		for j := 0; j < 200; j++ {
			devicetypeindex := strconv.Itoa(i)
			deviceindex := devicetypeindex + "-" + strconv.Itoa(j)
			id := "urn:infai:ses:device:" + uuid.NewString()
			b, err := json.Marshal(map[string]interface{}{
				"command": "PUT",
				"id":      id,
				"owner":   "dd69ea0d-f553-4336-80f3-7f4567f85c7b",
				"device": models.Device{
					Id:           id,
					LocalId:      "lid-" + deviceindex,
					Name:         deviceindex,
					DeviceTypeId: "urn:infai:ses:device-type:" + devicetypeindex,
				},
			})
			if err != nil {
				return err
			}
			err = writer.WriteMessages(
				context.Background(),
				kafka.Message{
					Key:   []byte(id),
					Value: b,
					Time:  time.Now(),
				},
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func createDummyMqttLikeDeviceTypes(config configuration.Config) error {
	writer := kafka.Writer{
		Addr:        kafka.TCP(config.KafkaUrl),
		Topic:       "device-types",
		MaxAttempts: 10,
		BatchSize:   1,
		Balancer:    &kafka.Hash{},
	}
	defer writer.Close()
	for i := 0; i < 2; i++ {
		indexstr := strconv.Itoa(i)
		id := "urn:infai:ses:device-type:" + indexstr
		b, err := json.Marshal(map[string]interface{}{
			"command": "PUT",
			"id":      id,
			"owner":   "dd69ea0d-f553-4336-80f3-7f4567f85c7b",
			"device_type": models.DeviceType{
				Id:   id,
				Name: indexstr,
				Services: []models.Service{
					{
						Id:          "urn:infai:ses:service:" + indexstr,
						LocalId:     "{{.DeviceId}}/" + indexstr,
						Name:        indexstr,
						Interaction: models.EVENT_AND_REQUEST,
						ProtocolId:  "urn:infai:ses:protocol:" + indexstr,
					},
				},
			},
		})
		if err != nil {
			return err
		}
		err = writer.WriteMessages(
			context.Background(),
			kafka.Message{
				Key:   []byte(id),
				Value: b,
				Time:  time.Now(),
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}
