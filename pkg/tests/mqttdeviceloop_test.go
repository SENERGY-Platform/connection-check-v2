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
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/connectionlog"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/tests/docker"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/worker"
	"github.com/SENERGY-Platform/device-repository/lib/client"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/google/uuid"
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
		PermissionsRequestDeviceBatchSize: 50,
		DeviceCheckInterval:               "100ms",
		DeviceConnectionLogTopic:          "device_log",
		HubConnectionLogTopic:             "gateway_log",
		HubCheckInterval:                  "-",
		PermissionsRequestHubBatchSize:    11,
		HubProtocolCheckCacheExpiration:   "1h",
		MaxErrorCountTilFatal:             -1,
		HttpRequestTimeout:                "30s",
	}

	var err error

	config.DeviceRepositoryUrl, config.KafkaUrl, err = docker.DeviceRepoWithDependencies(ctx, wg)
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

	metrics, err := prometheus.Start(ctx, config)
	if err != nil {
		t.Error(err)
		return
	}

	logger, err := connectionlog.New(ctx, wg, config, metrics)
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
	lmClient, err := providers.NewLastMessageClient(config, mock)
	if err != nil {
		t.Error(err)
		return
	}
	lmProvider := providers.NewLastMessageStateProvider(lmClient, false)
	w, err := worker.New(config, logger, deviceProvider, hubProvider, deviceTypeProvider, lmProvider, mock, metrics, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = w.RunDeviceLoop(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	err = waitUntil(3*time.Minute, func() error {
		return checkMqttDeviceLoopResult(config, mock)
	})
	if err != nil {
		t.Error(err)
	}
}

func checkMqttDeviceLoopResult(config configuration.Config, mock *Mock) error {
	mock.Mutex.Lock()
	checkTopicCallCount := len(mock.CheckTopicCalls)
	mock.Mutex.Unlock()

	if checkTopicCallCount < 200 {
		return fmt.Errorf("%#v", checkTopicCallCount)
	}

	deviceRepo := client.NewClient(config.DeviceRepositoryUrl, nil)
	senergyLikeDevices, _, err, _ := deviceRepo.ListExtendedDevices(TestToken, client.ExtendedDeviceListOptions{
		DeviceTypeIds: []string{"urn:infai:ses:device-type:0"},
		Limit:         200,
	})
	if err != nil {
		return err
	}
	if len(senergyLikeDevices) != 200 {
		return fmt.Errorf("%#v", senergyLikeDevices)
	}
	for i, device := range senergyLikeDevices {
		if !reflect.DeepEqual(device.ConnectionState, models.ConnectionStateOnline) {
			return fmt.Errorf("%v %#v", i, device)
		}
	}

	noneSenergyLikeDevices, _, err, _ := deviceRepo.ListExtendedDevices(TestToken, client.ExtendedDeviceListOptions{
		DeviceTypeIds: []string{"urn:infai:ses:device-type:1"},
		Limit:         200,
	})
	if err != nil {
		return err
	}
	if len(noneSenergyLikeDevices) != 200 {
		return fmt.Errorf("%#v", noneSenergyLikeDevices)
	}
	for _, device := range noneSenergyLikeDevices {
		if device.ConnectionState != models.ConnectionStateUnknown {
			return fmt.Errorf("expected no anotation: %#v", device)
		}
	}
	return nil
}

func TestMqttDeviceProviderWithoutMqttDevices(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := configuration.Config{
		Debug:                             true,
		TopicGenerator:                    "mqtt",
		HandledProtocols:                  []string{"urn:infai:ses:protocol:5"},
		DeviceTypeCacheExpiration:         "30m",
		PermissionsRequestDeviceBatchSize: 50,
		DeviceCheckInterval:               "100ms",
		DeviceConnectionLogTopic:          "device_log",
		HubConnectionLogTopic:             "gateway_log",
		HubCheckInterval:                  "-",
		PermissionsRequestHubBatchSize:    11,
		HubProtocolCheckCacheExpiration:   "1h",
		MaxErrorCountTilFatal:             -1,
	}

	var err error

	config.DeviceRepositoryUrl, config.KafkaUrl, err = docker.DeviceRepoWithDependencies(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	err = createDummySenergylikeDeviceTypes(config)
	if err != nil {
		t.Error(err)
		return
	}

	err = createDummyDevices(config)
	if err != nil {
		t.Error(err)
		return
	}

	err = createDummyHubs(config)
	if err != nil {
		t.Error(err)
		return
	}

	err = waitUntil(time.Minute, func() error {
		return checkDummyDevicesAndHubsListed(config)
	})
	if err != nil {
		t.Error(err)
		return
	}

	mock := &Mock{}
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

	_, _, err = deviceProvider.GetNextDevice()
	if !errors.Is(err, providers.BatchNoMatchAfterMultipleResets) {
		t.Error(err)
	}
}

func createDummyMqttDevices(config configuration.Config) error {
	c := client.NewClient(config.DeviceRepositoryUrl, nil)
	for i := 0; i < 2; i++ {
		for j := 0; j < 200; j++ {
			devicetypeindex := strconv.Itoa(i)
			deviceindex := devicetypeindex + "-" + strconv.Itoa(j)
			id := "urn:infai:ses:device:" + uuid.NewString()
			_, err, _ := c.SetDevice(TestToken, models.Device{
				Id:           id,
				LocalId:      "lid-" + deviceindex,
				Name:         deviceindex,
				DeviceTypeId: "urn:infai:ses:device-type:" + devicetypeindex,
				//OwnerId:      "dd69ea0d-f553-4336-80f3-7f4567f85c7b",
			}, client.DeviceUpdateOptions{})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func createDummyMqttLikeDeviceTypes(config configuration.Config) error {
	c := client.NewClient(config.DeviceRepositoryUrl, nil)
	for i := 0; i < 2; i++ {
		indexstr := strconv.Itoa(i)
		id := "urn:infai:ses:device-type:" + indexstr
		_, err, _ := c.SetDeviceType(client.InternalAdminToken, models.DeviceType{
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
		}, client.DeviceTypeUpdateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
