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
	"github.com/SENERGY-Platform/connection-check-v2/pkg/deviceprovider"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/devicetypes"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/tests/docker"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/worker"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/permission-search/lib/client"
	permmodel "github.com/SENERGY-Platform/permission-search/lib/model"
	"github.com/segmentio/kafka-go"
	"golang.org/x/exp/slices"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSenergyDeviceLoop(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := configuration.Config{
		Debug:                       true,
		TopicGenerator:              "senergy",
		HandledProtocols:            []string{"urn:infai:ses:protocol:0"},
		DeviceTypeCacheExpiration:   "30m",
		MaxDeviceAge:                "10s",
		PermissionsRequestBatchSize: 50,
		DeviceCheckInterval:         "100ms",
		DeviceConnectionLogTopic:    "device_log",
		HubConnectionLogTopic:       "gateway_log",
	}

	var err error

	config.DeviceRepositoryUrl, config.PermissionSearchUrl, config.KafkaUrl, err = docker.DeviceRepoWithDependencies(ctx, wg)
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

	time.Sleep(10 * time.Second)

	err = sendInitialConnectionStates(config)
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
			parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(topic, "command/lid-"), "/+"), "-")
			if len(parts) != 2 {
				return false
			}
			num, err := strconv.Atoi(parts[1])
			if err != nil {
				log.Println("ERROR:", err)
				return false
			}
			return num%2 == 0
		},
	}
	deviceTypeProvider, err := devicetypes.New(config, mock)
	if err != nil {
		t.Error(err)
		return
	}
	deviceProvider, err := deviceprovider.New(config, mock, deviceTypeProvider)
	if err != nil {
		t.Error(err)
		return
	}
	w, err := worker.New(config, logger, deviceProvider, deviceTypeProvider, mock)
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

	mock.CheckTopicCallsMux.Lock()
	defer mock.CheckTopicCallsMux.Unlock()

	if slices.Contains(mock.CheckTopicCalls, "command/lid-1-0/+") {
		t.Errorf("%#v", mock.CheckTopicCalls)
		return
	}
	if !slices.Contains(mock.CheckTopicCalls, "command/lid-0-98/+") {
		t.Errorf("%#v", mock.CheckTopicCalls)
		return
	}
	if !slices.Contains(mock.CheckTopicCalls, "command/lid-0-99/+") {
		t.Errorf("%#v", mock.CheckTopicCalls)
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
		parts := strings.Split(strings.TrimPrefix(device.LocalId, "lid-"), "-")
		if len(parts) != 2 {
			t.Errorf("%#v", parts)
			return
		}
		num, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Error(err)
			return
		}
		expected := num%2 == 0
		if !reflect.DeepEqual(device.Annotations[worker.ConnectionStateAnnotation], expected) {
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

func sendInitialConnectionStates(config configuration.Config) error {
	writer := kafka.Writer{
		Addr:        kafka.TCP(config.KafkaUrl),
		Topic:       config.DeviceConnectionLogTopic,
		MaxAttempts: 10,
		BatchSize:   1,
		Balancer:    &kafka.Hash{},
	}
	defer writer.Close()
	for i := 0; i < 200; i++ {
		deviceindex := strconv.Itoa(i)
		id := "urn:infai:ses:device:0-" + deviceindex
		b, err := json.Marshal(connectionlog.DeviceLog{
			Id:        id,
			Connected: i < 100,
			Time:      time.Now(),
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

func createDummyDevices(config configuration.Config) error {
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
			id := "urn:infai:ses:device:" + deviceindex
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

func createDummySenergylikeDeviceTypes(config configuration.Config) error {
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
						LocalId:     indexstr,
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

type Mock struct {
	CheckTopicF        func(topic string) bool
	CheckTopicCalls    []string
	CheckTopicCallsMux sync.Mutex
}

const TestToken = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIzaUtabW9aUHpsMmRtQnBJdS1vSkY4ZVVUZHh4OUFIckVOcG5CcHM5SjYwIn0.eyJqdGkiOiJiOGUyNGZkNy1jNjJlLTRhNWQtOTQ4ZC1mZGI2ZWVkM2JmYzYiLCJleHAiOjE1MzA1MzIwMzIsIm5iZiI6MCwiaWF0IjoxNTMwNTI4NDMyLCJpc3MiOiJodHRwczovL2F1dGguc2VwbC5pbmZhaS5vcmcvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoiZnJvbnRlbmQiLCJzdWIiOiJkZDY5ZWEwZC1mNTUzLTQzMzYtODBmMy03ZjQ1NjdmODVjN2IiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJmcm9udGVuZCIsIm5vbmNlIjoiMjJlMGVjZjgtZjhhMS00NDQ1LWFmMjctNGQ1M2JmNWQxOGI5IiwiYXV0aF90aW1lIjoxNTMwNTI4NDIzLCJzZXNzaW9uX3N0YXRlIjoiMWQ3NWE5ODQtNzM1OS00MWJlLTgxYjktNzMyZDgyNzRjMjNlIiwiYWNyIjoiMCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJjcmVhdGUtcmVhbG0iLCJhZG1pbiIsImRldmVsb3BlciIsInVtYV9hdXRob3JpemF0aW9uIiwidXNlciJdfSwicmVzb3VyY2VfYWNjZXNzIjp7Im1hc3Rlci1yZWFsbSI6eyJyb2xlcyI6WyJ2aWV3LWlkZW50aXR5LXByb3ZpZGVycyIsInZpZXctcmVhbG0iLCJtYW5hZ2UtaWRlbnRpdHktcHJvdmlkZXJzIiwiaW1wZXJzb25hdGlvbiIsImNyZWF0ZS1jbGllbnQiLCJtYW5hZ2UtdXNlcnMiLCJxdWVyeS1yZWFsbXMiLCJ2aWV3LWF1dGhvcml6YXRpb24iLCJxdWVyeS1jbGllbnRzIiwicXVlcnktdXNlcnMiLCJtYW5hZ2UtZXZlbnRzIiwibWFuYWdlLXJlYWxtIiwidmlldy1ldmVudHMiLCJ2aWV3LXVzZXJzIiwidmlldy1jbGllbnRzIiwibWFuYWdlLWF1dGhvcml6YXRpb24iLCJtYW5hZ2UtY2xpZW50cyIsInF1ZXJ5LWdyb3VwcyJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwicm9sZXMiOlsidW1hX2F1dGhvcml6YXRpb24iLCJhZG1pbiIsImNyZWF0ZS1yZWFsbSIsImRldmVsb3BlciIsInVzZXIiLCJvZmZsaW5lX2FjY2VzcyJdLCJuYW1lIjoiZGYgZGZmZmYiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXBsIiwiZ2l2ZW5fbmFtZSI6ImRmIiwiZmFtaWx5X25hbWUiOiJkZmZmZiIsImVtYWlsIjoic2VwbEBzZXBsLmRlIn0.eOwKV7vwRrWr8GlfCPFSq5WwR_p-_rSJURXCV1K7ClBY5jqKQkCsRL2V4YhkP1uS6ECeSxF7NNOLmElVLeFyAkvgSNOUkiuIWQpMTakNKynyRfH0SrdnPSTwK2V1s1i4VjoYdyZWXKNjeT2tUUX9eCyI5qOf_Dzcai5FhGCSUeKpV0ScUj5lKrn56aamlW9IdmbFJ4VwpQg2Y843Vc0TqpjK9n_uKwuRcQd9jkKHkbwWQ-wyJEbFWXHjQ6LnM84H0CQ2fgBqPPfpQDKjGSUNaCS-jtBcbsBAWQSICwol95BuOAqVFMucx56Wm-OyQOuoQ1jaLt2t-Uxtr-C9wKJWHQ"

func (this *Mock) Access() (token string, err error) {
	return TestToken, nil
}

func (this *Mock) CheckTopic(topic string) (result bool, err error) {
	log.Println("CheckTopic", topic)
	this.CheckTopicCallsMux.Lock()
	defer this.CheckTopicCallsMux.Unlock()
	this.CheckTopicCalls = append(this.CheckTopicCalls, topic)
	if this.CheckTopicF != nil {
		return this.CheckTopicF(topic), nil
	}
	return true, nil
}

func (this *Mock) LogDeviceDisconnect(id string) error {
	log.Println("LogDeviceDisconnect", id)
	return nil
}

func (this *Mock) LogDeviceConnect(id string) error {
	log.Println("LogDeviceConnect", id)
	return nil
}

func (this *Mock) LogHubDisconnect(id string) error {
	log.Println("LogHubDisconnect", id)
	return nil
}

func (this *Mock) LogHubConnect(id string) error {
	log.Println("LogHubConnect", id)
	return nil
}
