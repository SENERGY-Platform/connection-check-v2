/*
 * Copyright 2019 InfAI (CC SES)
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

package connectionlog

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Logger struct {
	devices *kafka.Writer
	hubs    *kafka.Writer
	metrics *prometheus.Metrics
}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, metrics *prometheus.Metrics) (logger *Logger, err error) {
	return &Logger{
		devices: getProducer(ctx, wg, config.KafkaUrl, config.DeviceConnectionLogTopic, config.Debug),
		hubs:    getProducer(ctx, wg, config.KafkaUrl, config.HubConnectionLogTopic, config.Debug),
		metrics: metrics,
	}, nil
}

func getProducer(ctx context.Context, wg *sync.WaitGroup, broker string, topic string, debug bool) (writer *kafka.Writer) {
	var logger *log.Logger
	if debug {
		logger = log.New(os.Stdout, "[KAFKA-PRODUCER] ", 0)
	} else {
		logger = log.New(io.Discard, "", 0)
	}
	writer = &kafka.Writer{
		Addr:        kafka.TCP(broker),
		Topic:       topic,
		MaxAttempts: 10,
		Logger:      logger,
		BatchSize:   1,
		Balancer:    &kafka.Hash{},
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		err := writer.Close()
		if err != nil {
			log.Println("ERROR: unable to close producer for", topic, err)
		}
	}()
	return writer
}

func (this *Logger) LogDeviceDisconnect(device model.ExtendedDevice) error {
	this.metrics.SendDeviceDisconnected.Inc()
	log.Printf("log device %v as disconnected", device.Id)
	monitorConnectionState := ""
	for _, attr := range device.Attributes {
		if attr.Key == "monitor_connection_state" {
			monitorConnectionState = attr.Value
		}
	}
	b, err := json.Marshal(DeviceLog{
		Connected:              false,
		Id:                     device.Id,
		Time:                   time.Now(),
		MonitorConnectionState: monitorConnectionState,
		DeviceOwner:            device.OwnerId,
		DeviceName:             device.DisplayName,
	})
	if err != nil {
		return err
	}
	return this.devices.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(device.Id),
			Value: b,
			Time:  time.Now(),
		},
	)
}

func (this *Logger) LogDeviceConnect(device model.ExtendedDevice) error {
	this.metrics.SendDeviceConnected.Inc()
	log.Printf("log device %v as connected", device.Id)
	b, err := json.Marshal(DeviceLog{
		Connected:              true,
		Id:                     device.Id,
		Time:                   time.Now(),
		MonitorConnectionState: model.GetMonitorConnectionState(device),
		DeviceOwner:            device.OwnerId,
		DeviceName:             device.DisplayName,
	})
	if err != nil {
		return err
	}
	return this.devices.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(device.Id),
			Value: b,
			Time:  time.Now(),
		},
	)
}

func (this *Logger) LogHubConnect(id string) error {
	this.metrics.SendHubConnected.Inc()
	log.Printf("log hub %v as connected", id)
	b, err := json.Marshal(HubLog{
		Connected: true,
		Id:        id,
		Time:      time.Now(),
	})
	if err != nil {
		return err
	}
	return this.hubs.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(id),
			Value: b,
			Time:  time.Now(),
		},
	)
}

func (this *Logger) LogHubDisconnect(id string) error {
	this.metrics.SendHubDisconnected.Inc()
	log.Printf("log hub %v as disconnected", id)
	b, err := json.Marshal(HubLog{
		Connected: false,
		Id:        id,
		Time:      time.Now(),
	})
	if err != nil {
		return err
	}
	return this.hubs.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(id),
			Value: b,
			Time:  time.Now(),
		},
	)
}
