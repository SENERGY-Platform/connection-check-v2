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

package worker

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/patrickmn/go-cache"
	"log"
	"sync"
	"time"
)

type Worker struct {
	config             configuration.Config
	logger             ConnectionLogger
	deviceprovider     DeviceProvider
	hubprovider        HubProvider
	deviceTypeProvider DeviceTypeProvider
	verne              Verne
	topic              common.TopicGenerator
	hintstore          *cache.Cache
	metrics            *prometheus.Metrics
	minimalRecheckWait time.Duration
}

func New(config configuration.Config, logger ConnectionLogger, deviceprovider DeviceProvider, hubprovider HubProvider, deviceTypeProvider DeviceTypeProvider, verne Verne, metrics *prometheus.Metrics) (*Worker, error) {
	topic, ok := topicgenerator.Known[config.TopicGenerator]
	if !ok {
		return nil, errors.New("unknown topic generator: " + config.TopicGenerator)
	}
	deviceCheckTopicHintExpiration, err := time.ParseDuration(config.DeviceCheckTopicHintExpiration)
	if err != nil {
		return nil, err
	}
	minimalRecheckWait, err := time.ParseDuration(config.MinimalRecheckWaitDuration)
	if err != nil {
		log.Printf("WARNING %v -> set MinimalRecheckWaitDuration to 0\n", err)
		minimalRecheckWait = time.Duration(0)
	}
	return &Worker{
		config:             config,
		logger:             logger,
		deviceprovider:     deviceprovider,
		hubprovider:        hubprovider,
		deviceTypeProvider: deviceTypeProvider,
		verne:              verne,
		topic:              topic,
		hintstore:          cache.New(deviceCheckTopicHintExpiration, time.Minute),
		metrics:            metrics,
		minimalRecheckWait: minimalRecheckWait,
	}, nil
}

type ConnectionLogger interface {
	LogDeviceDisconnect(device model.ExtendedDevice) error
	LogDeviceConnect(device model.ExtendedDevice) error
	LogHubDisconnect(id string) error
	LogHubConnect(id string) error
}

type DeviceProvider interface {
	GetNextDevice() (device model.ExtendedDevice, resets int, err error)
	GetDevice(id string) (result model.ExtendedDevice, err error)
}

type HubProvider interface {
	GetNextHub() (hub model.ExtendedHub, resets int, err error)
	GetHub(id string) (result model.ExtendedHub, err error)
}

type Verne interface {
	CheckTopic(topic string) (result bool, err error)
	CheckClient(clientId string) (result bool, err error)
}

type DeviceTypeProvider interface {
	GetDeviceType(deviceTypeId string) (dt models.DeviceType, err error)
}

func (this *Worker) RunDeviceLoop(ctx context.Context, wg *sync.WaitGroup) error {
	if this.config.DeviceCheckInterval == "" || this.config.DeviceCheckInterval == "-" {
		return nil
	}
	batchLoopStartTime := time.Now()

	dur, err := time.ParseDuration(this.config.DeviceCheckInterval)
	if err != nil {
		return err
	}
	errHandler := getErrorHandler(this.config)
	t := time.NewTicker(dur)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-t.C:
				var resets int
				resets, err = this.runDeviceCheck()
				errHandler(err)
				if resets > 0 {
					since := time.Since(batchLoopStartTime)
					if since < this.minimalRecheckWait {
						time.Sleep(this.minimalRecheckWait - since)
					}
					batchLoopStartTime = time.Now()
				}
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
	}()
	return nil
}

func getErrorHandler(config configuration.Config) func(error) {
	if config.MaxErrorCountTilFatal >= 0 {
		return getFatalErrOnRepeatHandler(config.MaxErrorCountTilFatal)
	} else {
		return getLogErrorHandler()
	}
}

func getLogErrorHandler() func(error) {
	return func(err error) {
		if err != nil {
			log.Println("ERROR:", err)
		}
	}
}

func getFatalErrOnRepeatHandler(maxCount int64) func(error) {
	var counter int64 = 0
	mux := sync.Mutex{}
	return func(err error) {
		mux.Lock()
		defer mux.Unlock()
		if err == nil {
			counter = 0
		} else {
			counter = counter + 1
			if counter > maxCount {
				log.Fatalln("ERROR:", err)
			} else {
				log.Println("ERROR:", err)
			}
		}
	}
}

func (this *Worker) runDeviceCheck() (resets int, err error) {
	this.metrics.DevicesChecked.Inc()
	start := time.Now()
	var device model.ExtendedDevice
	device, resets, err = this.deviceprovider.GetNextDevice()
	if errors.Is(err, providers.BatchNoMatchAfterMultipleResets) {
		log.Println("no device to check found")
		return resets, nil
	}
	if err != nil {
		return resets, err
	}
	topics, err := this.topic(this.config, this.deviceTypeProvider, device)
	if errors.Is(err, common.NoSubscriptionExpected) {
		return resets, nil
	}
	if err != nil {
		return resets, err
	}
	isOnline, err := this.checkTopics(device, topics)
	if err != nil {
		return resets, err
	}
	this.metrics.DeviceCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	expected := device.ConnectionState == models.ConnectionStateOnline
	if expected != isOnline || device.ConnectionState == models.ConnectionStateUnknown {
		return resets, this.updateDeviceState(device, isOnline)
	}
	return resets, nil
}

func (this *Worker) checkTopics(device model.ExtendedDevice, topics []string) (onlineSubscriptionExists bool, err error) {
	if this.config.Debug {
		defer func() {
			log.Printf("DEBUG: check device id=%v owner=%v local-id=%v result=%v err=%v\n", device.Id, device.OwnerId, device.LocalId, onlineSubscriptionExists, err)
		}()
	}
	hintTopic, useHint := this.getHint(device)
	if useHint {
		onlineSubscriptionExists, err = this.verne.CheckTopic(hintTopic)
		if onlineSubscriptionExists {
			return
		}
		if this.config.UseDeviceCheckTopicHintExclusively {
			return
		}
	}
	for _, topic := range topics {
		onlineSubscriptionExists, err = this.verne.CheckTopic(topic)
		if err != nil {
			return
		}
		if onlineSubscriptionExists {
			this.storeHint(device, topic)
			return
		}
	}
	return
}

func (this *Worker) updateDeviceState(device model.ExtendedDevice, online bool) error {
	reloaded, err := this.deviceprovider.GetDevice(device.Id)
	if err != nil {
		log.Println("WARNING: unable to reload device info", err)
		return err
	}
	currentlyOnline := reloaded.ConnectionState == models.ConnectionStateOnline
	if currentlyOnline == online && reloaded.ConnectionState != models.ConnectionStateUnknown && model.GetMonitorConnectionState(device) == "" {
		return nil //connection check has been too slow and the device has already the new online state
	}
	if online {
		return this.logger.LogDeviceConnect(device)
	} else {
		return this.logger.LogDeviceDisconnect(device)
	}
}
