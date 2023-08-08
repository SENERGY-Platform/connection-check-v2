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
	}, nil
}

type ConnectionLogger interface {
	LogDeviceDisconnect(id string) error
	LogDeviceConnect(id string) error
	LogHubDisconnect(id string) error
	LogHubConnect(id string) error
}

type DeviceProvider interface {
	GetNextDevice() (device model.PermDevice, err error)
	GetDevice(id string) (result model.PermDevice, err error)
}

type HubProvider interface {
	GetNextHub() (device model.PermHub, err error)
	GetHub(id string) (result model.PermHub, err error)
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
				errHandler(this.runDeviceCheck())
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

const ConnectionStateAnnotation = "connected"

func (this *Worker) runDeviceCheck() error {
	this.metrics.DevicesChecked.Inc()
	start := time.Now()
	device, err := this.deviceprovider.GetNextDevice()
	if err != nil {
		return err
	}
	topics, err := this.topic(this.config, this.deviceTypeProvider, device)
	if err == common.NoSubscriptionExpected {
		return nil
	}
	if err != nil {
		return err
	}
	isOnline, err := this.checkTopics(device, topics)
	if err != nil {
		return err
	}
	this.metrics.DeviceCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	annotation, ok := device.Annotations[ConnectionStateAnnotation]
	if !ok {
		return this.updateDeviceState(device, isOnline)
	}
	expected, ok := annotation.(bool)
	if !ok {
		log.Printf("WARNING: unexpected device state anotation in %#v", device)
		return this.updateDeviceState(device, isOnline)
	}
	if expected != isOnline {
		return this.updateDeviceState(device, isOnline)
	}
	return nil
}

func (this *Worker) checkTopics(device model.PermDevice, topics []string) (onlineSubscriptionExists bool, err error) {
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

func (this *Worker) updateDeviceState(device model.PermDevice, online bool) error {
	reloaded, err := this.deviceprovider.GetDevice(device.Id)
	if err != nil {
		log.Println("WARNING: unable to reload device info", err)
	}
	annotation, ok := reloaded.Annotations[ConnectionStateAnnotation]
	if ok {
		currentState, ok := annotation.(bool)
		if !ok {
			log.Printf("WARNING: unexpected device state anotation in %#v", device)
		} else if currentState == online {
			return nil //connection check has been too slow and the device has already the new online state
		}
	}
	if online {
		return this.logger.LogDeviceConnect(device.Id)
	} else {
		return this.logger.LogDeviceDisconnect(device.Id)
	}
}
