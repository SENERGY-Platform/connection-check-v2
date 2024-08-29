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

package providers

import (
	"errors"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	devicerepo "github.com/SENERGY-Platform/device-repository/lib/client"
	devicemodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"log"
	"strings"
	"sync"
	"time"
)

func NewHubProvider(config configuration.Config, tokengen TokenGenerator, devicetypes DeviceTypeProviderInterface) (result *HubProvider, err error) {
	expiration, err := time.ParseDuration(config.HubProtocolCheckCacheExpiration)
	if err != nil {
		return nil, err
	}
	result = &HubProvider{
		config:      config,
		tokengen:    tokengen,
		devicetypes: devicetypes,
		cache:       NewCache(expiration),
		devicerepo:  devicerepo.NewClient(config.DeviceRepositoryUrl),
	}
	result.handledProtocols = map[string]bool{}
	for _, protocolId := range config.HandledProtocols {
		result.handledProtocols[strings.TrimSpace(protocolId)] = true
	}
	result.maxAge, err = time.ParseDuration(config.MaxHubAge)
	if err != nil {
		return result, err
	}
	return
}

type HubProvider struct {
	config           configuration.Config
	tokengen         TokenGenerator
	devicetypes      DeviceTypeProviderInterface
	devicerepo       devicerepo.Interface
	batch            []model.ExtendedHub
	nextBatchIndex   int
	offset           int64
	lastRequest      time.Time
	maxAge           time.Duration
	mux              sync.Mutex
	handledProtocols map[string]bool
	cache            *Cache
}

func (this *HubProvider) GetNextHub() (hub model.ExtendedHub, isFirstHubOfBatchLoopRepeat bool, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if time.Since(this.lastRequest) > this.maxAge {
		if this.config.Debug {
			log.Println("max age: reset hub batch")
		}
		this.batch = []model.ExtendedHub{}
		this.nextBatchIndex = 0
	}
	backToBeginningCount := 0
	for hub.Id == "" && err == nil {
		hub, err = this.getNextHubFromBatch()
		if err == nil {
			match, err := this.HubMatchesProtocol(hub)
			if err != nil {
				return hub, isFirstHubOfBatchLoopRepeat, err
			}
			if !match {
				hub = model.ExtendedHub{}
			}
		}
		if !errors.Is(err, EmptyBatch) {
			this.offset = this.offset + 1
		} else {
			backToBeginning := false
			this.nextBatchIndex = 0
			this.batch, backToBeginning, err = this.loadBatch(this.offset)
			if err != nil {
				return hub, isFirstHubOfBatchLoopRepeat, err
			}
			if backToBeginning {
				isFirstHubOfBatchLoopRepeat = true
				this.offset = 0
				backToBeginningCount++
				if backToBeginningCount >= 2 {
					return hub, isFirstHubOfBatchLoopRepeat, ErrNoMatchingHub
				}
			}
		}
	}
	return hub, isFirstHubOfBatchLoopRepeat, err
}

var ErrNoMatchingHub = errors.New("no matching hub")

func (this *HubProvider) getNextHubFromBatch() (hub model.ExtendedHub, err error) {
	if this.nextBatchIndex >= len(this.batch) {
		return hub, EmptyBatch
	}
	index := this.nextBatchIndex
	this.nextBatchIndex = this.nextBatchIndex + 1
	return this.batch[index], nil
}

func (this *HubProvider) GetHub(id string) (result model.ExtendedHub, err error) {
	token, err := this.tokengen.Access()
	if err != nil {
		return result, err
	}
	result, err, _ = this.devicerepo.ReadExtendedHub(id, token, devicemodel.READ)
	return result, err
}

func (this *HubProvider) loadBatch(offset int64) (batch []model.ExtendedHub, backToTheBeginning bool, err error) {
	if this.config.Debug {
		log.Println("load hub batch", this.config.PermissionsRequestHubBatchSize)
	}
	token, err := this.tokengen.Access()
	if err != nil {
		return nil, false, err
	}
	batch, _, err, _ = this.devicerepo.ListExtendedHubs(token, devicerepo.HubListOptions{
		Limit:  int64(this.config.PermissionsRequestHubBatchSize),
		Offset: offset,
	})
	if err != nil {
		return nil, false, err
	}
	if len(batch) == 0 {
		backToTheBeginning = true
		if this.config.Debug {
			log.Println("load batch from beginning")
		}
		batch, _, err, _ = this.devicerepo.ListExtendedHubs(token, devicerepo.HubListOptions{
			Limit:  int64(this.config.PermissionsRequestHubBatchSize),
			Offset: offset,
		})
	}
	return batch, backToTheBeginning, nil
}

func (this *HubProvider) HubMatchesProtocol(hub model.ExtendedHub) (result bool, err error) {
	err = this.cache.Use("hubmatchesprotocols."+hub.Id, func() (interface{}, error) {
		token, err := this.tokengen.Access()
		if err != nil {
			log.Println("ERROR:", err)
			return nil, err
		}

		if len(hub.DeviceIds) == 0 {
			return false, nil
		}

		device, err, _ := this.devicerepo.ReadDevice(hub.DeviceIds[0], token, devicemodel.READ)
		if err != nil {
			log.Println("ERROR:", err)
			return nil, err
		}

		dt, err := this.devicetypes.GetDeviceType(device.DeviceTypeId)
		if err != nil {
			log.Println("ERROR:", err)
			return nil, err
		}

		return DeviceTypeUsesHandledProtocol(dt, this.handledProtocols), nil
	}, &result)
	return
}

func distinct(topics []string) (result []string) {
	result = []string{}
	index := map[string]bool{}
	for _, t := range topics {
		if !index[t] {
			result = append(result, t)
		}
		index[t] = true
	}
	return result
}
