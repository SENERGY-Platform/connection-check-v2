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
	"github.com/SENERGY-Platform/permission-search/lib/client"
	permmodel "github.com/SENERGY-Platform/permission-search/lib/model"
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
		permissions: client.NewClient(config.PermissionSearchUrl),
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
	permissions      client.Client
	batch            []model.PermHub
	nextBatchIndex   int
	lastHub          model.PermHub
	lastRequest      time.Time
	maxAge           time.Duration
	mux              sync.Mutex
	handledProtocols map[string]bool
	cache            *Cache
}

func (this *HubProvider) GetNextHub() (hub model.PermHub, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if time.Since(this.lastRequest) > this.maxAge {
		if this.config.Debug {
			log.Println("max age: reset hub batch")
		}
		this.batch = []model.PermHub{}
		this.nextBatchIndex = 0
	}
	for hub.Id == "" && err == nil {
		hub, err = this.getNextHubFromBatch()
		if err == nil {
			this.lastHub = hub
			match, err := this.HubMatchesProtocol(hub)
			if err != nil {
				return hub, err
			}
			if !match {
				hub = model.PermHub{}
			}
		}
		if err == EmptyBatch {
			err = this.loadBatch()
			if err != nil {
				return hub, err
			}
		}
	}
	return hub, err
}

func (this *HubProvider) getNextHubFromBatch() (hub model.PermHub, err error) {
	if this.nextBatchIndex >= len(this.batch) {
		return hub, EmptyBatch
	}
	index := this.nextBatchIndex
	this.nextBatchIndex = this.nextBatchIndex + 1
	return this.batch[index], nil
}

func (this *HubProvider) GetHub(id string) (result model.PermHub, err error) {
	token, err := this.tokengen.Access()
	if err != nil {
		return result, err
	}
	temp, _, err := client.Query[[]model.PermHub](this.permissions, token, permmodel.QueryMessage{
		Resource: "hubs",
		ListIds: &permmodel.QueryListIds{
			QueryListCommons: permmodel.QueryListCommons{
				Limit:  1,
				Offset: 0,
			},
			Ids: []string{id},
		},
	})
	if err != nil {
		return result, err
	}
	if len(temp) == 1 {
		return temp[0], nil
	}
	return result, errors.New("hub not found")
}

func (this *HubProvider) loadBatch() error {
	if this.config.Debug {
		log.Println("load hub batch", this.config.PermissionsRequestHubBatchSize)
	}
	token, err := this.tokengen.Access()
	if err != nil {
		return err
	}
	var after *permmodel.ListAfter
	if this.lastHub.Id != "" {
		after = &permmodel.ListAfter{
			Id: this.lastHub.Id,
		}
		if this.config.Debug {
			log.Printf("use after %#v", *after)
		}
	}
	this.lastRequest = time.Now()
	list, err := client.List[[]model.PermHub](this.permissions, token, "hubs", permmodel.ListOptions{
		QueryListCommons: permmodel.QueryListCommons{
			Limit:  this.config.PermissionsRequestHubBatchSize,
			After:  after,
			Rights: "r",
			SortBy: "id",
		},
	})
	if err != nil {
		return err
	}
	if len(list) == 0 {
		if this.config.Debug {
			log.Println("load hub batch from beginning")
		}
		this.lastRequest = time.Now()
		list, err = client.List[[]model.PermHub](this.permissions, token, "hubs", permmodel.ListOptions{
			QueryListCommons: permmodel.QueryListCommons{
				Limit:  this.config.PermissionsRequestHubBatchSize,
				Rights: "r",
				SortBy: "id",
			},
		})
		if err != nil {
			return err
		}
	}
	this.batch = list
	this.nextBatchIndex = 0
	return nil
}

func (this *HubProvider) HubMatchesProtocol(hub model.PermHub) (result bool, err error) {
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
