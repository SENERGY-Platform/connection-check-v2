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
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	devicerepo "github.com/SENERGY-Platform/device-repository/lib/client"
	devicemodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"log"
	"strings"
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
	result.batch = NewBatch(result.getHubs, result.HubMatchesProtocol)
	return
}

type HubProvider struct {
	config           configuration.Config
	tokengen         TokenGenerator
	devicetypes      DeviceTypeProviderInterface
	devicerepo       devicerepo.Interface
	handledProtocols map[string]bool
	cache            *Cache
	batch            *Batch[model.ExtendedHub]
}

func (this *HubProvider) GetNextHub() (hub model.ExtendedHub, resets int, err error) {
	return this.batch.GetNext()
}

func (this *HubProvider) GetHub(id string) (result model.ExtendedHub, err error) {
	token, err := this.tokengen.Access()
	if err != nil {
		return result, err
	}
	result, err, _ = this.devicerepo.ReadExtendedHub(id, token, devicemodel.READ)
	return result, err
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

func (this *HubProvider) getHubs(offset int64) (result []model.ExtendedHub, err error) {
	if this.config.Debug {
		log.Println("load hub batch", this.config.PermissionsRequestHubBatchSize)
	}
	token, err := this.tokengen.Access()
	if err != nil {
		return nil, err
	}
	result, _, err, _ = this.devicerepo.ListExtendedHubs(token, devicerepo.HubListOptions{
		Limit:  int64(this.config.PermissionsRequestHubBatchSize),
		Offset: offset,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
