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
	"github.com/SENERGY-Platform/device-repository/lib/client"
	model "github.com/SENERGY-Platform/models/go/models"
	"time"
)

func NewDeviceTypeProvider(config configuration.Config, tokengen TokenGenerator) (*DeviceTypeProvider, error) {
	expiration, err := time.ParseDuration(config.DeviceTypeCacheExpiration)
	if err != nil {
		return nil, err
	}
	return &DeviceTypeProvider{
		client:   client.NewClient(config.DeviceRepositoryUrl),
		tokengen: tokengen,
		cache:    NewCache(expiration),
	}, nil
}

type DeviceTypeProvider struct {
	client   client.Interface
	tokengen TokenGenerator
	cache    *Cache
}

func (this *DeviceTypeProvider) GetDeviceType(id string) (result model.DeviceType, err error) {
	err = this.cache.Use("device-type."+id, func() (interface{}, error) {
		token, err := this.tokengen.Access()
		if err != nil {
			return nil, err
		}
		temp, err, _ := this.client.ReadDeviceType(id, token)
		return temp, err
	}, &result)
	return
}
