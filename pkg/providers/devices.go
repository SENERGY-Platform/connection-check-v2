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
	"github.com/SENERGY-Platform/device-repository/lib/client"
	clientmodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/models/go/models"
	"log"
	"strings"
)

func NewDeviceProvider(config configuration.Config, tokengen TokenGenerator, devicetypes DeviceTypeProviderInterface) (result *DeviceProvider, err error) {
	result = &DeviceProvider{
		config:      config,
		tokengen:    tokengen,
		devicetypes: devicetypes,
		client:      client.NewClient(config.DeviceRepositoryUrl, nil),
	}
	result.handledProtocols = map[string]bool{}
	for _, protocolId := range config.HandledProtocols {
		result.handledProtocols[strings.TrimSpace(protocolId)] = true
	}
	result.batch = NewBatch(result.getDevices, result.deviceMatchesProtocol)
	return
}

type DeviceProvider struct {
	config           configuration.Config
	tokengen         TokenGenerator
	devicetypes      DeviceTypeProviderInterface
	handledProtocols map[string]bool
	client           client.Interface
	batch            *Batch[model.ExtendedDevice]
}

type TokenGenerator interface {
	Access() (token string, err error)
}

type DeviceTypeProviderInterface interface {
	GetDeviceType(deviceTypeId string) (dt models.DeviceType, err error)
}

func (this *DeviceProvider) GetNextDevice() (device model.ExtendedDevice, resets int, err error) {
	return this.batch.GetNext()
}

func (this *DeviceProvider) deviceMatchesProtocol(device model.ExtendedDevice) (bool, error) {
	dt, err := this.devicetypes.GetDeviceType(device.DeviceTypeId)
	if err != nil {
		log.Printf("ERROR: skip device %v %v because %v", device.Id, device.Name, err)
		return false, nil
	}
	return DeviceTypeUsesHandledProtocol(dt, this.handledProtocols), nil
}

func (this *DeviceProvider) GetDevice(id string) (result model.ExtendedDevice, err error) {
	token, err := this.tokengen.Access()
	if err != nil {
		return result, err
	}
	result, err, _ = this.client.ReadExtendedDevice(id, token, clientmodel.READ, true)
	return result, err
}

func (this *DeviceProvider) getDevices(offset int64) (result []model.ExtendedDevice, err error) {
	if this.config.Debug {
		log.Println("load batch", offset)
	}
	token, err := this.tokengen.Access()
	if err != nil {
		return nil, err
	}
	result, _, err, _ = this.client.ListExtendedDevices(token, client.ExtendedDeviceListOptions{
		Limit:  int64(this.config.PermissionsRequestDeviceBatchSize),
		Offset: offset,
		FullDt: true,
	})
	return
}

func DeviceTypeUsesHandledProtocol(dt models.DeviceType, handledProtocols map[string]bool) (result bool) {
	for _, service := range dt.Services {
		if handledProtocols[service.ProtocolId] {
			return true
		}
	}
	return false
}
