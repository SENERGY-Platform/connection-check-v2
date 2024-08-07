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
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/permission-search/lib/client"
	permmodel "github.com/SENERGY-Platform/permission-search/lib/model"
	"log"
	"strings"
	"sync"
	"time"
)

func NewDeviceProvider(config configuration.Config, tokengen TokenGenerator, devicetypes DeviceTypeProviderInterface) (result *DeviceProvider, err error) {
	result = &DeviceProvider{
		config:      config,
		tokengen:    tokengen,
		devicetypes: devicetypes,
		permissions: client.NewClient(config.PermissionSearchUrl),
	}
	result.handledProtocols = map[string]bool{}
	for _, protocolId := range config.HandledProtocols {
		result.handledProtocols[strings.TrimSpace(protocolId)] = true
	}
	result.maxAge, err = time.ParseDuration(config.MaxDeviceAge)
	if err != nil {
		return result, err
	}
	return
}

type DeviceProvider struct {
	config           configuration.Config
	tokengen         TokenGenerator
	devicetypes      DeviceTypeProviderInterface
	permissions      client.Client
	batch            []model.PermDevice
	nextBatchIndex   int
	lastDevice       model.PermDevice
	lastRequest      time.Time
	maxAge           time.Duration
	mux              sync.Mutex
	handledProtocols map[string]bool
}

type TokenGenerator interface {
	Access() (token string, err error)
}

type DeviceTypeProviderInterface interface {
	GetDeviceType(deviceTypeId string) (dt models.DeviceType, err error)
}

func (this *DeviceProvider) GetNextDevice() (device model.PermDevice, isFirstDeviceOfBatchLoopRepeat bool, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if time.Since(this.lastRequest) > this.maxAge {
		if this.config.Debug {
			log.Println("max age: reset batch")
		}
		this.batch = []model.PermDevice{}
		this.nextBatchIndex = 0
	}
	backToBeginningCount := 0
	for device.Id == "" && err == nil {
		device, err = this.getNextDeviceFromBatch()
		if err == nil {
			this.lastDevice = device
			if !this.deviceMatchesProtocol(device) {
				device = model.PermDevice{}
			}
		}
		if err == EmptyBatch {
			backToBeginning := false
			this.nextBatchIndex = 0
			this.batch, backToBeginning, err = this.getBatch(this.lastDevice)
			if err != nil {
				return device, isFirstDeviceOfBatchLoopRepeat, err
			}
			if backToBeginning {
				isFirstDeviceOfBatchLoopRepeat = true
				this.lastDevice = model.PermDevice{}
				backToBeginningCount++
				if backToBeginningCount >= 2 {
					return device, isFirstDeviceOfBatchLoopRepeat, ErrNoMatchingDevice
				}
			}
		}
	}
	return device, isFirstDeviceOfBatchLoopRepeat, err
}

var EmptyBatch = errors.New("empty batch")
var ErrNoMatchingDevice = errors.New("no matching device")

func (this *DeviceProvider) getNextDeviceFromBatch() (device model.PermDevice, err error) {
	if this.nextBatchIndex >= len(this.batch) {
		return device, EmptyBatch
	}
	index := this.nextBatchIndex
	this.nextBatchIndex = this.nextBatchIndex + 1
	return this.batch[index], nil
}

func (this *DeviceProvider) deviceMatchesProtocol(device model.PermDevice) bool {
	dt, err := this.devicetypes.GetDeviceType(device.DeviceTypeId)
	if err != nil {
		log.Printf("ERROR: skip device %v %v because %v", device.Id, device.Name, err)
		return false
	}
	return DeviceTypeUsesHandledProtocol(dt, this.handledProtocols)
}

func (this *DeviceProvider) GetDevice(id string) (result model.PermDevice, err error) {
	token, err := this.tokengen.Access()
	if err != nil {
		return result, err
	}
	temp, _, err := client.Query[[]model.PermDevice](this.permissions, token, permmodel.QueryMessage{
		Resource: "devices",
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
	return result, errors.New("device not found")
}

func (this *DeviceProvider) getBatch(from model.PermDevice) (batch []model.PermDevice, backToTheBeginning bool, err error) {
	if this.config.Debug {
		log.Println("load batch")
	}
	token, err := this.tokengen.Access()
	if err != nil {
		return nil, false, err
	}
	var after *permmodel.ListAfter
	if this.lastDevice.Id != "" {
		after = &permmodel.ListAfter{
			Id: from.Id,
		}
		if this.config.Debug {
			log.Printf("use after %#v", *after)
		}
	}
	this.lastRequest = time.Now()
	list, err := client.List[[]model.PermDevice](this.permissions, token, "devices", permmodel.ListOptions{
		QueryListCommons: permmodel.QueryListCommons{
			Limit:  this.config.PermissionsRequestDeviceBatchSize,
			After:  after,
			Rights: "r",
			SortBy: "id",
		},
	})
	if err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		backToTheBeginning = true
		if this.config.Debug {
			log.Println("load batch from beginning")
		}
		list, err = client.List[[]model.PermDevice](this.permissions, token, "devices", permmodel.ListOptions{
			QueryListCommons: permmodel.QueryListCommons{
				Limit:  this.config.PermissionsRequestDeviceBatchSize,
				Rights: "r",
				SortBy: "id",
			},
		})
		if err != nil {
			return list, backToTheBeginning, nil
		}
	}
	return list, backToTheBeginning, nil
}

func DeviceTypeUsesHandledProtocol(dt models.DeviceType, handledProtocols map[string]bool) (result bool) {
	for _, service := range dt.Services {
		if handledProtocols[service.ProtocolId] {
			return true
		}
	}
	return false
}
