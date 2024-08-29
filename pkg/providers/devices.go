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
	"github.com/SENERGY-Platform/device-repository/lib/client"
	clientmodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/models/go/models"
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
		client:      client.NewClient(config.DeviceRepositoryUrl),
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
	batch            []model.ExtendedDevice
	nextBatchIndex   int
	offset           int64
	lastRequest      time.Time
	maxAge           time.Duration
	mux              sync.Mutex
	handledProtocols map[string]bool
	client           client.Interface
}

type TokenGenerator interface {
	Access() (token string, err error)
}

type DeviceTypeProviderInterface interface {
	GetDeviceType(deviceTypeId string) (dt models.DeviceType, err error)
}

func (this *DeviceProvider) GetNextDevice() (device model.ExtendedDevice, isFirstDeviceOfBatchLoopRepeat bool, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if time.Since(this.lastRequest) > this.maxAge {
		if this.config.Debug {
			log.Println("max age: reset batch")
		}
		this.batch = []model.ExtendedDevice{}
		this.nextBatchIndex = 0
	}
	backToBeginningCount := 0
	for device.Id == "" && err == nil {
		device, err = this.getNextDeviceFromBatch()
		if err == nil {
			if !this.deviceMatchesProtocol(device) {
				device = model.ExtendedDevice{}
			}
		}
		if !errors.Is(err, EmptyBatch) {
			this.offset = this.offset + 1
		} else {
			backToBeginning := false
			this.nextBatchIndex = 0
			this.batch, backToBeginning, err = this.getBatch(this.offset)
			if err != nil {
				return device, isFirstDeviceOfBatchLoopRepeat, err
			}
			if backToBeginning {
				isFirstDeviceOfBatchLoopRepeat = true
				this.offset = 0
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

func (this *DeviceProvider) getNextDeviceFromBatch() (device model.ExtendedDevice, err error) {
	if this.nextBatchIndex >= len(this.batch) {
		return device, EmptyBatch
	}
	index := this.nextBatchIndex
	this.nextBatchIndex = this.nextBatchIndex + 1
	return this.batch[index], nil
}

func (this *DeviceProvider) deviceMatchesProtocol(device model.ExtendedDevice) bool {
	dt, err := this.devicetypes.GetDeviceType(device.DeviceTypeId)
	if err != nil {
		log.Printf("ERROR: skip device %v %v because %v", device.Id, device.Name, err)
		return false
	}
	return DeviceTypeUsesHandledProtocol(dt, this.handledProtocols)
}

func (this *DeviceProvider) GetDevice(id string) (result model.ExtendedDevice, err error) {
	token, err := this.tokengen.Access()
	if err != nil {
		return result, err
	}
	result, err, _ = this.client.ReadExtendedDevice(id, token, clientmodel.READ)
	return result, err
}

func (this *DeviceProvider) getBatch(offset int64) (batch []model.ExtendedDevice, backToTheBeginning bool, err error) {
	if this.config.Debug {
		log.Println("load batch", offset)
	}
	token, err := this.tokengen.Access()
	if err != nil {
		return nil, false, err
	}
	batch, _, err, _ = this.client.ListExtendedDevices(token, client.DeviceListOptions{
		Limit:  int64(this.config.PermissionsRequestDeviceBatchSize),
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
		batch, _, err, _ = this.client.ListExtendedDevices(token, client.DeviceListOptions{
			Limit:  int64(this.config.PermissionsRequestDeviceBatchSize),
			Offset: 0,
		})
	}
	return batch, backToTheBeginning, nil
}

func DeviceTypeUsesHandledProtocol(dt models.DeviceType, handledProtocols map[string]bool) (result bool) {
	for _, service := range dt.Services {
		if handledProtocols[service.ProtocolId] {
			return true
		}
	}
	return false
}
