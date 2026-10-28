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
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/chirpstack/chirpstack/api/go/v4/api"
)

const lastMessageMaxAgeAttrKey = "last_message_max_age"

var noStateChecksErr = errors.New("no checks available")

type Worker struct {
	config             configuration.Config
	logger             ConnectionLogger
	deviceprovider     DeviceProvider
	hubprovider        HubProvider
	deviceTypeProvider DeviceTypeProvider
	lmProvider         LastMessageProvider
	verne              Verne
	topic              common.TopicGenerator
	metrics            *prometheus.Metrics
	minimalRecheckWait time.Duration
	chirpGateway       api.GatewayServiceClient
	chirpTenant        api.TenantServiceClient
}

func New(config configuration.Config, logger ConnectionLogger, deviceprovider DeviceProvider, hubprovider HubProvider, deviceTypeProvider DeviceTypeProvider, lmProvider LastMessageProvider, verne Verne, metrics *prometheus.Metrics, chirpGateway api.GatewayServiceClient, chirpTenant api.TenantServiceClient) (*Worker, error) {
	topic, ok := topicgenerator.Known[config.TopicGenerator]
	if !ok {
		return nil, errors.New("unknown topic generator: " + config.TopicGenerator)
	}
	minimalRecheckWait, err := time.ParseDuration(config.MinimalRecheckWaitDuration)
	if err != nil {
		config.GetLogger().Warn("invalid MinimalRecheckWaitDuration -> set MinimalRecheckWaitDuration to 0", "error", err)
		minimalRecheckWait = time.Duration(0)
	}
	return &Worker{
		config:             config,
		logger:             logger,
		deviceprovider:     deviceprovider,
		hubprovider:        hubprovider,
		deviceTypeProvider: deviceTypeProvider,
		lmProvider:         lmProvider,
		verne:              verne,
		topic:              topic,
		metrics:            metrics,
		minimalRecheckWait: minimalRecheckWait,
		chirpGateway:       chirpGateway,
		chirpTenant:        chirpTenant,
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
	// live checks that bypass the snapshot, used only before a disconnect
	ConfirmTopic(topic string) (result bool, err error)
	ConfirmClient(clientId string) (result bool, err error)
}

type DeviceTypeProvider interface {
	GetDeviceType(deviceTypeId string) (dt models.DeviceType, err error)
}

type LastMessageProvider interface {
	CheckLastMessages(deviceID string, serviceIDs []string, maxAge time.Duration) (bool, error)
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

func (this *Worker) CheckDeviceState(deviceID string, lmResult, subResult int) error {
	device, err := this.deviceprovider.GetDevice(deviceID)
	if err != nil {
		return err
	}
	isOnline, subFromSnapshot, err := this.getDeviceState(device, lmResult, subResult)
	if err != nil {
		if errors.Is(err, noStateChecksErr) {
			return nil
		}
		this.config.GetLogger().Error("checking device state failed", "deviceId", device.Id, "owner", device.OwnerId, "deviceLocalId", device.LocalId, "error", err)
		return err
	}
	return this.setDeviceState(device, isOnline, subFromSnapshot, lmResult, subResult)
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
			slog.Default().Error("getLogErrorHandler()", "error", err)
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
			slog.Default().Error("getFatalErrOnRepeatHandler", "error", err)
			if counter > maxCount {
				log.Fatalln("ERROR:", err)
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
		this.config.GetLogger().Error("no device to check found", "error", err)
		return resets, nil
	}
	if err != nil {
		return resets, err
	}
	isOnline, subFromSnapshot, err := this.getDeviceState(device, 0, 0)
	if err != nil {
		if errors.Is(err, noStateChecksErr) {
			return resets, nil
		}
		this.config.GetLogger().Error("checking device state failed", "deviceId", device.Id, "owner", device.OwnerId, "deviceLocalId", device.LocalId, "error", err)
		return resets, err
	}
	this.metrics.DeviceCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	return resets, this.setDeviceState(device, isOnline, subFromSnapshot, 0, 0)
}

func (this *Worker) setDeviceState(device model.ExtendedDevice, isOnline, subFromSnapshot bool, lmResult, subResult int) error {
	expected := device.ConnectionState == models.ConnectionStateOnline
	if expected != isOnline || device.ConnectionState == models.ConnectionStateUnknown || model.GetMonitorConnectionState(device) != "" {
		return this.updateDeviceState(device, isOnline, subFromSnapshot, lmResult, subResult)
	}
	return nil
}

// subFromSnapshot reports that the verdict includes a subscription check answered by the snapshot
func (this *Worker) getDeviceState(device model.ExtendedDevice, lmResult, subResult int) (isOnline, subFromSnapshot bool, err error) {
	lmOnline, lmAvailable := getExtResult(lmResult)
	var lmCheckErr error
	if !lmAvailable {
		lmOnline, lmAvailable, lmCheckErr = this.checkLastMessages(device)
	}
	subOnline, subAvailable := getExtResult(subResult)
	var subCheckErr error
	if !subAvailable {
		subOnline, subAvailable, subCheckErr = this.checkTopicsWrapper(device)
	}
	if this.config.Debug {
		defer func() {
			this.config.GetLogger().Debug(
				"check device connection state",
				"device", device.Id,
				"owner", device.OwnerId,
				"deviceLocalId", device.LocalId,
				"lm-result-ext", lmResult,
				"sub-result-ext", subResult,
				"lm-available", lmAvailable,
				"sub-available", subAvailable,
				"lm-result", lmOnline,
				"sub-result", subOnline,
				"lm-err", lmCheckErr,
				"sub-err", subCheckErr,
			)
		}()
	}
	switch {
	case lmCheckErr != nil && subCheckErr != nil:
		return false, false, errors.Join(lmCheckErr, subCheckErr)
	case lmCheckErr != nil && !subAvailable:
		return false, false, lmCheckErr
	case subCheckErr != nil && !lmAvailable:
		return false, false, subCheckErr
	}
	switch {
	case !lmAvailable && !subAvailable:
		return false, false, noStateChecksErr
	case lmAvailable && subAvailable:
		isOnline = lmOnline && subOnline
	case lmAvailable && !subAvailable:
		isOnline = lmOnline
	case !lmAvailable && subAvailable:
		isOnline = subOnline
	}
	return isOnline, subAvailable && subResult == 0, nil
}

func (this *Worker) checkLastMessages(device model.ExtendedDevice) (isOnline, available bool, err error) {
	if device.DeviceType == nil {
		return false, false, nil
	}
	maxAge, ok, err := getLastMessageAttr(device.Attributes)
	if err != nil {
		this.config.GetLogger().Error("getting last message attribute failed", "device", device.Id, "owner", device.OwnerId, "deviceLocalId", device.LocalId, "error", err)
		return false, false, err
	}
	if !ok {
		return false, false, nil
	}
	serviceIDs := getRequestServiceIDs(device.DeviceType.Services)
	if len(serviceIDs) == 0 {
		return false, false, nil
	}
	this.config.GetLogger().Debug("LAST MESSAGE CHECK", "device", device.Id, "attributes", device.Attributes, "maxAge", maxAge, "service-ids", serviceIDs)
	isOnline, err = this.lmProvider.CheckLastMessages(device.Id, serviceIDs, maxAge)
	if err != nil {
		this.config.GetLogger().Error("checking last messages failed", "device", device.Id, "owner", device.OwnerId, "deviceLocalId", device.LocalId, "error", err)
		return false, false, err
	}
	return isOnline, true, nil
}

func (this *Worker) checkTopicsWrapper(device model.ExtendedDevice) (isOnline, available bool, err error) {
	topics, err := this.topic(this.config, this.deviceTypeProvider, device)
	if err != nil {
		if errors.Is(err, common.NoSubscriptionExpected) {
			return false, false, nil
		}
		this.config.GetLogger().Error("getting topics failed", "device", device.Id, "owner", device.OwnerId, "deviceLocalId", device.LocalId, "error", err)
		return false, false, err
	}
	isOnline, err = this.checkTopics(topics)
	if err != nil {
		this.config.GetLogger().Error("checking topics failed", "device", device.Id, "owner", device.OwnerId, "deviceLocalId", device.LocalId, "error", err)
		return false, false, err
	}
	return isOnline, true, nil
}

func (this *Worker) checkTopics(topics []string) (onlineSubscriptionExists bool, err error) {
	if this.verne == nil {
		return false, nil
	}
	for _, topic := range topics {
		onlineSubscriptionExists, err = this.verne.CheckTopic(topic)
		if err != nil {
			return
		}
		if onlineSubscriptionExists {
			return
		}
	}
	return
}

func (this *Worker) updateDeviceState(device model.ExtendedDevice, online, subFromSnapshot bool, lmResult, subResult int) error {
	reloaded, err := this.deviceprovider.GetDevice(device.Id)
	if err != nil {
		this.config.GetLogger().Error("updating device state failed", "device", device.Id, "owner", device.OwnerId, "deviceLocalId", device.LocalId, "error", err)
		return err
	}
	currentlyOnline := reloaded.ConnectionState == models.ConnectionStateOnline
	if currentlyOnline == online && reloaded.ConnectionState != models.ConnectionStateUnknown && model.GetMonitorConnectionState(reloaded) == "" {
		return nil //connection check has been too slow and the device has already the new online state
	}
	if online {
		// the snapshot may predate a fresh disconnect, so a connect of a device that is not online needs a live hit
		if !currentlyOnline && subFromSnapshot && this.verne != nil && subResult == 0 {
			subscribed, expected, err := this.confirmTopics(reloaded)
			if err != nil {
				this.config.GetLogger().Error("confirming device subscription failed", "device", reloaded.Id, "owner", reloaded.OwnerId, "deviceLocalId", reloaded.LocalId, "error", err)
				return err
			}
			if expected && !subscribed {
				isOnline, _, err := this.getDeviceState(reloaded, lmResult, -1)
				if err != nil {
					return err
				}
				if currentlyOnline == isOnline && reloaded.ConnectionState != models.ConnectionStateUnknown && model.GetMonitorConnectionState(reloaded) == "" {
					return nil
				}
				if !isOnline {
					return this.logger.LogDeviceDisconnect(reloaded)
				}
			}
		}
		return this.logger.LogDeviceConnect(reloaded)
	}
	// the snapshot may predate a fresh subscription, so a disconnect of an online device needs a live miss
	if currentlyOnline && this.verne != nil && subResult == 0 {
		subscribed, _, err := this.confirmTopics(reloaded)
		if err != nil {
			this.config.GetLogger().Error("confirming device subscription failed", "device", reloaded.Id, "owner", reloaded.OwnerId, "deviceLocalId", reloaded.LocalId, "error", err)
			return err
		}
		if subscribed {
			isOnline, _, err := this.getDeviceState(reloaded, lmResult, 1)
			if err != nil {
				return err
			}
			if isOnline {
				return nil
			}
		}
	}
	return this.logger.LogDeviceDisconnect(reloaded)
}

// expected is false when the device needs no subscription, subscribed is then false as well
func (this *Worker) confirmTopics(device model.ExtendedDevice) (subscribed, expected bool, err error) {
	topics, err := this.topic(this.config, this.deviceTypeProvider, device)
	if errors.Is(err, common.NoSubscriptionExpected) {
		return false, false, nil
	}
	if err != nil {
		return false, true, err
	}
	for _, topic := range topics {
		subscribed, err = this.verne.ConfirmTopic(topic)
		if err != nil || subscribed {
			return subscribed, true, err
		}
	}
	return false, true, nil
}

func getRequestServiceIDs(services []models.Service) []string {
	var ids []string
	for _, service := range services {
		if service.Interaction == models.EVENT || service.Interaction == models.EVENT_AND_REQUEST {
			ids = append(ids, service.Id)
		}
	}
	return ids
}

var lastMessageAttrPrios = map[string]int{
	"web-ui": 2,
	"dcc":    1,
}

func getLastMessageAttr(attributes []models.Attribute) (time.Duration, bool, error) {
	attr, ok := getAttribute(attributes, lastMessageMaxAgeAttrKey, lastMessageAttrPrios)
	if !ok {
		return 0, false, nil
	}
	dur, err := time.ParseDuration(attr.Value)
	if err != nil {
		return 0, false, err
	}
	return dur, true, nil
}

func getExtResult(r int) (isOnline, available bool) {
	if r > 0 {
		return true, true
	}
	if r < 0 {
		return false, true
	}
	return false, false
}

func getAttribute(attributes []models.Attribute, key string, originPriorities map[string]int) (models.Attribute, bool) {
	var result models.Attribute
	var ok bool
	var currentPriority int
	for _, attr := range attributes {
		if attr.Key == key {
			if !ok {
				result = attr
				currentPriority = originPriorities[attr.Origin]
				ok = true
				continue
			}
			newPriority := originPriorities[attr.Origin]
			if newPriority > currentPriority {
				result = attr
				currentPriority = newPriority
			}
		}
	}
	return result, ok
}
