/*
 * Copyright (c) 2026 InfAI (CC SES)
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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/vernemq"
	"github.com/SENERGY-Platform/models/go/models"
)

const guardDeviceTopic = "command/o1/l1/+"

// snapshot answers come from topics/clients, live answers from liveTopics/liveClients
type guardVerne struct {
	topics, clients         map[string]bool
	liveTopics, liveClients map[string]bool
	confirmErr              error
	confirmCalls            []string
}

func (this *guardVerne) CheckTopic(topic string) (bool, error) {
	return this.topics[topic], nil
}

func (this *guardVerne) CheckClient(clientId string) (bool, error) {
	return this.clients[clientId], nil
}

func (this *guardVerne) ConfirmTopic(topic string) (bool, error) {
	this.confirmCalls = append(this.confirmCalls, topic)
	return this.liveTopics[topic], this.confirmErr
}

func (this *guardVerne) ConfirmClient(clientId string) (bool, error) {
	this.confirmCalls = append(this.confirmCalls, clientId)
	return this.liveClients[clientId], this.confirmErr
}

type guardDevices struct {
	device model.ExtendedDevice
}

func (this *guardDevices) GetNextDevice() (model.ExtendedDevice, int, error) {
	return this.device, 0, nil
}

func (this *guardDevices) GetDevice(id string) (model.ExtendedDevice, error) {
	return this.device, nil
}

type guardHubs struct {
	hub model.ExtendedHub
}

func (this *guardHubs) GetNextHub() (model.ExtendedHub, int, error) {
	return this.hub, 0, nil
}

func (this *guardHubs) GetHub(id string) (model.ExtendedHub, error) {
	return this.hub, nil
}

type guardDeviceTypes struct {
	deviceClassId string
}

func (this guardDeviceTypes) GetDeviceType(id string) (models.DeviceType, error) {
	return models.DeviceType{Id: id, DeviceClassId: this.deviceClassId}, nil
}

type guardLastMessages struct {
	online bool
}

func (this guardLastMessages) CheckLastMessages(deviceID string, serviceIDs []string, maxAge time.Duration) (bool, error) {
	return this.online, nil
}

type guardLogger struct {
	deviceConnects, deviceDisconnects, hubConnects, hubDisconnects []string
}

func (this *guardLogger) LogDeviceDisconnect(device model.ExtendedDevice) error {
	this.deviceDisconnects = append(this.deviceDisconnects, device.Id)
	return nil
}

func (this *guardLogger) LogDeviceConnect(device model.ExtendedDevice) error {
	this.deviceConnects = append(this.deviceConnects, device.Id)
	return nil
}

func (this *guardLogger) LogHubDisconnect(id string) error {
	this.hubDisconnects = append(this.hubDisconnects, id)
	return nil
}

func (this *guardLogger) LogHubConnect(id string) error {
	this.hubConnects = append(this.hubConnects, id)
	return nil
}

type guardSetup struct {
	worker  *Worker
	verne   *guardVerne
	devices *guardDevices
	hubs    *guardHubs
	logger  *guardLogger
}

func newGuardSetup(state models.ConnectionState) *guardSetup {
	setup := &guardSetup{
		verne: &guardVerne{topics: map[string]bool{}, clients: map[string]bool{}, liveTopics: map[string]bool{}, liveClients: map[string]bool{}},
		devices: &guardDevices{device: model.ExtendedDevice{
			Device:          models.Device{Id: "d1", LocalId: "l1", OwnerId: "o1", DeviceTypeId: "dt1"},
			ConnectionState: state,
		}},
		hubs:   &guardHubs{hub: model.ExtendedHub{Hub: models.Hub{Id: "h1", OwnerId: "o1"}, ConnectionState: state}},
		logger: &guardLogger{},
	}
	setup.worker = &Worker{
		config:             configuration.Config{TopicGenerator: "senergy"},
		logger:             setup.logger,
		deviceprovider:     setup.devices,
		hubprovider:        setup.hubs,
		deviceTypeProvider: guardDeviceTypes{},
		verne:              setup.verne,
		topic:              topicgenerator.Known["senergy"],
		metrics:            prometheus.NewMetrics("test"),
	}
	return setup
}

// lets the last message check decide alongside the subscription check
func (this *guardSetup) withLastMessages(online bool) {
	this.devices.device.Attributes = []models.Attribute{{Key: lastMessageMaxAgeAttrKey, Value: "1h"}}
	this.devices.device.DeviceType = &models.DeviceType{Services: []models.Service{{Id: "s1", Interaction: models.EVENT}}}
	this.worker.lmProvider = guardLastMessages{online: online}
}

func (this *guardSetup) expectLogged(t *testing.T, deviceConnects, deviceDisconnects, hubConnects, hubDisconnects int) {
	t.Helper()
	if len(this.logger.deviceConnects) != deviceConnects || len(this.logger.deviceDisconnects) != deviceDisconnects ||
		len(this.logger.hubConnects) != hubConnects || len(this.logger.hubDisconnects) != hubDisconnects {
		t.Errorf("logged device connects %v, device disconnects %v, hub connects %v, hub disconnects %v; expected %v, %v, %v, %v",
			this.logger.deviceConnects, this.logger.deviceDisconnects, this.logger.hubConnects, this.logger.hubDisconnects,
			deviceConnects, deviceDisconnects, hubConnects, hubDisconnects)
	}
}

func (this *guardSetup) expectConfirmCalls(t *testing.T, expected int) {
	t.Helper()
	if len(this.verne.confirmCalls) != expected {
		t.Errorf("expected %v live confirmations, got %v", expected, this.verne.confirmCalls)
	}
}

func TestDeviceSnapshotMissWithLiveHitIsNotDisconnected(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.liveTopics[guardDeviceTopic] = true
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 0, 0)
}

func TestDeviceSnapshotMissWithLiveMissIsDisconnected(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 1, 0, 0)
	setup.expectConfirmCalls(t, 2) // every topic candidate of the senergy generator
}

func TestDeviceLiveHitWithLastMessageOfflineIsDisconnected(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.withLastMessages(false)
	setup.verne.liveTopics[guardDeviceTopic] = true
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 1, 0, 0)
}

func TestDeviceLiveHitWithLastMessageOnlineIsNotDisconnected(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.withLastMessages(true)
	setup.verne.liveTopics[guardDeviceTopic] = true
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 0, 0)
}

func TestDeviceConfirmErrorPreventsDisconnect(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.confirmErr = errors.New("broker unavailable")
	if _, err := setup.worker.runDeviceCheck(); err == nil {
		t.Error("expected the confirm error to be returned")
	}
	setup.expectLogged(t, 0, 0, 0, 0)
}

func TestDeviceWithoutExpectedSubscriptionSkipsConfirm(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.withLastMessages(false)
	setup.worker.deviceTypeProvider = guardDeviceTypes{deviceClassId: "no-sub"}
	setup.worker.config.DeviceClassesWithoutExpectedSubscriptions = []string{"no-sub"}
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 1, 0, 0)
	setup.expectConfirmCalls(t, 0)
}

func TestDeviceConnectSkipsConfirm(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOffline)
	setup.verne.topics[guardDeviceTopic] = true
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 1, 0, 0, 0)
	setup.expectConfirmCalls(t, 0)
}

func TestDeviceWithUnknownStateSkipsConfirm(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateUnknown)
	setup.verne.liveTopics[guardDeviceTopic] = true
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 1, 0, 0)
	setup.expectConfirmCalls(t, 0)
}

func TestCheckDeviceStateConfirmsSnapshotResult(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.liveTopics[guardDeviceTopic] = true
	if err := setup.worker.CheckDeviceState("d1", 0, 0); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 0, 0)
}

func TestCheckDeviceStateWithExternalSubResultSkipsConfirm(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.liveTopics[guardDeviceTopic] = true
	if err := setup.worker.CheckDeviceState("d1", 0, -1); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 1, 0, 0)
	setup.expectConfirmCalls(t, 0)
}

// against the real snapshot: an old snapshot and a device online long before both miss, so it is disconnected
func TestLongOnlineDeviceWithoutSubscriptionIsDisconnected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"table":[]}`))
	}))
	t.Cleanup(server.Close)
	verne, err := vernemq.New(configuration.Config{
		VerneManagementUrl:           server.URL,
		VerneSnapshotRefreshInterval: "1h",
		VerneSnapshotMaxAge:          "2h",
		VerneSnapshotRowTimeout:      "5s",
		HttpRequestTimeout:           "10s",
	}, prometheus.NewMetrics("test"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	if err := verne.Start(ctx, wg); err != nil {
		t.Fatal(err)
	}
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.worker.verne = verne
	time.Sleep(10 * time.Millisecond) // the device is read well after the snapshot was taken
	if _, err := setup.worker.runDeviceCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 1, 0, 0)
}

func TestHubSnapshotMissWithLiveHitIsNotDisconnected(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.liveClients["h1"] = true
	if _, err := setup.worker.runHubCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 0, 0)
}

func TestHubSnapshotMissWithLiveMissIsDisconnected(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	if _, err := setup.worker.runHubCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 0, 1)
	setup.expectConfirmCalls(t, 1)
}

func TestHubConfirmErrorPreventsDisconnect(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.confirmErr = errors.New("broker unavailable")
	if _, err := setup.worker.runHubCheck(); err == nil {
		t.Error("expected the confirm error to be returned")
	}
	setup.expectLogged(t, 0, 0, 0, 0)
}

func TestHubConnectSkipsConfirm(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOffline)
	setup.verne.clients["h1"] = true
	if _, err := setup.worker.runHubCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 1, 0)
	setup.expectConfirmCalls(t, 0)
}

func TestHubWithUnknownStateSkipsConfirm(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateUnknown)
	setup.verne.liveClients["h1"] = true
	if _, err := setup.worker.runHubCheck(); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 0, 1)
	setup.expectConfirmCalls(t, 0)
}

func TestHubStateOutsideVerneSkipsConfirm(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.liveClients["h1"] = true
	if err := setup.worker.updateHubState(setup.hubs.hub, false, false); err != nil {
		t.Fatal(err)
	}
	setup.expectLogged(t, 0, 0, 0, 1)
	setup.expectConfirmCalls(t, 0)
}
