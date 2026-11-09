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
	"bufio"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	"github.com/SENERGY-Platform/models/go/models"
)

type switchableDevices struct {
	*guardDevices
	empty bool
}

func (this *switchableDevices) GetNextDevice() (model.ExtendedDevice, int, error) {
	if this.empty {
		return model.ExtendedDevice{}, 2, providers.BatchNoMatchAfterMultipleResets
	}
	return this.guardDevices.GetNextDevice()
}

type switchableHubs struct {
	*guardHubs
	empty bool
}

func (this *switchableHubs) GetNextHub() (models.ExtendedHub, int, error) {
	if this.empty {
		return models.ExtendedHub{}, 2, providers.BatchNoMatchAfterMultipleResets
	}
	return this.guardHubs.GetNextHub()
}

// captureLogLevels runs fn with the worker's logger writing into a pipe and returns the level of every line with msg.
func captureLogLevels(t *testing.T, setup *guardSetup, msg string, fn func()) (levels []string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	setup.worker.config.LogLevel = "info"
	setup.worker.config.GetLogger()
	os.Stdout = stdout
	done := make(chan []string)
	go func() {
		var result []string
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := struct {
				Level string `json:"level"`
				Msg   string `json:"msg"`
			}{}
			if json.Unmarshal(scanner.Bytes(), &line) == nil && line.Msg == msg {
				result = append(result, line.Level)
			}
		}
		done <- result
	}()
	fn()
	w.Close()
	levels = <-done
	_, _ = io.Copy(io.Discard, r)
	return levels
}

func TestNoDeviceToCheckIsLoggedOnceUntilDevicesReturn(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.liveTopics[guardDeviceTopic] = true
	devices := &switchableDevices{guardDevices: setup.devices}
	setup.worker.deviceprovider = devices
	levels := captureLogLevels(t, setup, "no device to check found", func() {
		for _, empty := range []bool{true, true, true, false, true} {
			devices.empty = empty
			if _, err := setup.worker.runDeviceCheck(); err != nil {
				t.Fatal(err)
			}
		}
	})
	if len(levels) != 2 || levels[0] != "INFO" || levels[1] != "INFO" {
		t.Errorf("expected two INFO lines, one per empty phase, got %v", levels)
	}
}

func TestNoHubToCheckIsLoggedOnceUntilHubsReturn(t *testing.T) {
	setup := newGuardSetup(models.ConnectionStateOnline)
	setup.verne.liveClients["h1"] = true
	hubs := &switchableHubs{guardHubs: setup.hubs}
	setup.worker.hubprovider = hubs
	levels := captureLogLevels(t, setup, "no hub to check found", func() {
		for _, empty := range []bool{true, true, true, false, true} {
			hubs.empty = empty
			if _, err := setup.worker.runHubCheck(); err != nil {
				t.Fatal(err)
			}
		}
	})
	if len(levels) != 2 || levels[0] != "INFO" || levels[1] != "INFO" {
		t.Errorf("expected two INFO lines, one per empty phase, got %v", levels)
	}
}
