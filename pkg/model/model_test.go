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

package model

import (
	"testing"

	"github.com/SENERGY-Platform/models/go/models"
)

func TestGetMonitorConnectionState(t *testing.T) {
	newDevice := func(attributes ...models.Attribute) ExtendedDevice {
		device := ExtendedDevice{}
		device.Attributes = attributes
		return device
	}
	tests := []struct {
		name   string
		device ExtendedDevice
		want   string
	}{
		{"not set", newDevice(), ""},
		{"set", newDevice(models.Attribute{Key: "monitor_connection_state", Value: "1h"}), "1h"},
		{"set and inactive", newDevice(models.Attribute{Key: "monitor_connection_state", Value: "1h"}, models.Attribute{Key: "inactive", Value: "true"}), ""},
		{"set and inactive before", newDevice(models.Attribute{Key: "inactive", Value: "true"}, models.Attribute{Key: "monitor_connection_state", Value: "1h"}), ""},
		{"set and inactive false", newDevice(models.Attribute{Key: "monitor_connection_state", Value: "1h"}, models.Attribute{Key: "inactive", Value: "false"}), "1h"},
		{"set from default and inactive", newDevice(models.Attribute{Key: "monitor_connection_state", Value: "1h", Origin: "default"}, models.Attribute{Key: "inactive", Value: "true", Origin: "web-ui"}), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetMonitorConnectionState(tt.device); got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}
