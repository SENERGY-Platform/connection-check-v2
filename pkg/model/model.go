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

package model

import "github.com/SENERGY-Platform/models/go/models"

type ExtendedDevice = models.ExtendedDevice

type ExtendedHub = models.ExtendedHub

type PermDeviceType struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

func GetMonitorConnectionState(device ExtendedDevice) string {
	for _, attr := range device.Attributes {
		if attr.Key == "monitor_connection_state" {
			return attr.Value
		}
	}
	return ""
}

type StatesRefreshRequestItem struct {
	ID        string `json:"id"`
	LMResult  int    `json:"lm_result"`
	SubResult int    `json:"sub_result"`
}

type StatesRefreshResponseErrItem struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}
