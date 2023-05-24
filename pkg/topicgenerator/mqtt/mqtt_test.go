/*
 * Copyright 2020 InfAI (CC SES)
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

package mqtt_test

import (
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/known"
	model "github.com/SENERGY-Platform/models/go/models"
	"reflect"
	"testing"
)

func TestMqtt(t *testing.T) {
	const shortDeviceId = "a9B7ddfMShqI26yT9hqnsw"
	const longDeviceId = "urn:infai:ses:device:6bd07b75-d7cc-4a1a-88db-ac93f61aa7b3"
	const protocolId = "pid"

	topics, err := known.Generators["mqtt"](model.Device{
		Id:           longDeviceId,
		LocalId:      "foo",
		Name:         "bar",
		DeviceTypeId: "dt1",
	}, model.DeviceType{
		Id: "dt1",
		Services: []model.Service{
			{
				Id:          "s1",
				LocalId:     "{{.ShortDeviceId}}/s1",
				Interaction: model.REQUEST,
				ProtocolId:  protocolId,
			},
			{
				Id:          "s2",
				LocalId:     "{{.DeviceId}}/s2",
				Interaction: model.EVENT_AND_REQUEST,
				ProtocolId:  protocolId,
			},
			{
				Id:      "s3",
				LocalId: "s3",
				Inputs: []model.Content{
					{
						Id: "i1",
						ContentVariable: model.ContentVariable{
							Id:         "cv1",
							Name:       "cv1",
							Type:       model.String,
							FunctionId: common.CONTROLLING_FUNCTION_PREFIX + "f3",
						},
						Serialization:     model.JSON,
						ProtocolSegmentId: "ps1",
					},
				},
				ProtocolId: protocolId,
			},
			{
				Id:         "nope",
				LocalId:    "nope",
				ProtocolId: protocolId,
			},
			{
				Id:          "nope2",
				LocalId:     "{{.ShortDeviceId}}/nope2",
				Interaction: model.REQUEST,
			},
			{
				Id:          "nope3",
				LocalId:     "{{.DeviceId}}/nope3",
				Interaction: model.EVENT_AND_REQUEST,
			},
			{
				Id:      "nope4",
				LocalId: "nope4",
				Inputs: []model.Content{
					{
						Id: "i2",
						ContentVariable: model.ContentVariable{
							Id:         "cv2",
							Name:       "cv2",
							Type:       model.String,
							FunctionId: common.CONTROLLING_FUNCTION_PREFIX + "f3",
						},
						Serialization:     model.JSON,
						ProtocolSegmentId: "ps1",
					},
				},
			},
		},
	}, map[string]bool{protocolId: true})
	if err != nil {
		t.Error(err)
		return
	}

	expected := []string{
		shortDeviceId + "/s1",
		longDeviceId + "/s2",
		longDeviceId + "/cmnd/s3",
		longDeviceId + "/cmnd/+",
		longDeviceId + "/cmnd/#",
	}

	if !reflect.DeepEqual(topics, expected) {
		t.Error(topics, expected)
	}

}
