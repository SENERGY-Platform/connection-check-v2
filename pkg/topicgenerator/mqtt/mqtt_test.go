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

package mqtt

import (
	"errors"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/known"
	"github.com/SENERGY-Platform/models/go/models"
	"reflect"
	"testing"
)

func TestSortByWildcardCount(t *testing.T) {
	result := []string{
		"pre/1/foo/bar",
		"pre/1/foo/+",
		"pre/1/+/bar",
		"pre/1/+/+",
		"+/1/foo/bar",
		"+/1/foo/+",
		"+/1/+/bar",
		"+/1/+/+",

		"pre/1/foo/#",
		"pre/1/#",
		"pre/1/+/#",
		"+/1/foo/#",
		"+/1/#",
		"+/1/+/#",
	}
	SortByWildcardCount(result)
	expected := []string{
		"pre/1/foo/bar",
		"+/1/foo/bar",
		"pre/1/#",
		"pre/1/+/bar",
		"pre/1/foo/#",
		"pre/1/foo/+",
		"+/1/#",
		"+/1/+/bar",
		"+/1/foo/#",
		"+/1/foo/+",
		"pre/1/+/#",
		"pre/1/+/+",
		"+/1/+/#",
		"+/1/+/+",
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("\n%#v\n%#v\n", expected, result)
	}
}

func TestPermuteWildcards(t *testing.T) {
	result := PermuteWildcards("pre/device/foo/bar", []string{"device", "short_device"})
	expected := []string{
		"pre/device/foo/bar",
		"pre/device/foo/+",
		"pre/device/+/bar",
		"pre/device/+/+",
		"+/device/foo/bar",
		"+/device/foo/+",
		"+/device/+/bar",
		"+/device/+/+",

		"pre/device/foo/#",
		"pre/device/#",
		"pre/device/+/#",
		"+/device/foo/#",
		"+/device/#",
		"+/device/+/#",
	}
	SortByWildcardCount(expected)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("\n%#v\n%#v\n", expected, result)
	}
}

func TestPermuteWildcards2(t *testing.T) {
	result := PermuteWildcards("foo/bar/batz", []string{})
	expected := []string{
		"foo/bar/batz",
		"foo/bar/+",
		"foo/+/batz",
		"foo/+/+",
		"+/bar/batz",
		"+/bar/+",
		"+/+/batz",
		"+/+/+",

		"foo/bar/#",
		"foo/#",
		"#",
		"foo/+/#",
		"+/bar/#",
		"+/#",
		"+/+/#",
	}
	SortByWildcardCount(expected)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("\n%#v\n%#v\n", expected, result)
	}
}

type DeviceTypeProviderMock struct {
	DeviceTypes []models.DeviceType
}

func (this *DeviceTypeProviderMock) GetDeviceType(deviceTypeId string) (dt models.DeviceType, err error) {
	for _, dt := range this.DeviceTypes {
		if dt.Id == deviceTypeId {
			return dt, nil
		}
	}
	return dt, errors.New("not found")
}

func TestMqttTopicGenerator(t *testing.T) {
	const shortDeviceId = "a9B7ddfMShqI26yT9hqnsw"
	const longDeviceId = "urn:infai:ses:device:6bd07b75-d7cc-4a1a-88db-ac93f61aa7b3"
	const protocolId = "pid"

	//config configuration.Config, deviceTypeProvider DeviceTypeProvider, device model.ExtendedDevice
	topics, err := known.Generators["mqtt"](
		configuration.Config{
			HandledProtocols: []string{protocolId},
		},
		&DeviceTypeProviderMock{
			DeviceTypes: []models.DeviceType{
				{
					Id: "dt1",
					Services: []models.Service{
						{
							Id:          "s1",
							LocalId:     "{{.ShortDeviceId}}/s1",
							Interaction: models.REQUEST,
							ProtocolId:  protocolId,
						},
						{
							Id:          "s2",
							LocalId:     "{{.DeviceId}}/s2",
							Interaction: models.EVENT_AND_REQUEST,
							ProtocolId:  protocolId,
						},
						{
							Id:      "s3",
							LocalId: "s3",
							Inputs: []models.Content{
								{
									Id: "i1",
									ContentVariable: models.ContentVariable{
										Id:         "cv1",
										Name:       "cv1",
										Type:       models.String,
										FunctionId: common.CONTROLLING_FUNCTION_PREFIX + "f3",
									},
									Serialization:     models.JSON,
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
							Interaction: models.REQUEST,
						},
						{
							Id:          "nope3",
							LocalId:     "{{.DeviceId}}/nope3",
							Interaction: models.EVENT_AND_REQUEST,
						},
						{
							Id:      "nope4",
							LocalId: "nope4",
							Inputs: []models.Content{
								{
									Id: "i2",
									ContentVariable: models.ContentVariable{
										Id:         "cv2",
										Name:       "cv2",
										Type:       models.String,
										FunctionId: common.CONTROLLING_FUNCTION_PREFIX + "f3",
									},
									Serialization:     models.JSON,
									ProtocolSegmentId: "ps1",
								},
							},
						},
					},
				},
			},
		}, model.ExtendedDevice{
			Device: models.Device{
				Id:           longDeviceId,
				LocalId:      "foo",
				Name:         "bar",
				DeviceTypeId: "dt1",
			},
		})
	if err != nil {
		t.Error(err)
		return
	}

	expected := []string{
		shortDeviceId + "/s1",
		shortDeviceId + "/#",
		shortDeviceId + "/+",
	}

	if !reflect.DeepEqual(topics, expected) {
		t.Error(topics, expected)
	}

}
