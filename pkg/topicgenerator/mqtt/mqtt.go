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
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/known"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/mqtt/topic"
	"strings"
)

const DefaultActuatorPattern = "{{.DeviceId}}/cmnd/{{.LocalServiceId}}"

func init() {
	known.Generators["mqtt"] = func(config configuration.Config, deviceTypeProvider common.DeviceTypeProvider, device model.PermDevice) (topicCandidates []string, err error) {
		deviceType, err := deviceTypeProvider.GetDeviceType(device.DeviceTypeId)
		if err != nil {
			return topicCandidates, err
		}
		handledProtocols := map[string]bool{}
		for _, protocolId := range config.HandledProtocols {
			handledProtocols[strings.TrimSpace(protocolId)] = true
		}
		services := common.GetHandledServices(deviceType.Services, handledProtocols)
		if len(services) == 0 {
			return topicCandidates, common.NoSubscriptionExpected
		}

		gen := topic.New(DefaultActuatorPattern)

		oneLevelPlaceholderTopic, err := gen.Create(device.Id, "+")
		if err != nil {
			return topicCandidates, err
		}
		topicCandidates = append(topicCandidates, oneLevelPlaceholderTopic)

		for _, service := range services {
			topic, err := gen.Create(device.Id, service.LocalId)
			if err != nil {
				return topicCandidates, err
			}
			topicCandidates = append(topicCandidates, topic)
		}

		multiLevelPlaceholderTopic, err := gen.Create(device.Id, "#")
		if err != nil {
			return topicCandidates, err
		}
		topicCandidates = append(topicCandidates, multiLevelPlaceholderTopic)

		return topicCandidates, nil
	}
}
