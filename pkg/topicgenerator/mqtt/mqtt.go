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
	"github.com/SENERGY-Platform/mqtt-platform-connector/lib/shortid"
	"github.com/SENERGY-Platform/mqtt-platform-connector/lib/topic"
	"slices"
	"sort"
	"strings"
)

const DefaultActuatorPattern = "{{.DeviceId}}/cmnd/{{.LocalServiceId}}"

func init() {
	known.Generators["mqtt"] = TopicGenerator
}

func TopicGenerator(config configuration.Config, deviceTypeProvider common.DeviceTypeProvider, device model.ExtendedDevice) (topicCandidates []string, err error) {
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

	gen := topic.New(nil, DefaultActuatorPattern)

	serviceTopicCandidates := [][]string{}
	for _, service := range services {
		t, err := gen.Create(device.Id, service.LocalId)
		if err != nil {
			return topicCandidates, err
		}
		shortDeviceId, err := shortid.ShortId(device.Id)
		if err != nil {
			return topicCandidates, err
		}
		candidates := PermuteWildcards(t, []string{device.Id, shortDeviceId})
		if len(candidates) > 0 {
			serviceTopicCandidates = append(serviceTopicCandidates, candidates)
		}
	}

	sort.Slice(serviceTopicCandidates, func(i, j int) bool {
		return len(serviceTopicCandidates[i]) < len(serviceTopicCandidates[j])
	})
	if len(serviceTopicCandidates) > 0 {
		return serviceTopicCandidates[0], nil
	}
	return topicCandidates, nil
}

func PermuteWildcards(topic string, fixedParts []string) (result []string) {
	singleLevelWildcards := PermuteSingleLevelWildcards(topic, fixedParts)
	result = []string{}
	for _, t := range singleLevelWildcards {
		result = append(result, PermuteMultiLevelWildcards(t, fixedParts)...)
	}
	result = distinct(result)
	SortByWildcardCount(result)
	return result
}

func PermuteMultiLevelWildcards(t string, fixedParts []string) []string {
	result := []string{t}
	parts := strings.Split(t, "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if slices.Contains(fixedParts, parts[i]) {
			return result
		}
		prefix := parts[:i]
		withWildcard := append(prefix, "#")
		result = append(result, strings.Join(withWildcard, "/"))
	}
	return result
}

func PermuteSingleLevelWildcards(topic string, fixedParts []string) (result []string) {
	parts := strings.Split(topic, "/")
	return permuteSingleLevelWildcards(parts, fixedParts)
}

func permuteSingleLevelWildcards(topicParts []string, fixedParts []string) (result []string) {
	if len(topicParts) == 0 {
		return []string{}
	}
	current := topicParts[0]
	fixed := slices.Contains(fixedParts, current)
	if len(topicParts) == 1 {
		if fixed {
			return []string{current}
		} else {
			return []string{current, "+"}
		}
	}
	result = []string{strings.Join(topicParts, "/")}
	rest := topicParts[1:]
	temp := permuteSingleLevelWildcards(rest, fixedParts)
	for _, e := range temp {
		if !fixed {
			result = append(result, "+/"+e)
		}
		result = append(result, current+"/"+e)
	}
	return distinct(result)
}

func distinct(topics []string) (result []string) {
	result = []string{}
	index := map[string]bool{}
	for _, t := range topics {
		if !index[t] {
			result = append(result, t)
		}
		index[t] = true
	}
	return result
}

func SortByWildcardCount(list []string) {
	sort.Slice(list, func(i, j int) bool {
		lenIWildcards := strings.Count(list[i], "+") + strings.Count(list[i], "#")
		lenJWildcards := strings.Count(list[j], "+") + strings.Count(list[j], "#")
		if lenIWildcards == lenJWildcards {
			return list[i] < list[j]
		}
		return lenIWildcards < lenJWildcards
	})
}
