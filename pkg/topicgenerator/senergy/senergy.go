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

package senergy

import (
	"fmt"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/known"
)

func init() {
	known.Generators["senergy"] = func(config configuration.Config, deviceTypeProvider common.DeviceTypeProvider, device model.ExtendedDevice) (topicCandidates []string, err error) {
		topicCandidates = append(topicCandidates,
			fmt.Sprintf("command/%v/%v/+", device.OwnerId, device.LocalId),
			fmt.Sprintf("command/%v/+", device.LocalId), //fallback to old topic structure
		)
		return topicCandidates, nil
	}
}
