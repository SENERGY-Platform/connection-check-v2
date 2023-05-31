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
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/patrickmn/go-cache"
	"log"
)

func (this *Worker) getHint(device model.PermDevice) (hint string, used bool) {
	var temp interface{}
	temp, used = this.hintstore.Get(device.Id)
	if used {
		hint, used = temp.(string)
		if !used {
			log.Printf("ERROR: unable to interpret stored topic hint as string: %v %#v\n", device.Id, temp)
		}
	}
	return
}

func (this *Worker) storeHint(device model.PermDevice, topic string) {
	this.hintstore.Set(device.Id, topic, cache.DefaultExpiration)
}
