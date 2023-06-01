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
	"context"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"log"
	"sync"
	"time"
)

func (this *Worker) RunHubLoop(ctx context.Context, wg *sync.WaitGroup) error {
	if this.config.HubCheckInterval == "" || this.config.HubCheckInterval == "-" {
		return nil
	}
	if this.config.TopicGenerator != "senergy" {
		log.Println("hub connection check is currently only for the senergy connector supported")
		return nil
	}
	dur, err := time.ParseDuration(this.config.HubCheckInterval)
	if err != nil {
		return err
	}
	t := time.NewTicker(dur)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-t.C:
				err = this.runHubCheck()
				if err != nil {
					log.Println("ERROR:", err)
				}
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
	}()
	return nil
}

func (this *Worker) runHubCheck() error {
	this.metrics.HubsChecked.Inc()
	start := time.Now()
	hub, err := this.hubprovider.GetNextHub()
	if err != nil {
		return err
	}
	isOnline, err := this.verne.CheckClient(hub.Id)
	if err != nil {
		return err
	}
	this.metrics.HubCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	annotation, ok := hub.Annotations[ConnectionStateAnnotation]
	if !ok {
		return this.updateHubState(hub, isOnline)
	}
	expected, ok := annotation.(bool)
	if !ok {
		log.Printf("WARNING: unexpected hub state anotation in %#v", hub)
		return this.updateHubState(hub, isOnline)
	}
	if expected != isOnline {
		return this.updateHubState(hub, isOnline)
	}
	return nil
}

func (this *Worker) updateHubState(hub model.PermHub, online bool) error {
	if online {
		return this.logger.LogHubConnect(hub.Id)
	} else {
		return this.logger.LogHubDisconnect(hub.Id)
	}
}
