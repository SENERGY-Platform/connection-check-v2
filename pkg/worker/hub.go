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
	"errors"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	"github.com/SENERGY-Platform/models/go/models"
	"log"
	"sync"
	"time"
)

func (this *Worker) RunHubLoop(ctx context.Context, wg *sync.WaitGroup) error {
	if this.config.HubCheckInterval == "" || this.config.HubCheckInterval == "-" {
		return nil
	}
	batchLoopStartTime := time.Now()
	if this.config.TopicGenerator != "senergy" {
		log.Println("hub connection check is currently only for the senergy connector supported")
		return nil
	}
	dur, err := time.ParseDuration(this.config.HubCheckInterval)
	if err != nil {
		return err
	}
	errHandler := getErrorHandler(this.config)
	t := time.NewTicker(dur)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-t.C:
				var isFirstDeviceOfBatchLoopRepeat bool
				isFirstDeviceOfBatchLoopRepeat, err = this.runHubCheck()
				errHandler(err)
				if isFirstDeviceOfBatchLoopRepeat {
					since := time.Since(batchLoopStartTime)
					if since < this.minimalRecheckWait {
						time.Sleep(this.minimalRecheckWait - since)
					}
					batchLoopStartTime = time.Now()
				}
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
	}()
	return nil
}

func (this *Worker) runHubCheck() (isFirstDeviceOfBatchLoopRepeat bool, err error) {
	this.metrics.HubsChecked.Inc()
	start := time.Now()
	hub := models.ExtendedHub{}
	hub, isFirstDeviceOfBatchLoopRepeat, err = this.hubprovider.GetNextHub()
	if errors.Is(err, providers.ErrNoMatchingDevice) {
		log.Println("no hub to check found")
		return isFirstDeviceOfBatchLoopRepeat, nil
	}
	if err != nil {
		return isFirstDeviceOfBatchLoopRepeat, err
	}
	isOnline, err := this.verne.CheckClient(hub.Id)
	if err != nil {
		return isFirstDeviceOfBatchLoopRepeat, err
	}
	this.metrics.HubCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	if hub.ConnectionState == models.ConnectionStateUnknown {
		return isFirstDeviceOfBatchLoopRepeat, this.updateHubState(hub, isOnline)
	}
	expected := hub.ConnectionState == models.ConnectionStateOnline
	if expected != isOnline {
		return isFirstDeviceOfBatchLoopRepeat, this.updateHubState(hub, isOnline)
	}
	return isFirstDeviceOfBatchLoopRepeat, nil
}

func (this *Worker) updateHubState(hub model.ExtendedHub, online bool) error {
	reloaded, err := this.hubprovider.GetHub(hub.Id)
	if err != nil {
		log.Println("WARNING: unable to reload hub info", err)
		return err
	}
	currentlyOnline := reloaded.ConnectionState == models.ConnectionStateOnline
	if currentlyOnline == online && reloaded.ConnectionState != models.ConnectionStateUnknown {
		return nil //connection check has been too slow and the device has already the new online state
	}
	if online {
		return this.logger.LogHubConnect(hub.Id)
	} else {
		return this.logger.LogHubDisconnect(hub.Id)
	}
}
