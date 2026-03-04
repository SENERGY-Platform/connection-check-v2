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
	"sync"
	"time"

	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	lpc "github.com/SENERGY-Platform/lorawan-platform-connector/pkg/controller"
	lpc_model "github.com/SENERGY-Platform/lorawan-platform-connector/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	chirpstack "github.com/chirpstack/chirpstack/api/go/v4/api"
)

func (this *Worker) RunHubLoop(ctx context.Context, wg *sync.WaitGroup) error {
	if this.config.HubCheckInterval == "" || this.config.HubCheckInterval == "-" {
		return nil
	}
	batchLoopStartTime := time.Now()
	if this.config.TopicGenerator != "senergy" && this.config.TopicGenerator != "lorawan" {
		this.config.GetLogger().Warn("hub connection check is currently only for the senergy or lorawan connector supported")
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
				var resets int
				resets, err = this.runHubCheck()
				errHandler(err)
				if resets > 0 {
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

func (this *Worker) runHubCheck() (resets int, err error) {
	this.metrics.HubsChecked.Inc()
	start := time.Now()
	hub := models.ExtendedHub{}
	hub, resets, err = this.hubprovider.GetNextHub()
	if errors.Is(err, providers.BatchNoMatchAfterMultipleResets) {
		this.config.GetLogger().Error("no hub to check found", "error", err)
		return resets, nil
	}
	if err != nil {
		return resets, err
	}
	if this.verne == nil {
		if this.chirpGateway == nil || this.chirpTenant == nil {
			this.config.GetLogger().Error("no connection check provider available", "hubId", hub.Id)
			return resets, nil
		}
		eui := lpc.GetHubEUI(&hub.Hub)
		if eui == nil {
			return resets, nil
		}
		ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
		defer cf()
		var limit uint32 = 1000
		var offset uint32 = 0
		for {
			// search all gateways with the same name as the hub, because chirpstack does not support searching by EUI.
			gw, err := this.chirpGateway.List(ctx, &chirpstack.ListGatewaysRequest{
				Limit:  limit,
				Offset: offset,
				Search: hub.Name,
			})
			if err != nil {
				return resets, err
			}
			for _, g := range gw.Result {
				// Check if any of the found gateways has a matching EUI.
				if &g.GatewayId == eui {
					// Check if the tenant of the gateway has a tag with the user id of the hub owner
					tenant, err := this.chirpTenant.Get(ctx, &chirpstack.GetTenantRequest{
						Id: g.TenantId,
					})
					if err != nil || tenant.Tenant == nil {
						return resets, err
					}
					for k, v := range tenant.Tenant.Tags {
						if k == lpc_model.ChirpTagUserId && v == hub.OwnerId {
							return resets, this.updateHubState(hub, g.State == chirpstack.GatewayState_ONLINE)
						}
					}
				}
			}
			if uint32(len(gw.Result)) < limit {
				break
			}
			offset += limit
		}
		return resets, nil
	}
	isOnline, err := this.verne.CheckClient(hub.Id)
	if err != nil {
		return resets, err
	}
	this.metrics.HubCheckLatencyMs.Set(float64(time.Since(start).Milliseconds()))
	if hub.ConnectionState == models.ConnectionStateUnknown {
		return resets, this.updateHubState(hub, isOnline)
	}
	expected := hub.ConnectionState == models.ConnectionStateOnline
	if expected != isOnline {
		return resets, this.updateHubState(hub, isOnline)
	}
	return resets, nil
}

func (this *Worker) updateHubState(hub model.ExtendedHub, online bool) error {
	reloaded, err := this.hubprovider.GetHub(hub.Id)
	if err != nil {
		this.config.GetLogger().Error("unable to reload hub info", "hubId", hub.Id, "error", err)
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
