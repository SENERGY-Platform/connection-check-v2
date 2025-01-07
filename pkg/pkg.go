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

package pkg

import (
	"context"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/auth"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/connectionlog"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/prometheus"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/providers"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/vernemq"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/worker"
	"github.com/SENERGY-Platform/device-repository/lib/client"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) error {
	metrics, err := prometheus.Start(ctx, config)
	if err != nil {
		return err
	}

	tokengen := auth.New(config)

	//try config
	_, err = tokengen.Access()
	if err != nil {
		return err
	}

	if config.ExportTotalConnected {
		repo := client.NewClient(config.DeviceRepositoryUrl, nil)
		metrics.SetOnMetricsServeRequest(getOnMetricsServeRequestHandler(tokengen, repo, metrics))
	}

	logger, err := connectionlog.New(ctx, wg, config, metrics)
	if err != nil {
		return err
	}

	deviceTypeProvider, err := providers.NewDeviceTypeProvider(config, tokengen)
	if err != nil {
		return err
	}
	deviceProvider, err := providers.NewDeviceProvider(config, tokengen, deviceTypeProvider)
	if err != nil {
		return err
	}
	hubProvider, err := providers.NewHubProvider(config, tokengen, deviceTypeProvider)
	if err != nil {
		return err
	}
	verne := vernemq.New(config, metrics)
	w, err := worker.New(config, logger, deviceProvider, hubProvider, deviceTypeProvider, verne, metrics)
	if err != nil {
		return err
	}

	err = w.RunDeviceLoop(ctx, wg)
	if err != nil {
		return err
	}

	err = w.RunHubLoop(ctx, wg)
	if err != nil {
		return err
	}

	return nil
}

func getOnMetricsServeRequestHandler(tokengen *auth.Security, deviceRepo client.Interface, metrics *prometheus.Metrics) func() {
	return func() {
		token, err := tokengen.Access()
		if err != nil {
			log.Println("ERROR:", err)
			debug.PrintStack()
			return
		}
		wg := sync.WaitGroup{}

		start := time.Now()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, connected, err, _ := deviceRepo.ListExtendedDevices(token, client.ExtendedDeviceListOptions{Limit: 1, ConnectionState: client.ConnectionStateOnline})
			if err != nil {
				log.Println("ERROR: unable to load total connected device count from permission-search;", err)
				return
			}
			metrics.TotalConnected.Set(float64(connected))
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, disconnected, err, _ := deviceRepo.ListExtendedDevices(token, client.ExtendedDeviceListOptions{Limit: 1, ConnectionState: client.ConnectionStateOffline})
			if err != nil {
				log.Println("ERROR: unable to load total disconnected device count from permission-search;", err)
				return
			}
			metrics.TotalDisconnected.Set(float64(disconnected))
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, connected, err, _ := deviceRepo.ListExtendedHubs(token, client.HubListOptions{Limit: 1, ConnectionState: client.ConnectionStateOnline})
			if err != nil {
				log.Println("ERROR: unable to load total connected hub count from permission-search;", err)
				return
			}
			metrics.TotalHubsConnected.Set(float64(connected))
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, disconnected, err, _ := deviceRepo.ListExtendedHubs(token, client.HubListOptions{Limit: 1, ConnectionState: client.ConnectionStateOffline})
			if err != nil {
				log.Println("ERROR: unable to load total disconnected hub count from permission-search;", err)
				return
			}
			metrics.TotalHubsDisconnected.Set(float64(disconnected))
		}()

		wg.Wait()
		metrics.PermissionsRequestDurationForConnectionMetrics.Set(float64(time.Since(start).Milliseconds()))
	}
}
