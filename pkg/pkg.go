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
	"sync"
)

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) error {
	metrics, err := prometheus.Start(ctx, config)
	if err != nil {
		return err
	}

	logger, err := connectionlog.New(ctx, wg, config, metrics)
	if err != nil {
		return err
	}
	tokengen := auth.New(config)
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
