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
	"github.com/SENERGY-Platform/connection-check-v2/pkg/deviceprovider"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/devicetypes"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/vernemq"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/worker"
	"sync"
)

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) error {
	logger, err := connectionlog.New(ctx, wg, config)
	if err != nil {
		return err
	}
	tokengen := auth.New(config)
	deviceTypeProvider, err := devicetypes.New(config, tokengen)
	if err != nil {
		return err
	}
	deviceProvider, err := deviceprovider.New(config, tokengen, deviceTypeProvider)
	if err != nil {
		return err
	}
	verne := vernemq.New(config)
	w, err := worker.New(config, logger, deviceProvider, deviceTypeProvider, verne)
	if err != nil {
		return err
	}
	return w.RunDeviceLoop(ctx, wg)
}
