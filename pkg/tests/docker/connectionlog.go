/*
 * Copyright 2025 InfAI (CC SES)
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

package docker

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"log"
	"strings"
	"sync"
)

func Influxdb(ctx context.Context, wg *sync.WaitGroup) (hostport string, containerip string, err error) {
	log.Println("start connectionlog influx")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "influxdb:1.6.3",
			ExposedPorts: []string{"8086/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("8086/tcp"),
			),
			Env: map[string]string{
				"INFLUXDB_DB":             "connectionlog",
				"INFLUXDB_ADMIN_ENABLED":  "true",
				"INFLUXDB_ADMIN_USER":     "user",
				"INFLUXDB_ADMIN_PASSWORD": "pw",
			},
		},
		Started: true,
	})
	if err != nil {
		return "", "", err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container connectionlog influx", c.Terminate(context.Background()))
	}()

	containerip, err = c.ContainerIP(ctx)
	if err != nil {
		return "", "", err
	}
	temp, err := c.MappedPort(ctx, "8086/tcp")
	if err != nil {
		return "", "", err
	}
	hostport = temp.Port()

	return hostport, containerip, err
}

func ConnectionLogWorker(ctx context.Context, wg *sync.WaitGroup, kafkaUrl, mongourl string, influxurl string, deviceRepositoryUrl string) (err error) {
	log.Println("start connectionlog-worker")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/senergy-platform/connectionlog-worker:dev",
			ExposedPorts: []string{"8080/tcp"},
			Env: map[string]string{
				"KAFKA_URL":             kafkaUrl,
				"MONGO_URL":             mongourl,
				"INFLUXDB_URL":          influxurl,
				"INFLUXDB_TIMEOUT":      "3",
				"INFLUXDB_USER":         "user",
				"INFLUXDB_PW":           "pw",
				"DEVICE_REPOSITORY_URL": deviceRepositoryUrl,
				"DEBUG":                 "true",
			},
			WaitingFor: wait.ForLog("DEBUG: consume topic: \"devices\""),
		},
		Started: true,
	})
	if err != nil {
		reader, err2 := c.Logs(context.Background())
		if err2 != nil {
			log.Println("ERROR: unable to get container log")
			return err
		}
		buf := new(strings.Builder)
		io.Copy(buf, reader)
		fmt.Println("CONNECTION-LOG-WORKER LOGS: ------------------------------------------")
		fmt.Println(buf.String())
		fmt.Println("\n---------------------------------------------------------------")
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container connectionlog-worker", c.Terminate(context.Background()))
	}()

	return err
}
