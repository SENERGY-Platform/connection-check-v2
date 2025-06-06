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

package docker

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

func VernemqttErlio(ctx context.Context, wg *sync.WaitGroup) (brokerUrl string, apiUrl string, err error) {
	log.Println("start mqtt")

	ports := []string{"1883/tcp", "8888/tcp"}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/senergy-platform/vernemq:prod",
			Tmpfs:        map[string]string{},
			ExposedPorts: ports,
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("1883/tcp"),
				wait.ForLog("loading modules"),
			),
			AlwaysPullImage: true,
			Env: map[string]string{
				"DOCKER_VERNEMQ_ALLOW_ANONYMOUS":            "on",
				"DOCKER_VERNEMQ_LOG__CONSOLE__LEVEL":        "debug",
				"DOCKER_VERNEMQ_SHARED_SUBSCRIPTION_POLICY": "random",
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
		//time.Sleep(time.Second)
		//log.Println("read logs form container mqtt:", Dockerlog(c, "MQTT"))
		log.Println("DEBUG: remove container mqtt", c.Terminate(context.Background()))
	}()

	time.Sleep(2 * time.Second)

	_, out, err := c.Exec(ctx, []string{"vmq-admin", "api-key", "add", "key=testkey"})
	if err != nil {
		return "", "", err
	}
	_, err = io.Copy(os.Stdout, out)
	log.Println("print cmt out:", err)

	ipAddress, err := c.ContainerIP(ctx)
	if err != nil {
		return "", "", err
	}

	brokerUrl = "tcp://" + ipAddress + ":1883"
	apiUrl = "http://testkey@" + ipAddress + ":8888"

	return brokerUrl, apiUrl, err

}
