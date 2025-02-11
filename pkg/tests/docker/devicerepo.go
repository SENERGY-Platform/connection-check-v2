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

package docker

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"sync"
)

func DeviceRepoWithDependencies(basectx context.Context, wg *sync.WaitGroup) (repoUrl string, kafkaUrl string, err error) {
	ctx, cancel := context.WithCancel(basectx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	_, zkIp, err := Zookeeper(ctx, wg)
	if err != nil {
		return repoUrl, kafkaUrl, err
	}
	zookeeperUrl := zkIp + ":2181"

	kafkaUrl, err = Kafka(ctx, wg, zookeeperUrl)
	if err != nil {
		return repoUrl, kafkaUrl, err
	}

	_, mongoIp, err := MongoDB(ctx, wg)
	if err != nil {
		return repoUrl, kafkaUrl, err
	}
	mongoUrl := "mongodb://" + mongoIp + ":27017"

	_, influxip, err := Influxdb(ctx, wg)
	if err != nil {
		return repoUrl, kafkaUrl, err
	}
	influxUrl := "http://" + influxip + ":8086"

	_, permV2Ip, err := PermissionsV2(ctx, wg, mongoUrl, kafkaUrl)
	if err != nil {
		return repoUrl, kafkaUrl, err
	}
	permv2Url := "http://" + permV2Ip + ":8080"

	_, repoIp, err := DeviceRepo(ctx, wg, kafkaUrl, mongoUrl, permv2Url)
	if err != nil {
		return repoUrl, kafkaUrl, err
	}
	repoUrl = "http://" + repoIp + ":8080"

	err = ConnectionLogWorker(ctx, wg, kafkaUrl, mongoUrl, influxUrl, repoUrl)
	if err != nil {
		return repoUrl, kafkaUrl, err
	}

	return repoUrl, kafkaUrl, err
}

func DeviceRepo(ctx context.Context, wg *sync.WaitGroup, kafkaUrl string, mongoUrl string, permv2Url string) (hostPort string, ipAddress string, err error) {
	log.Println("start device-repository")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "ghcr.io/senergy-platform/device-repository:prod",
			Env: map[string]string{
				"DEBUG":                                 "true",
				"KAFKA_URL":                             kafkaUrl,
				"MONGO_URL":                             mongoUrl,
				"PERMISSIONS_V2_URL":                    permv2Url,
				"DISABLE_STRICT_VALIDATION_FOR_TESTING": "true",
			},
			ExposedPorts:    []string{"8080/tcp"},
			WaitingFor:      wait.ForListeningPort("8080/tcp"),
			AlwaysPullImage: true,
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
		log.Println("DEBUG: remove container device-repository", c.Terminate(context.Background()))
	}()

	/*
		err = Dockerlog(ctx, c, "DEVICE-REPOSITORY")
		if err != nil {
			return "", "", err
		}
	*/

	ipAddress, err = c.ContainerIP(ctx)
	if err != nil {
		return "", "", err
	}
	temp, err := c.MappedPort(ctx, "8080/tcp")
	if err != nil {
		return "", "", err
	}
	hostPort = temp.Port()

	return hostPort, ipAddress, err
}
