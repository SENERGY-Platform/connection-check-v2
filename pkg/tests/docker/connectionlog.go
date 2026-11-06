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
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// connectionlogPostgresDb, connectionlogPostgresUser and connectionlogPostgresPw are what the Timescale fixture
	// provisions for the worker.
	connectionlogPostgresDb   = "connectionlog"
	connectionlogPostgresUser = "connectionlog"
	connectionlogPostgresPw   = "pw"
)

// Timescale starts the image of the production serving layer and provisions what the worker expects to find:
// the role connectionlog, the database connectionlog owned by it and the timescaledb extension in it.
// The worker creates its tables itself.
func Timescale(ctx context.Context, wg *sync.WaitGroup) (hostport string, containerip string, err error) {
	log.Println("start connectionlog timescale")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "timescale/timescaledb-ha:pg18.6-ts2.29.2",
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor:   wait.ForListeningPort("5432/tcp"),
			Env: map[string]string{
				"POSTGRES_PASSWORD": "superuser-pw",
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
		log.Println("DEBUG: remove container connectionlog timescale", c.Terminate(context.Background()))
	}()

	// the port is open during the entrypoint's temporary init run already, so retry until the final server answers
	psql := func(database string, statement string) (string, error) {
		code, reader, err := c.Exec(ctx, []string{"psql", "-U", "postgres", "-d", database, "-v", "ON_ERROR_STOP=1", "-tA", "-c", statement})
		if err != nil {
			return "", err
		}
		out, _ := io.ReadAll(reader)
		if code != 0 {
			return string(out), fmt.Errorf("psql exited with %d: %s", code, out)
		}
		return string(out), nil
	}
	deadline := time.Now().Add(120 * time.Second)
	for {
		_, err = psql("postgres", "SELECT 1")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return "", "", fmt.Errorf("timescale does not accept connections: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, step := range []struct{ database, statement string }{
		{"postgres", "CREATE ROLE " + connectionlogPostgresUser + " LOGIN PASSWORD '" + connectionlogPostgresPw + "'"},
		{"postgres", "CREATE DATABASE " + connectionlogPostgresDb + " OWNER " + connectionlogPostgresUser},
		{connectionlogPostgresDb, "CREATE EXTENSION IF NOT EXISTS timescaledb"},
	} {
		if _, err = psql(step.database, step.statement); err != nil {
			return "", "", err
		}
	}

	containerip, err = c.ContainerIP(ctx)
	if err != nil {
		return "", "", err
	}
	temp, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return "", "", err
	}
	hostport = temp.Port()

	return hostport, containerip, err
}

func ConnectionLogWorker(ctx context.Context, wg *sync.WaitGroup, kafkaUrl, mongourl string, postgresip string, deviceRepositoryUrl string) (err error) {
	log.Println("start connectionlog-worker")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/senergy-platform/connectionlog-worker:v2.0.0",
			ExposedPorts: []string{"8080/tcp"},
			Env: map[string]string{
				"KAFKA_URL":             kafkaUrl,
				"MONGO_URL":             mongourl,
				"POSTGRES_HOST":         postgresip,
				"POSTGRES_PORT":         "5432",
				"POSTGRES_USER":         connectionlogPostgresUser,
				"POSTGRES_PW":           connectionlogPostgresPw,
				"POSTGRES_DB":           connectionlogPostgresDb,
				"DEVICE_REPOSITORY_URL": deviceRepositoryUrl,
				"DEBUG":                 "true",
			},
			WaitingFor:      wait.ForLog(`"level":"DEBUG","msg":"start kafka topic consumer","organization":"github.com/SENERGY-Platform","project":"connection-log-worker","topic":"devices"`),
			AlwaysPullImage: true,
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
