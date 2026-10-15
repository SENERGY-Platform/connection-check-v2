/*
 * Copyright (c) 2026 InfAI (CC SES)
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

package configuration

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestHandleEnvironmentVarsLogsNoSecret(t *testing.T) {
	t.Setenv("AUTH_CLIENT_SECRET", "secret-client-secret")
	t.Setenv("AUTH_CLIENT_ID", "secret-client-id")
	t.Setenv("VERNE_MANAGEMENT_URL", "http://user:secret-verne@vernemq:8888")
	t.Setenv("CHIRPSTACK_URL", "http://user:secret-chirpstack@chirpstack:8080")
	t.Setenv("CHIRPSTACK_TOKEN", "secret-chirpstack-token")
	t.Setenv("KAFKA_URL", "kafka:9092")

	stdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = stdout }()
	config := &Config{}
	handleEnvironmentVars(config)
	os.Stdout = stdout
	w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(out), "secret-") {
		t.Errorf("secret in log: %s", out)
	}
	if !strings.Contains(string(out), "KAFKA_URL") {
		t.Errorf("non-secret value not logged: %s", out)
	}
	if config.AuthClientSecret != "secret-client-secret" || config.ChirpstackToken != "secret-chirpstack-token" {
		t.Error("secret values not loaded")
	}
}
