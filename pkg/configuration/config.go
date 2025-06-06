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

package configuration

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	PrometheusPort                     string   `json:"prometheus_port"`
	Debug                              bool     `json:"debug"`
	TopicGenerator                     string   `json:"topic_generator"`
	KafkaUrl                           string   `json:"kafka_url"`
	DeviceRepositoryUrl                string   `json:"device_repository_url"`
	VerneManagementUrl                 string   `json:"verne_management_url"`
	HandledProtocols                   []string `json:"handled_protocols"`
	AuthEndpoint                       string   `json:"auth_endpoint"`
	AuthClientId                       string   `json:"auth_client_id"`
	AuthClientSecret                   string   `json:"auth_client_secret"`
	AuthExpirationTimeBuffer           float64  `json:"auth_expiration_time_buffer"`
	DeviceTypeCacheExpiration          string   `json:"device_type_cache_expiration"`
	HubProtocolCheckCacheExpiration    string   `json:"hub_protocol_check_cache_expiration"`
	PermissionsRequestDeviceBatchSize  int      `json:"permissions_request_device_batch_size"`
	PermissionsRequestHubBatchSize     int      `json:"permissions_request_hub_batch_size"`
	MinimalRecheckWaitDuration         string   `json:"minimal_recheck_wait_duration"`
	DeviceCheckInterval                string   `json:"device_check_interval"`
	HubCheckInterval                   string   `json:"hub_check_interval"`
	DeviceConnectionLogTopic           string   `json:"device_connection_log_topic"`
	HubConnectionLogTopic              string   `json:"hub_connection_log_topic"`
	DeviceCheckTopicHintExpiration     string   `json:"device_check_topic_hint_expiration"`
	UseDeviceCheckTopicHintExclusively bool     `json:"use_device_check_topic_hint_exclusively"`
	ExportTotalConnected               bool     `json:"export_total_connected"`
	MaxErrorCountTilFatal              int64    `json:"max_error_count_til_fatal"`
	HttpRequestTimeout                 string   `json:"http_request_timeout"`
	LastMessageDBUrl                   string   `json:"last_message_db_url"`
	UseUTC                             bool     `json:"use_utc"`
	ServerPort                         string   `json:"server_port"`
	ApiDocsProviderBaseUrl             string   `json:"api_docs_provider_base_url"`
}

// loads config from json in location and used environment variables (e.g KafkaUrl --> KAFKA_URL)
func Load(location string) (config Config, err error) {
	file, err := os.Open(location)
	if err != nil {
		return config, err
	}
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		return config, err
	}
	handleEnvironmentVars(&config)
	return config, nil
}

var camel = regexp.MustCompile("(^[^A-Z]*|[A-Z]*)([A-Z][^A-Z]+|$)")

func fieldNameToEnvName(s string) string {
	var a []string
	for _, sub := range camel.FindAllStringSubmatch(s, -1) {
		if sub[1] != "" {
			a = append(a, sub[1])
		}
		if sub[2] != "" {
			a = append(a, sub[2])
		}
	}
	return strings.ToUpper(strings.Join(a, "_"))
}

// preparations for docker
func handleEnvironmentVars(config *Config) {
	configValue := reflect.Indirect(reflect.ValueOf(config))
	configType := configValue.Type()
	for index := 0; index < configType.NumField(); index++ {
		fieldName := configType.Field(index).Name
		fieldConfig := configType.Field(index).Tag.Get("config")
		envName := fieldNameToEnvName(fieldName)
		envValue := os.Getenv(envName)
		if envValue != "" {
			if !strings.Contains(fieldConfig, "secret") {
				fmt.Println("use environment variable: ", envName, " = ", envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Int64 || configValue.FieldByName(fieldName).Kind() == reflect.Int {
				i, _ := strconv.ParseInt(envValue, 10, 64)
				configValue.FieldByName(fieldName).SetInt(i)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.String {
				configValue.FieldByName(fieldName).SetString(envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Bool {
				b, _ := strconv.ParseBool(envValue)
				configValue.FieldByName(fieldName).SetBool(b)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Float64 {
				f, _ := strconv.ParseFloat(envValue, 64)
				configValue.FieldByName(fieldName).SetFloat(f)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Slice {
				val := []string{}
				for _, element := range strings.Split(envValue, ",") {
					val = append(val, strings.TrimSpace(element))
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(val))
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Map {
				value := map[string]string{}
				for _, element := range strings.Split(envValue, ",") {
					keyVal := strings.Split(element, ":")
					key := strings.TrimSpace(keyVal[0])
					val := strings.TrimSpace(keyVal[1])
					value[key] = val
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(value))
			}
		}
	}
}
