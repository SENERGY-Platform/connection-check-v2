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

package prometheus

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	TopicsChecked       prometheus.Counter
	TopicCheckLatencyMs prometheus.Gauge

	ClientsChecked       prometheus.Counter
	ClientCheckLatencyMs prometheus.Gauge

	DevicesChecked       prometheus.Counter
	DeviceCheckLatencyMs prometheus.Gauge

	HubsChecked       prometheus.Counter
	HubCheckLatencyMs prometheus.Gauge

	SendDeviceConnected    prometheus.Counter
	SendDeviceDisconnected prometheus.Counter
	SendHubConnected       prometheus.Counter
	SendHubDisconnected    prometheus.Counter
}

func NewMetrics(prefix string) *Metrics {
	return NewMetricsWithRegistry(prefix, prometheus.NewRegistry())
}

func NewMetricsWithRegistry(prefix string, reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		TopicsChecked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_topics_checked",
			Help: "count of devices checked since connection check startup",
		}),
		TopicCheckLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_topic_check_latency_ms",
			Help: "latency of device check in ms",
		}),
		ClientsChecked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_clients_checked",
			Help: "count of devices checked since connection check startup",
		}),
		ClientCheckLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_client_check_latency_ms",
			Help: "latency of device check in ms",
		}),
		DevicesChecked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_devices_checked",
			Help: "count of devices checked since connection check startup",
		}),
		DeviceCheckLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_device_check_latency_ms",
			Help: "latency of device check in ms",
		}),
		HubsChecked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_hubs_checked",
			Help: "count of hubs checked since connection check startup",
		}),
		HubCheckLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_hub_check_latency_ms",
			Help: "latency of hub check in ms",
		}),
		SendDeviceConnected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_send_device_connected_count",
			Help: "count of send device connected messages since connection check startup",
		}),
		SendDeviceDisconnected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_send_device_disconnected_count",
			Help: "count of send device disconnected messages since connection check startup",
		}),
		SendHubConnected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_send_hub_connected_count",
			Help: "count of send hub connected messages since connection check startup",
		}),
		SendHubDisconnected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_send_hub_disconnected_count",
			Help: "count of send hub disconnected messages since connection check startup",
		}),
	}

	reg.MustRegister(m.TopicsChecked)
	reg.MustRegister(m.TopicCheckLatencyMs)
	reg.MustRegister(m.ClientsChecked)
	reg.MustRegister(m.ClientCheckLatencyMs)
	reg.MustRegister(m.DevicesChecked)
	reg.MustRegister(m.DeviceCheckLatencyMs)
	reg.MustRegister(m.HubsChecked)
	reg.MustRegister(m.HubCheckLatencyMs)
	reg.MustRegister(m.SendDeviceConnected)
	reg.MustRegister(m.SendDeviceDisconnected)
	reg.MustRegister(m.SendHubConnected)
	reg.MustRegister(m.SendHubDisconnected)

	return m
}