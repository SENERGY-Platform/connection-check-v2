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

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
)

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

	TotalConnected    prometheus.Gauge
	TotalDisconnected prometheus.Gauge

	TotalHubsConnected    prometheus.Gauge
	TotalHubsDisconnected prometheus.Gauge

	PermissionsRequestDurationForConnectionMetrics prometheus.Gauge

	httphandler http.Handler

	onMetricsServeRequest func()
}

func NewMetrics(prefix string) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		httphandler: promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				Registry: reg,
			},
		),
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
		TotalConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_total_connected",
			Help: "total count of all connected devices",
		}),
		TotalDisconnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_total_disconnected",
			Help: "total count of all disconnected devices",
		}),
		TotalHubsConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_hubs_total_connected",
			Help: "total count of all connected hubs",
		}),
		TotalHubsDisconnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_hubs_total_disconnected",
			Help: "total count of all disconnected hubs",
		}),
		PermissionsRequestDurationForConnectionMetrics: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_permissions_request_duration_for_connection_metrics_ms",
			Help: "time needed for permissions-search requests that count (dis)connected hubs and devices for metrics",
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
	reg.MustRegister(m.TotalConnected)
	reg.MustRegister(m.TotalDisconnected)
	reg.MustRegister(m.TotalHubsConnected)
	reg.MustRegister(m.TotalHubsDisconnected)
	reg.MustRegister(m.PermissionsRequestDurationForConnectionMetrics)

	return m
}

func (this *Metrics) SetOnMetricsServeRequest(f func()) {
	this.onMetricsServeRequest = f
}

func (this *Metrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	log.Printf("%v [%v] %v \n", request.RemoteAddr, request.Method, request.URL)
	if this.onMetricsServeRequest != nil {
		this.onMetricsServeRequest()
	}
	this.httphandler.ServeHTTP(writer, request)
}
