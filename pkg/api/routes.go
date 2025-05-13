package api

import (
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"net/http"
)

var routes = []func(config configuration.Config, worker Worker) (method, path string, handlerFunc http.HandlerFunc){
	PostStatesRefreshDevices,
	PatchStatesRefreshDevice,
	Swagger,
}
