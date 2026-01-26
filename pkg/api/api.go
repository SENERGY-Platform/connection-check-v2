package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"sync"

	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
)

type Worker interface {
	CheckDeviceState(deviceID string, lmResult, subResult int) error
}

// Start godoc
// @title connection check v2
// @version 0.0.0
// @description Checks and updates connection states.
// @license.name Apache-2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath /
func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, worker Worker) (err error) {
	config.GetLogger().Info("start api")
	router := http.NewServeMux()
	for _, route := range routes {
		method, path, handlerFunc := route(config, worker)
		config.GetLogger().Info("add api route", "method", method, "path", path)
		router.HandleFunc(fmt.Sprintf("%s %s", method, path), handlerFunc)
	}
	server := &http.Server{Addr: ":" + config.ServerPort, Handler: router}
	go func() {
		config.GetLogger().Info("listening", "address", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			debug.PrintStack()
			config.GetLogger().Error("FATAL ERROR", "error", err)
			log.Fatal("FATAL:", err)
		}
	}()
	wg.Add(1)
	go func() {
		<-ctx.Done()
		config.GetLogger().Info("shutdown api", "result", server.Shutdown(context.Background()))
		wg.Done()
	}()
	return
}
