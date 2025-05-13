package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"log"
	"net/http"
	"runtime/debug"
	"sync"
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
	log.Println("start api")
	router := http.NewServeMux()
	for _, route := range routes {
		method, path, handlerFunc := route(config, worker)
		log.Println("add route", method, path)
		router.HandleFunc(fmt.Sprintf("%s %s", method, path), handlerFunc)
	}
	server := &http.Server{Addr: ":" + config.ServerPort, Handler: router}
	go func() {
		log.Println("listening on", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			debug.PrintStack()
			log.Fatal("FATAL:", err)
		}
	}()
	wg.Add(1)
	go func() {
		<-ctx.Done()
		log.Println("api shutdown", server.Shutdown(context.Background()))
		wg.Done()
	}()
	return
}
