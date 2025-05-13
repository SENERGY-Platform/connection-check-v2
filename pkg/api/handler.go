package api

import (
	"encoding/json"
	"fmt"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

// PostStatesRefreshDevices godoc
// @Summary Refresh devices states
// @Description Refresh the states of multiple devices.
// @Tags States
// @Accept json
// @Produce	json
// @Param devices body []model.StatesRefreshRequestItem true "list of devices"
// @Success	200
// @Success	207 {array} model.StatesRefreshResponseErrItem "list of error messages mapped to device IDs"
// @Failure 400 {string} string "error message"
// @Failure	500 {array} model.StatesRefreshResponseErrItem "list of error messages mapped to device IDs"
// @Router /states/refresh/devices [post]
func PostStatesRefreshDevices(_ configuration.Config, worker Worker) (method, path string, handlerFunc http.HandlerFunc) {
	return http.MethodPost, "/states/refresh/devices", func(w http.ResponseWriter, r *http.Request) {
		defer recoverFromPanic(w)
		var reqItems []model.StatesRefreshRequestItem
		err := json.NewDecoder(r.Body).Decode(&reqItems)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var errs []model.StatesRefreshResponseErrItem
		for _, reqItem := range reqItems {
			if err = worker.CheckDeviceState(reqItem.ID, reqItem.LMResult, reqItem.SubResult); err != nil {
				log.Println("ERROR: device state refresh failed:", err)
				errs = append(errs, model.StatesRefreshResponseErrItem{ID: reqItem.ID, Error: err.Error()})
			}
		}
		if len(errs) > 0 {
			if err = json.NewEncoder(w).Encode(errs); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			if len(errs) == len(reqItems) {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusMultiStatus)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// PatchStatesRefreshDevice godoc
// @Summary Refresh device state
// @Description Refresh the state of a device.
// @Tags States
// @Produce	plain
// @Param id path string true "device ID"
// @Param lm_result query integer false "result of last message check by caller"
// @Param sub_result query integer false "result of subscription check by caller"
// @Success	200
// @Failure	400 {string} string "error message"
// @Failure	500 {string} string "error message"
// @Router /states/refresh/devices/{id} [patch]
func PatchStatesRefreshDevice(_ configuration.Config, worker Worker) (method, path string, handlerFunc http.HandlerFunc) {
	return http.MethodPatch, "/states/refresh/devices/{id}", func(w http.ResponseWriter, r *http.Request) {
		defer recoverFromPanic(w)
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "missing device id", http.StatusBadRequest)
			return
		}
		lmResult, subResult, err := parseStatesRefreshDeviceQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err = worker.CheckDeviceState(id, lmResult, subResult); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func Swagger(_ configuration.Config, _ Worker) (method, path string, handlerFunc http.HandlerFunc) {
	return http.MethodGet, "/doc", func(w http.ResponseWriter, r *http.Request) {
		defer recoverFromPanic(w)
		file, err := os.Open("docs/swagger.json")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer file.Close()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if _, err = io.Copy(w, file); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func recoverFromPanic(writer http.ResponseWriter) {
	if r := recover(); r != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		log.Printf("recovered from panic:\n%v", r)
	}
}

func parseStatesRefreshDeviceQuery(values url.Values) (lmResult, subResult int, err error) {
	if v := values.Get("lm_result"); v != "" {
		tmp, err := strconv.ParseInt(v, 10, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("parsing lm_result failed: %s", err)
		}
		lmResult = int(tmp)
	}
	if v := values.Get("sub_result"); v != "" {
		tmp, err := strconv.ParseInt(v, 10, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("parsing sub_result failed: %s", err)
		}
		subResult = int(tmp)
	}
	return
}
