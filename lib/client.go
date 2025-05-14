package lib

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Client struct {
	client  HTTPClient
	baseUrl string
}

func New(httpClient HTTPClient, baseUrl string) *Client {
	return &Client{
		client:  httpClient,
		baseUrl: baseUrl,
	}
}

func (c *Client) RefreshDeviceState(deviceID string, lmResult, subResult int) error {
	u, err := url.JoinPath(c.baseUrl, "states/refresh/devices", url.PathEscape(deviceID))
	if err != nil {
		return err
	}
	var qParams []string
	if lmResult != 0 {
		qParams = append(qParams, fmt.Sprintf("%s=%d", LMResultParam, lmResult))
	}
	if subResult != 0 {
		qParams = append(qParams, fmt.Sprintf("%s=%d", SubResultParam, subResult))
	}
	if len(qParams) > 0 {
		u += "?" + strings.Join(qParams, "&")
	}
	req, err := http.NewRequest(http.MethodPatch, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, err := io.ReadAll(resp.Body)
		if err != nil || len(b) == 0 {
			return NewResponseError(resp.StatusCode, resp.Status)
		}
		return NewResponseError(resp.StatusCode, string(b))
	}
	_, _ = io.ReadAll(resp.Body)
	return nil
}

func (c *Client) RefreshDevicesStates(devices []StatesRefreshRequestItem) error {
	u, err := url.JoinPath(c.baseUrl, "states/refresh/devices")
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(nil)
	err = json.NewEncoder(buffer).Encode(devices)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, u, buffer)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusMultiStatus:
		rErr := NewResponseError(resp.StatusCode, resp.Status)
		var errItems []StatesRefreshResponseErrItem
		err = json.NewDecoder(resp.Body).Decode(&errItems)
		if err != nil {
			return NewMultiStatusError(errors.Join(rErr, err), nil)
		}
		return NewMultiStatusError(rErr, errItems)
	case resp.StatusCode >= http.StatusMultipleChoices:
		b, err := io.ReadAll(resp.Body)
		if err != nil || len(b) == 0 {
			return NewResponseError(resp.StatusCode, resp.Status)
		}
		return NewResponseError(resp.StatusCode, string(b))
	}
	_, _ = io.ReadAll(resp.Body)
	return nil
}
