package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/configuration"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type LastMessageClientItf interface {
	GetLastMessageTime(deviceID, serviceID string) (time.Time, error)
}

type LastMessageStateProvider struct {
	cache    map[string]lmsCacheItem
	lmClient LastMessageClientItf
	utcTime  bool
	mu       sync.RWMutex
}

type lmsCacheItem struct {
	LastMsg time.Time
	MaxAge  time.Duration
}

func NewLastMessageStateProvider(lmClient LastMessageClientItf, utcTime bool) *LastMessageStateProvider {
	return &LastMessageStateProvider{
		cache:    make(map[string]lmsCacheItem),
		lmClient: lmClient,
		utcTime:  utcTime,
	}
}

func (p *LastMessageStateProvider) CheckLastMessages(deviceID string, serviceIDs []string, maxAge time.Duration) (bool, error) {
	threshold := p.timeNow().Add(maxAge * -1)
	item, ok := p.cacheGet(deviceID)
	if ok && item.LastMsg.After(threshold) {
		if item.MaxAge != maxAge {
			item.MaxAge = maxAge
			p.cacheSet(deviceID, item)
		}
		return true, nil
	}
	var timestamps []time.Time
	var errs []error
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, serviceID := range serviceIDs {
		wg.Add(1)
		go func(deviceID, serviceID string) {
			defer wg.Done()
			timestamp, err := p.lmClient.GetLastMessageTime(deviceID, serviceID)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			mu.Lock()
			timestamps = append(timestamps, timestamp)
			mu.Unlock()
		}(deviceID, serviceID)
	}
	wg.Wait()
	if len(errs) > 0 {
		return false, errors.Join(errs...)
	}
	var lastMsgTime time.Time
	for _, timestamp := range timestamps {
		if timestamp.After(lastMsgTime) {
			lastMsgTime = timestamp
		}
	}
	if lastMsgTime.After(threshold) {
		p.cacheSet(deviceID, lmsCacheItem{LastMsg: lastMsgTime, MaxAge: maxAge})
		return true, nil
	}
	return false, nil
}

func (p *LastMessageStateProvider) RunCacheCleaner(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-ticker.C:
				p.cleanCache()
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (p *LastMessageStateProvider) cacheGet(key string) (lmsCacheItem, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	item, ok := p.cache[key]
	return item, ok
}

func (p *LastMessageStateProvider) cacheSet(key string, item lmsCacheItem) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache[key] = item
}

func (p *LastMessageStateProvider) cleanCache() {
	p.mu.Lock()
	defer p.mu.Unlock()
	tmp := make(map[string]lmsCacheItem)
	now := p.timeNow()
	for key, item := range p.cache {
		threshold := now.Add(item.MaxAge * -1)
		if item.LastMsg.After(threshold) {
			tmp[key] = item
		}
	}
	p.cache = tmp
}

func (p *LastMessageStateProvider) timeNow() time.Time {
	if p.utcTime {
		return time.Now().UTC()
	}
	return time.Now()
}

type LastMessageClient struct {
	tokengen TokenGenerator
	baseUrl  string
	client   http.Client
}

type resp struct {
	Time string `json:"time"`
}

func NewLastMessageClient(config configuration.Config, tokengen TokenGenerator) (*LastMessageClient, error) {
	timeout, err := time.ParseDuration(config.HttpRequestTimeout)
	if err != nil {
		return nil, err
	}
	return &LastMessageClient{
		tokengen: tokengen,
		baseUrl:  config.LastMessageDBUrl,
		client:   http.Client{Timeout: timeout},
	}, nil
}

func (c *LastMessageClient) GetLastMessageTime(deviceID, serviceID string) (time.Time, error) {
	u, err := url.JoinPath(c.baseUrl, "last-message")
	if err != nil {
		return time.Time{}, err
	}
	u += "?device_id=" + url.QueryEscape(deviceID) + "&service_id=" + url.QueryEscape(serviceID)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return time.Time{}, err
	}
	token, err := c.tokengen.Access()
	if err != nil {
		return time.Time{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	body, err := c.execRequest(req)
	if err != nil {
		return time.Time{}, err
	}
	defer body.Close()
	var tmp resp
	err = readJSON(body, &tmp)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, tmp.Time)
}

func (c *LastMessageClient) execRequest(req *http.Request) (io.ReadCloser, error) {
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		defer res.Body.Close()
		errMsg, err := readString(res.Body)
		if err != nil || errMsg == "" {
			errMsg = res.Status
		}
		return nil, fmt.Errorf("%d - %s", res.StatusCode, errMsg)
	}
	return res.Body, nil
}

func readJSON(rc io.ReadCloser, v any) error {
	jd := json.NewDecoder(rc)
	err := jd.Decode(v)
	if err != nil {
		_, _ = io.ReadAll(rc)
		return err
	}
	return nil
}

func readString(rc io.ReadCloser) (string, error) {
	b, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
