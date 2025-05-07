package providers

import (
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/topicgenerator/common"
	"reflect"
	"testing"
	"time"
)

func TestLastMessageStateProvider_CheckLastMessages(t *testing.T) {
	now := time.Now()
	lastMsgTime := now.Add(time.Second * -1)
	lmClientMock := &lastMessageClientMock{
		values: map[string]time.Time{
			"d1s1": now.Add(time.Minute * -30),
			"d1s2": now.Add(time.Hour * -2),
			"d1s3": lastMsgTime,
			"d2s1": now.Add(time.Hour * -2),
		},
	}
	p := NewLastMessageStateProvider(lmClientMock, false)
	t.Run("is online", func(t *testing.T) {
		state, err := p.CheckLastMessages("d1", []string{"s1", "s2", "s3"}, time.Hour)
		if err != nil {
			t.Error(err)
		}
		if !state {
			t.Error("expected true")
		}
		a := lmsCacheItem{
			LastMsg: lastMsgTime,
			MaxAge:  time.Hour,
		}
		b, ok := p.cache["d1"]
		if !ok {
			t.Error("item not in cache")
		}
		if !reflect.DeepEqual(a, b) {
			t.Errorf("expected: %v, got: %v", a, b)
		}
	})
	t.Run("is offline", func(t *testing.T) {
		state, err := p.CheckLastMessages("d2", []string{"s1"}, time.Hour)
		if err != nil {
			t.Error(err)
		}
		if state {
			t.Error("expected false")
		}
		if _, ok := p.cache["d2"]; ok {
			t.Error("item should not be in cache")
		}
	})
	t.Run("update cache", func(t *testing.T) {
		state, err := p.CheckLastMessages("d1", []string{"s1", "s2", "s3"}, time.Hour*2)
		if err != nil {
			t.Error(err)
		}
		if !state {
			t.Error("expected true")
		}
		item, ok := p.cache["d1"]
		if !ok {
			t.Error("item not in cache")
		}
		if item.MaxAge != time.Hour*2 {
			t.Errorf("expected %v, got %v", time.Hour*2, item.MaxAge)
		}
	})
	t.Run("error", func(t *testing.T) {
		_, err := p.CheckLastMessages("d2", []string{"s1", "s2"}, time.Hour)
		if err == nil {
			t.Error("expected error")
		}
	})
}

func TestLastMessageStateProvider_cleanCache(t *testing.T) {
	now := time.Now()
	p := LastMessageStateProvider{
		cache: map[string]lmsCacheItem{
			"a": {LastMsg: now.Add(time.Minute * -30), MaxAge: time.Hour},
			"b": {LastMsg: now.Add(time.Hour * -2), MaxAge: time.Hour},
		},
	}
	p.cleanCache()
	if _, ok := p.cache["a"]; !ok {
		t.Error("item not in cache")
	}
	if _, ok := p.cache["b"]; ok {
		t.Error("item still in cache")
	}
}

type lastMessageClientMock struct {
	values map[string]time.Time
}

func (m *lastMessageClientMock) GetLastMessageTime(deviceID, serviceID string) (time.Time, error) {
	timestamp, ok := m.values[deviceID+serviceID]
	if !ok {
		return time.Time{}, errors.New("not found")
	}
	return timestamp, nil
}

func TestName(t *testing.T) {
	tSubOK := !errors.Is(nil, common.NoSubscriptionExpected)
	fmt.Println(tSubOK)
}
