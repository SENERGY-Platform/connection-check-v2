package worker

import (
	"errors"
	"github.com/SENERGY-Platform/connection-check-v2/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"reflect"
	"testing"
	"time"
)

func TestWorker_checkLastMessages(t *testing.T) {
	lmMock := &lastMessageProviderMock{
		IsOnline:   true,
		DeviceID:   "d1",
		ServiceIDs: []string{"s1", "s3"},
		MaxAge:     time.Hour,
		t:          t,
	}
	w := &Worker{lmProvider: lmMock}
	t.Run("available", func(t *testing.T) {
		isOnline, available, err := w.checkLastMessages(model.ExtendedDevice{
			Device: models.Device{
				Id:         "d1",
				Attributes: []models.Attribute{{Key: lastMessageMaxAgeAttrKey, Value: "1h"}},
			},
			DeviceType: &models.DeviceType{Services: []models.Service{
				{Id: "s1", Interaction: models.EVENT},
				{Id: "s2", Interaction: models.REQUEST},
				{Id: "s3", Interaction: models.EVENT_AND_REQUEST},
			}},
		})
		if err != nil {
			t.Error(err)
		}
		if !available {
			t.Error("expected available true")
		}
		if !isOnline {
			t.Error("expected isOnline true")
		}
	})
	t.Run("not available", func(t *testing.T) {
		t.Run("no attribute", func(t *testing.T) {
			_, available, err := w.checkLastMessages(model.ExtendedDevice{})
			if err != nil {
				t.Error(err)
			}
			if available {
				t.Error("expected false")
			}
		})
		t.Run("no services", func(t *testing.T) {
			_, available, err := w.checkLastMessages(model.ExtendedDevice{
				Device: models.Device{
					Attributes: []models.Attribute{{Key: lastMessageMaxAgeAttrKey, Value: "1h"}},
				},
				DeviceType: &models.DeviceType{},
			})
			if err != nil {
				t.Error(err)
			}
			if available {
				t.Error("expected false")
			}
		})
	})
	t.Run("error", func(t *testing.T) {
		t.Run("attribute", func(t *testing.T) {
			_, _, err := w.checkLastMessages(model.ExtendedDevice{
				Device: models.Device{
					Id:         "d1",
					Attributes: []models.Attribute{{Key: lastMessageMaxAgeAttrKey, Value: "test"}},
				},
				DeviceType: &models.DeviceType{},
			})
			if err == nil {
				t.Error("expected error")
			}
		})
		t.Run("check last messages", func(t *testing.T) {
			lmMock.Error = errors.New("test error")
			_, _, err := w.checkLastMessages(model.ExtendedDevice{
				Device: models.Device{
					Id:         "d1",
					Attributes: []models.Attribute{{Key: lastMessageMaxAgeAttrKey, Value: "1h"}},
				},
				DeviceType: &models.DeviceType{Services: []models.Service{
					{Id: "s1", Interaction: models.EVENT},
					{Id: "s2", Interaction: models.REQUEST},
					{Id: "s3", Interaction: models.EVENT_AND_REQUEST},
				}},
			})
			if err == nil {
				t.Error("expected error")
			}
		})
	})
}

func Test_getLastMessageAttr(t *testing.T) {
	t.Run("in list", func(t *testing.T) {
		dur, ok, err := getLastMessageAttr([]models.Attribute{{Key: lastMessageMaxAgeAttrKey, Value: "1h"}})
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Error("expected true")
		}
		if dur != time.Hour {
			t.Error("expected", time.Hour, "got", dur)
		}
	})
	t.Run("not in list", func(t *testing.T) {
		_, ok, err := getLastMessageAttr([]models.Attribute{})
		if err != nil {
			t.Error(err)
		}
		if ok {
			t.Error("expected false")
		}
	})
	t.Run("error", func(t *testing.T) {
		_, _, err := getLastMessageAttr([]models.Attribute{{Key: lastMessageMaxAgeAttrKey, Value: "test"}})
		if err == nil {
			t.Error("expected error")
		}
	})
}

func Test_getRequestServiceIDs(t *testing.T) {
	a := []string{"s1", "s3"}
	b := getRequestServiceIDs([]models.Service{
		{Id: "s1", Interaction: models.EVENT},
		{Id: "s2", Interaction: models.REQUEST},
		{Id: "s3", Interaction: models.EVENT_AND_REQUEST},
	})
	if !reflect.DeepEqual(a, b) {
		t.Errorf("expected %v, got %v", a, b)
	}
}

func Test_getAttribute(t *testing.T) {
	priorities := map[string]int{"low": 1, "mid": 2, "high": 3}
	tests := []struct {
		name       string
		attributes []models.Attribute
		priorities map[string]int
		wantAttr   models.Attribute
		wantOk     bool
	}{
		{
			name:       "empty list",
			attributes: []models.Attribute{},
			priorities: priorities,
		},
		{
			name:       "key not in list",
			attributes: []models.Attribute{{Key: "other", Value: "v", Origin: "high"}},
			priorities: priorities,
		},
		{
			name: "single match",
			attributes: []models.Attribute{
				{Key: "other", Value: "x", Origin: "high"},
				{Key: "k", Value: "v", Origin: "low"},
			},
			priorities: priorities,
			wantAttr:   models.Attribute{Key: "k", Value: "v", Origin: "low"},
			wantOk:     true,
		},
		{
			name: "highest priority first",
			attributes: []models.Attribute{
				{Key: "k", Value: "high", Origin: "high"},
				{Key: "k", Value: "low", Origin: "low"},
				{Key: "k", Value: "mid", Origin: "mid"},
			},
			priorities: priorities,
			wantAttr:   models.Attribute{Key: "k", Value: "high", Origin: "high"},
			wantOk:     true,
		},
		{
			name: "highest priority middle",
			attributes: []models.Attribute{
				{Key: "k", Value: "low", Origin: "low"},
				{Key: "k", Value: "high", Origin: "high"},
				{Key: "k", Value: "mid", Origin: "mid"},
			},
			priorities: priorities,
			wantAttr:   models.Attribute{Key: "k", Value: "high", Origin: "high"},
			wantOk:     true,
		},
		{
			name: "highest priority last",
			attributes: []models.Attribute{
				{Key: "k", Value: "low", Origin: "low"},
				{Key: "k", Value: "mid", Origin: "mid"},
				{Key: "k", Value: "high", Origin: "high"},
			},
			priorities: priorities,
			wantAttr:   models.Attribute{Key: "k", Value: "high", Origin: "high"},
			wantOk:     true,
		},
		{
			name: "higher priority with other key is ignored",
			attributes: []models.Attribute{
				{Key: "k", Value: "low", Origin: "low"},
				{Key: "other", Value: "high", Origin: "high"},
			},
			priorities: priorities,
			wantAttr:   models.Attribute{Key: "k", Value: "low", Origin: "low"},
			wantOk:     true,
		},
		{
			name: "unknown origin loses against known origin",
			attributes: []models.Attribute{
				{Key: "k", Value: "unknown", Origin: "unknown"},
				{Key: "k", Value: "low", Origin: "low"},
			},
			priorities: priorities,
			wantAttr:   models.Attribute{Key: "k", Value: "low", Origin: "low"},
			wantOk:     true,
		},
		{
			name: "equal priority keeps first",
			attributes: []models.Attribute{
				{Key: "k", Value: "first", Origin: "mid"},
				{Key: "k", Value: "second", Origin: "mid"},
			},
			priorities: priorities,
			wantAttr:   models.Attribute{Key: "k", Value: "first", Origin: "mid"},
			wantOk:     true,
		},
		{
			name: "nil priorities keeps first",
			attributes: []models.Attribute{
				{Key: "k", Value: "first", Origin: "low"},
				{Key: "k", Value: "second", Origin: "high"},
			},
			wantAttr: models.Attribute{Key: "k", Value: "first", Origin: "low"},
			wantOk:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attr, ok := getAttribute(tt.attributes, "k", tt.priorities)
			if ok != tt.wantOk {
				t.Errorf("expected ok %v, got %v", tt.wantOk, ok)
			}
			if !reflect.DeepEqual(attr, tt.wantAttr) {
				t.Errorf("expected %v, got %v", tt.wantAttr, attr)
			}
		})
	}
}

type lastMessageProviderMock struct {
	IsOnline   bool
	Error      error
	DeviceID   string
	ServiceIDs []string
	MaxAge     time.Duration
	t          *testing.T
}

func (l *lastMessageProviderMock) CheckLastMessages(deviceID string, serviceIDs []string, maxAge time.Duration) (bool, error) {
	if deviceID != l.DeviceID {
		l.t.Error("expected", l.DeviceID, "got", deviceID)
	}
	if !reflect.DeepEqual(serviceIDs, l.ServiceIDs) {
		l.t.Error("expected", l.ServiceIDs, "got", serviceIDs)
	}
	if maxAge != l.MaxAge {
		l.t.Error("expected", l.MaxAge, "got", maxAge)
	}
	if l.Error != nil {
		return false, l.Error
	}
	return l.IsOnline, nil
}
