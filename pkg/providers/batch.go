/*
 * Copyright (c) 2024 InfAI (CC SES)
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

package providers

import (
	"errors"
	"sync"
)

type Batch[T any] struct {
	window      []T
	windowIndex int
	offset      int64
	getWindow   func(offset int64) ([]T, error)
	check       func(T) (bool, error)
	mux         sync.Mutex
}

func NewBatch[T any](getter func(offset int64) ([]T, error), checker func(T) (bool, error)) *Batch[T] {
	return &Batch[T]{
		getWindow: getter,
		check:     checker,
	}
}

var BatchNotInWindowErr = errors.New("no matching element in batch window")
var BatchNoMatchAfterMultipleResets = errors.New("no matching element found after multiple resets")

func (this *Batch[T]) GetNext() (result T, resets int, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	for resets = 0; resets < 2; {
		result, err = this.getFromWindow()
		if err == nil {
			return result, resets, nil
		}
		if errors.Is(err, BatchNotInWindowErr) {
			newOffset := this.offset + int64(len(this.window))
			newWindow, err := this.getWindow(newOffset)
			if err != nil {
				return result, resets, err
			}
			this.offset = newOffset
			this.window = newWindow
			if len(this.window) == 0 {
				resets = resets + 1
				this.offset = 0
			}
		}
	}
	return result, resets, BatchNoMatchAfterMultipleResets
}

func (this *Batch[T]) getFromWindow() (result T, err error) {
	for this.windowIndex < len(this.window) {
		element := this.window[this.windowIndex]
		this.windowIndex = this.windowIndex + 1
		ok, err := this.check(element)
		if err != nil {
			return result, err
		}
		if ok {
			return element, nil
		}
	}
	this.windowIndex = 0
	return result, BatchNotInWindowErr
}
