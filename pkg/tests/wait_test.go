/*
 * Copyright (c) 2026 InfAI (CC SES)
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

package tests

import (
	"fmt"
	"log"
	"time"
)

// waitUntil calls check every second until it returns nil or timeout is reached; it returns the last error of check.
func waitUntil(timeout time.Duration, check func() error) error {
	start := time.Now()
	for {
		err := check()
		if err == nil {
			log.Println("TEST: condition met after", time.Since(start).String())
			return nil
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("condition not met after %v: %w", timeout, err)
		}
		time.Sleep(time.Second)
	}
}
