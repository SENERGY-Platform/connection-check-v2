/*
 * Copyright 2021 InfAI (CC SES)
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

package docker

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"io"
	"log"
	"os"
)

func Dockerlog(ctx context.Context, container testcontainers.Container, name string) error {
	l, err := container.Logs(ctx)
	if err != nil {
		return err
	}
	out := &LogWriter{logger: log.New(os.Stdout, "["+name+"] ", log.LstdFlags)}
	go func() {
		_, err := io.Copy(out, l)
		if err != nil {
			log.Println("ERROR: unable to copy docker log", err)
		}
	}()
	return nil
}

type LogWriter struct {
	logger *log.Logger
}

func (this *LogWriter) Write(p []byte) (n int, err error) {
	this.logger.Print(string(p))
	return len(p), nil
}
