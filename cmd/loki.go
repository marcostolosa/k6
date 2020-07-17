/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2020 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// TODO move this to it's own package
// benchmark it

type lokiHook struct {
	addr             string
	additionalParams [][2]string
	ch               chan *logrus.Entry
	limit            int
	levels           []logrus.Level
	pushPeriod       time.Duration
}

func lokiFromConfigLine(line string) (*lokiHook, error) {
	h := &lokiHook{
		addr:       "http://127.0.0.1:3100/loki/api/v1/push",
		limit:      100,
		levels:     logrus.AllLevels,
		pushPeriod: time.Second * 1, // TODO configurable,
	}
	if line == "loki" {
		return h, nil
	}

	parts := strings.SplitN(line, "=", 2)
	if parts[0] != "loki" {
		return nil, fmt.Errorf("loki configuration should be in the form `loki=url-to-push` but is `%s`", logOutput)
	}
	args := strings.Split(parts[1], ",")
	h.addr = args[0]
	// TODO use something better ... maybe
	// https://godoc.org/github.com/kubernetes/helm/pkg/strvals
	// atleast until https://github.com/loadimpact/k6/issues/926?
	if len(args) == 1 {
		return h, nil
	}

	for _, arg := range args[1:] {
		paramParts := strings.SplitN(arg, "=", 2)

		if len(paramParts) != 2 {
			return nil, fmt.Errorf("loki arguments should be in the form `address,key1=value1,key2=value2`, got %s", arg)
		}

		key := paramParts[0]
		value := paramParts[1]
		switch key {
		case "additionalParams":
			values := strings.Split(value, ";") // ; because , is already used

			h.additionalParams = make([][2]string, len(values))
			for i, value := range values {
				additionalParamParts := strings.SplitN(value, "=", 2)
				if len(additionalParamParts) != 2 {
					return nil, fmt.Errorf("additionalparam should be in the form key1=value1;key2=value2, got %s", value)
				}
				h.additionalParams[i] = [2]string{additionalParamParts[0], additionalParamParts[1]}
			}
		case "limit":
			var err error
			h.limit, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("couldn't parse the loki limit as a number %w", err)
			}
		case "level":
			// TODO figure out if `tracing`,`fatal` and `panic` should be included
			h.levels = []logrus.Level{}
			switch value {
			case "debug":
				h.levels = append(h.levels, logrus.DebugLevel)
				fallthrough
			case "info":
				h.levels = append(h.levels, logrus.InfoLevel)
				fallthrough
			case "warning":
				h.levels = append(h.levels, logrus.WarnLevel)
				fallthrough
			case "error":
				h.levels = append(h.levels, logrus.ErrorLevel)
			default:
				return nil, fmt.Errorf("unknown log level %s", value)
			}
		default:
			return nil, fmt.Errorf("unknown loki config key %s", key)
		}
	}

	return h, nil
}

func (h *lokiHook) start() error {
	h.ch = make(chan *logrus.Entry, 1000)
	go h.loop()

	return nil
}

// fill one of two equally sized slices with entries and then push it while filling the other one
// TODO clean old entries after push?
// TODO this will be much faster if we can reuse rfc5424.Messages and they can use less intermediary
// buffers
func (h *lokiHook) loop() {
	var (
		entrys             = make([]*logrus.Entry, h.limit)
		entriesBeingPushed = make([]*logrus.Entry, h.limit)
		dropped            int
		count              int
		ticker             = time.NewTicker(h.pushPeriod)
		pushCh             = make(chan chan struct{})
	)

	defer close(pushCh)

	go func() {
		oldLogs := make([]*logrus.Entry, 0, h.limit)
		for ch := range pushCh {
			// TODO rewrite this when we know by what we are going to limit
			entriesBeingPushed, entrys = entrys, entriesBeingPushed
			oldCount, oldDropped := count, dropped
			count, dropped = 0, 0
			close(ch) // signal that more buffering can continue
			// TODO optimize a lot
			newslice := append(append([]*logrus.Entry{}, oldLogs...), entriesBeingPushed[:oldCount]...)
			n, err := h.push(newslice, oldDropped)
			_ = err // TODO print it on terminal ?!?
			if n > len(newslice) {
				oldLogs = oldLogs[:0]
				continue
			}
			leftOver := newslice[n:]
			oldLogs = oldLogs[:len(leftOver)]
			copy(oldLogs, leftOver)
		}
	}()

	for {
		select {
		case entry, ok := <-h.ch:
			if !ok {
				return
			}
			if count == h.limit {
				dropped++
				continue
			}
			entrys[count] = entry
			count++
		case <-ticker.C:
			ch := make(chan struct{})
			pushCh <- ch
			<-ch
		}
	}
}

func (h *lokiHook) push(entrys []*logrus.Entry, dropped int) (int, error) {
	if len(entrys) == 0 {
		return 0, nil
	}

	// Here UnixNano is used instead of Before as ... before doesn't order by the value of UnixNano
	// ... somehow. To be investigated further
	sort.Slice(entrys, func(i, j int) bool {
		return entrys[i].Time.UnixNano() < entrys[j].Time.UnixNano()
	})

	cutoff := time.Now().Add(-time.Millisecond * 500) // probably better to be configurable

	cutoffPoint := sort.Search(len(entrys), func(i int) bool {
		return !(entrys[i].Time.UnixNano() < cutoff.UnixNano())
	})

	if cutoffPoint > len(entrys) {
		cutoffPoint = len(entrys)
	}

	strms := new(lokiPushMessage)

	for _, entry := range entrys[:cutoffPoint] {
		addMsg(strms, entry, h.additionalParams)
	}
	if dropped != 0 {
		addMsg(strms,
			&logrus.Entry{
				Data: logrus.Fields{
					"droppedCount": dropped,
				},
				Level: logrus.WarnLevel,
				Message: fmt.Sprintf("k6 dropped some packages because they were above the limit of %d/%s",
					h.limit, h.pushPeriod),
				Time: cutoff,
			},
			h.additionalParams,
		)
	}

	b, err := json.Marshal(strms)
	if err != nil {
		return cutoffPoint, err
	}
	// TODO use a custom client
	res, err := http.Post(h.addr, "application/json", bytes.NewBuffer(b)) //nolint:noctx
	_, _ = io.Copy(ioutil.Discard, res.Body)
	_ = res.Body.Close()
	return cutoffPoint, err
}

func addMsg(strms *lokiPushMessage, entry *logrus.Entry, additionalParams [][2]string) {
	labels := msgToLabels(entry, additionalParams)
	var foundStrm *stream
	for _, strm := range strms.Streams {
		if reflect.DeepEqual(strm.Stream, labels) {
			foundStrm = strm
			break
		}
	}
	if foundStrm == nil {
		foundStrm = &stream{Stream: labels}
		strms.Streams = append(strms.Streams, foundStrm)
	}
	foundStrm.Values = append(foundStrm.Values,
		[2]string{strconv.FormatInt(entry.Time.UnixNano(), 10), entry.Message})
}

func msgToLabels(entry *logrus.Entry, additionalParams [][2]string) map[string]string {
	labels := make(map[string]string, 1+len(entry.Data)+len(additionalParams))
	// TODO figure out if entrys share their entry.Data and use that to not recreate the same
	// sdParams
	labels["level"] = entry.Level.String()
	for name, value := range entry.Data {
		labels[name] = fmt.Sprint(value)
	}

	for _, param := range additionalParams {
		labels[param[0]] = param[1]
	}
	return labels
}

func (h *lokiHook) Fire(entry *logrus.Entry) error {
	h.ch <- entry
	return nil
}

func (h *lokiHook) Levels() []logrus.Level {
	return h.levels
}

/*
{
  "streams": [
    {
      "stream": {
        "label": "value"
      },
      "values": [
          [ "<unix epoch in nanoseconds>", "<log line>" ],
          [ "<unix epoch in nanoseconds>", "<log line>" ]
      ]
    }
  ]
}
*/
type lokiPushMessage struct {
	Streams []*stream `json:"streams"`
}

type stream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}
