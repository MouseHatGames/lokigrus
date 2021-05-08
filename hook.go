package lokigrus

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

type entry struct {
	entry *logrus.Entry
	str   string
}

type Hook struct {
	LokiURL       string
	MaxBatchAge   time.Duration
	MaxBatchCount int
	Data          map[string]string

	lineChan chan *entry
	batch    []*entry
	maxTime  *time.Timer
}

type Option func(*Hook)

func MaxBatchAge(age time.Duration) Option {
	return func(h *Hook) {
		h.MaxBatchAge = age
	}
}

func MaxBatchCount(count int) Option {
	return func(h *Hook) {
		h.MaxBatchCount = count
	}
}

func Data(data map[string]string) Option {
	return func(h *Hook) {
		h.Data = data
	}
}

func NewHook(lokiURL string, opts ...Option) *Hook {
	h := &Hook{
		LokiURL: lokiURL,
	}

	for _, opt := range opts {
		opt(h)
	}

	go h.start()
	return h
}

func (l *Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (l *Hook) Fire(ll *logrus.Entry) error {
	str, err := ll.String()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to format line: %s", err)
		return err
	}

	l.lineChan <- &entry{ll, str}
	return nil
}

func (l *Hook) Close() error {
	close(l.lineChan)
	return nil
}

func (l *Hook) start() {
	l.maxTime = time.NewTimer(l.MaxBatchAge)

	defer l.mustSendBatch()

loop:
	for {
		select {
		case ll, ok := <-l.lineChan:
			if !ok {
				break loop
			}

			l.batch = append(l.batch, ll)

			if len(l.batch) >= l.MaxBatchCount {
				l.mustSendBatch()
			}

		case <-l.maxTime.C:
			l.mustSendBatch()
		}
	}
}

func (l *Hook) mustSendBatch() {
	if len(l.batch) == 0 {
		return
	}

	l.maxTime.Reset(l.MaxBatchAge)

	labels := formatLabels(l.Data)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sendBatch(ctx, l.batch, labels, l.LokiURL); err != nil {
		fmt.Fprintf(os.Stderr, "failed to send batch: %s", err)
	}

	l.batch = nil
}
