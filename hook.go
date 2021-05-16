package lokigrus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"
)

const postPath = "/api/prom/push"

var ErrInvalidJSON = errors.New("invalid json written")

type entry struct {
	time int64
	str  string
}

type Writer struct {
	Out           io.Writer
	LokiURL       string
	MaxBatchAge   time.Duration
	MaxBatchCount int
	CheckJSON     bool
	labels        string

	lineChan chan *entry
	batch    []*entry
	maxTime  *time.Timer
}

type Option func(*Writer)

// MaxBatchAge is the maximum time after a batch's first entry when the batch will be sent to Loki
// if there is at least one entry
func MaxBatchAge(age time.Duration) Option {
	return func(h *Writer) {
		h.MaxBatchAge = age
	}
}

// MaxBatchCount is the maximum amount of entries in a batch before it gets sent to Loki
func MaxBatchCount(count int) Option {
	return func(h *Writer) {
		h.MaxBatchCount = count
	}
}

// Data is the labels to be attached to the Loki stream
func Data(data map[string]string) Option {
	return func(h *Writer) {
		h.labels = formatLabels(data)
	}
}

// Output is the writer where the logs will be written to. Default is os.Stdout, and if nil no logs will be written.
func Output(w io.Writer) Option {
	return func(h *Writer) {
		h.Out = w
	}
}

// If true, all entries will be validated as JSON before being sent to Loki
func CheckJSON(check bool) Option {
	return func(h *Writer) {
		h.CheckJSON = check
	}
}

// NewWriter creates a new writer that sends the data written into it to a Loki instance.
func NewWriter(lokiURL string, opts ...Option) *Writer {
	h := &Writer{
		Out:           os.Stdout,
		LokiURL:       lokiURL,
		MaxBatchAge:   30 * time.Second,
		MaxBatchCount: 5,
		CheckJSON:     true,
		lineChan:      make(chan *entry, 5),
	}

	for _, opt := range opts {
		opt(h)
	}

	u, err := url.Parse(h.LokiURL)
	if err != nil {
		panic(err)
	}
	if !strings.Contains(u.Path, postPath) {
		u.Path = postPath
		q := u.Query()
		u.RawQuery = q.Encode()
		h.LokiURL = u.String()
	}

	if h.labels == "" {
		panic("data must be set")
	}

	go h.start()
	return h
}

func (l *Writer) Write(b []byte) (n int, err error) {
	if l.CheckJSON && !json.Valid(b) {
		return 0, ErrInvalidJSON
	}

	l.lineChan <- &entry{time.Now().Unix(), string(b)}

	if l.Out != nil {
		return l.Out.Write(b)
	}
	return len(b), nil
}

// Close closes the input channel.
func (l *Writer) Close() error {
	close(l.lineChan)
	return nil
}

// Flush synchronously sends the current batched entries (if any) to Loki.
func (l *Writer) Flush() {
	l.mustSendBatch()
}

func (l *Writer) start() {
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

func (l *Writer) mustSendBatch() error {
	if len(l.batch) == 0 {
		return nil
	}

	l.maxTime.Reset(l.MaxBatchAge)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := sendBatch(ctx, l.batch, l.labels, l.LokiURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to send batch: %s\n", err)
	}

	l.batch = nil
	return err
}
