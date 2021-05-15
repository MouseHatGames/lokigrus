package lokigrus

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/MouseHatGames/lokigrus/internal/logproto"
	"github.com/golang/snappy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func sendBatch(ctx context.Context, entries []*entry, labels string, lokiURL string) error {
	adapterEntries := make([]*logproto.EntryAdapter, len(entries))

	for i, e := range entries {
		adapterEntries[i] = &logproto.EntryAdapter{
			Timestamp: &timestamppb.Timestamp{Seconds: e.time},
			Line:      e.str,
		}
	}

	req := logproto.PushRequest{
		Streams: []*logproto.StreamAdapter{
			{
				Labels:  labels,
				Entries: adapterEntries,
			},
		},
	}

	b, err := proto.Marshal(&req)
	if err != nil {
		return fmt.Errorf("format push request: %s", err)
	}

	b = snappy.Encode(nil, b)

	_, err = send(ctx, lokiURL, b)
	if err != nil {
		return fmt.Errorf("send push request: %s", err)
	}

	return nil
}

func send(ctx context.Context, lokiURL string, buf []byte) (int, error) {
	req, err := http.NewRequest("POST", lokiURL, bytes.NewReader(buf))
	if err != nil {
		return -1, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s (%d): %s", resp.Status, resp.StatusCode, line)
	}
	return resp.StatusCode, err
}
