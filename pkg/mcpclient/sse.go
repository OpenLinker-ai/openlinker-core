package mcpclient

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	errEventTooLarge  = errors.New("event exceeds the size limit")
	errStreamTooLarge = errors.New("event stream exceeds the size limit")
	errNotJSONRPC     = errors.New("event is not JSON-RPC")
)

type sseEvent struct {
	Event string
	Data  []byte
}

// sseReader parses a text/event-stream body. It enforces a per-event limit on
// the accumulated data and on any single line, and a limit on all bytes read.
type sseReader struct {
	r        *bufio.Reader
	maxEvent int64
}

func newSSEReader(body io.Reader, maxEvent, maxStream int64) *sseReader {
	return &sseReader{
		r:        bufio.NewReaderSize(&cappedReader{r: body, remaining: maxStream}, 32<<10),
		maxEvent: maxEvent,
	}
}

// next returns the next event with data. Comments, id and retry fields, and
// events without data are skipped as the SSE specification requires.
func (s *sseReader) next() (sseEvent, error) {
	var event sseEvent
	var data bytes.Buffer
	hasData := false
	for {
		line, err := s.readLine()
		if err != nil {
			if errors.Is(err, io.EOF) && hasData {
				// A stream that ends without the blank terminator does not
				// dispatch the pending event.
				return sseEvent{}, io.ErrUnexpectedEOF
			}
			return sseEvent{}, err
		}
		if len(line) == 0 {
			if hasData {
				event.Data = data.Bytes()
				if event.Event == "" {
					event.Event = "message"
				}
				return event, nil
			}
			event = sseEvent{}
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value := line, []byte(nil)
		if i := bytes.IndexByte(line, ':'); i >= 0 {
			field, value = line[:i], line[i+1:]
			if len(value) > 0 && value[0] == ' ' {
				value = value[1:]
			}
		}
		switch string(field) {
		case "event":
			event.Event = string(value)
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.Write(value)
			hasData = true
			if int64(data.Len()) > s.maxEvent {
				return sseEvent{}, errEventTooLarge
			}
		}
	}
}

// readLine returns one line without its terminator.
func (s *sseReader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := s.r.ReadSlice('\n')
		if int64(len(line)+len(chunk)) > s.maxEvent+2 {
			return nil, errEventTooLarge
		}
		line = append(line, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if len(line) > 0 && errors.Is(err, io.EOF) {
				return bytes.TrimRight(line, "\r\n"), nil
			}
			return nil, err
		}
		return bytes.TrimRight(line, "\r\n"), nil
	}
}

// cappedReader fails once more than remaining bytes are read, so a stream
// that keeps sending is cut off instead of read forever.
type cappedReader struct {
	r         io.Reader
	remaining int64
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		// Distinguish a stream that ends exactly at the limit from one that
		// goes on: probe one byte.
		var probe [1]byte
		n, err := c.r.Read(probe[:])
		if n > 0 {
			return 0, errStreamTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.r.Read(p)
	c.remaining -= int64(n)
	return n, err
}

func protocolErrorFromStream(stage string, err error) error {
	switch {
	case errors.Is(err, errEventTooLarge), errors.Is(err, errStreamTooLarge), errors.Is(err, errNotJSONRPC):
		return &ProtocolError{Stage: stage, Message: err.Error()}
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return &ProtocolError{Stage: stage, Message: "event stream ended before the response"}
	default:
		return fmt.Errorf("mcp %s: read event stream: %w", stage, err)
	}
}
