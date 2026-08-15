package orisun

import (
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
)

const (
	// DefaultReadBatchSize is used by storage backends when an in-process
	// caller does not provide a page size.
	DefaultReadBatchSize uint32 = 1_000
	// MaxReadBatchSize bounds one database read and its result allocation.
	// Larger logical reads must advance by position and fetch another page.
	MaxReadBatchSize uint32 = 10_000
)

// ReadEvent is the backend-neutral, contiguous read representation. Positions
// and timestamps are scalar values so storage and internal consumers do not
// allocate an object graph for every row.
type ReadEvent struct {
	EventId         string
	EventType       string
	Data            string
	Metadata        string
	CommitPosition  int64
	PreparePosition int64
	DateCreated     time.Time
}

// ReadEventBatch keeps database results in one value slab.
type ReadEventBatch []ReadEvent

func publicReadEvent(event ReadEvent) ReadEvent {
	event.Data = eventdata.WithoutStorageEventType(event.Data)
	return event
}

func publicReadEventBatch(batch ReadEventBatch) ReadEventBatch {
	publicBatch := make(ReadEventBatch, len(batch))
	for index, event := range batch {
		publicBatch[index] = publicReadEvent(event)
	}
	return publicBatch
}

func publicLatestByCriteriaBatch(batch LatestByCriteriaBatch) LatestByCriteriaBatch {
	publicBatch := batch
	publicBatch.Matches = make([]LatestCriterionMatch, len(batch.Matches))
	copy(publicBatch.Matches, batch.Matches)
	for index := range publicBatch.Matches {
		if publicBatch.Matches[index].Found {
			publicBatch.Matches[index].Event = publicReadEvent(publicBatch.Matches[index].Event)
		}
	}
	return publicBatch
}

// Event materializes the legacy in-process event representation.
func (e *ReadEvent) Event() *Event {
	if e == nil {
		return nil
	}
	event := &Event{}
	fillEvent(event, e)
	return event
}

func fillEvent(event *Event, read *ReadEvent) {
	if event == nil || read == nil {
		return
	}
	event.EventId = read.EventId
	event.EventType = read.EventType
	event.Data = eventdata.WithoutStorageEventType(read.Data)
	event.Metadata = read.Metadata
	event.Position = &Position{
		CommitPosition:  read.CommitPosition,
		PreparePosition: read.PreparePosition,
	}
	event.DateCreated = read.DateCreated
}

// Response materializes the legacy in-process response shape.
func (b ReadEventBatch) Response() *GetEventsResponse {
	rows := make([]Event, len(b))
	pointers := make([]*Event, len(b))
	for i := range b {
		read := &b[i]
		row := &rows[i]
		fillEvent(row, read)
		pointers[i] = row
	}
	return &GetEventsResponse{Events: pointers}
}

// MarshalJSON preserves the stable NATS event envelope while keeping the
// publisher on the packed representation.
func (e ReadEvent) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, 192+len(e.EventId)+len(e.EventType)+len(e.Data)+len(e.Metadata))
	buf = append(buf, '{')
	comma := false
	buf, comma = appendJSONField(buf, comma, `"event_id":`, e.EventId)
	buf, comma = appendJSONField(buf, comma, `"event_type":`, e.EventType)
	buf, comma = appendJSONField(buf, comma, `"data":`, e.Data)
	buf, comma = appendJSONField(buf, comma, `"metadata":`, e.Metadata)
	if comma {
		buf = append(buf, ',')
	}
	buf = append(buf, `"position":{`...)
	if e.CommitPosition != 0 {
		buf = append(buf, `"commit_position":`...)
		buf = strconv.AppendInt(buf, e.CommitPosition, 10)
		if e.PreparePosition != 0 {
			buf = append(buf, ',')
		}
	}
	if e.PreparePosition != 0 {
		buf = append(buf, `"prepare_position":`...)
		buf = strconv.AppendInt(buf, e.PreparePosition, 10)
	}
	buf = append(buf, `},"date_created":{`...)
	seconds := e.DateCreated.Unix()
	nanos := int32(e.DateCreated.Nanosecond())
	if seconds != 0 {
		buf = append(buf, `"seconds":`...)
		buf = strconv.AppendInt(buf, seconds, 10)
		if nanos != 0 {
			buf = append(buf, ',')
		}
	}
	if nanos != 0 {
		buf = append(buf, `"nanos":`...)
		buf = strconv.AppendInt(buf, int64(nanos), 10)
	}
	buf = append(buf, '}', '}')
	return buf, nil
}

func appendJSONField(buf []byte, comma bool, name, value string) ([]byte, bool) {
	if value == "" {
		return buf, comma
	}
	if comma {
		buf = append(buf, ',')
	}
	buf = append(buf, name...)
	buf = appendJSONString(buf, value)
	return buf, true
}

// appendJSONString mirrors the escaping used by goccy/go-json without
// allocating a temporary encoded string for every event field.
func appendJSONString(buf []byte, value string) []byte {
	const hex = "0123456789abcdef"
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(value); {
		c := value[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '\\' && c != '"' && c != '<' && c != '>' && c != '&' {
				i++
				continue
			}
			buf = append(buf, value[start:i]...)
			switch c {
			case '\\', '"':
				buf = append(buf, '\\', c)
			case '\n':
				buf = append(buf, `\n`...)
			case '\r':
				buf = append(buf, `\r`...)
			case '\t':
				buf = append(buf, `\t`...)
			default:
				buf = append(buf, `\u00`...)
				buf = append(buf, hex[c>>4], hex[c&0x0f])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, value[start:i]...)
			buf = append(buf, `\ufffd`...)
			i++
			start = i
			continue
		}
		if r == '\u2028' || r == '\u2029' {
			buf = append(buf, value[start:i]...)
			buf = append(buf, `\u202`...)
			buf = append(buf, byte('8'+r-'\u2028'))
			i += size
			start = i
			continue
		}
		i += size
	}
	buf = append(buf, value[start:]...)
	return append(buf, '"')
}
