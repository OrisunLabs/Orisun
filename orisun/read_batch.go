package orisun

import (
	"time"
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
// allocate an object graph for every row. Backends extract stored envelope
// fields before returning this representation; Data contains application data.
type ReadEvent struct {
	WriteId         string
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

// Event materializes the in-process event representation.
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
	event.WriteId = read.WriteId
	event.EventId = read.EventId
	event.EventType = read.EventType
	event.Data = read.Data
	event.Metadata = read.Metadata
	event.Position = &Position{
		CommitPosition:  read.CommitPosition,
		PreparePosition: read.PreparePosition,
	}
	event.DateCreated = read.DateCreated
}

// Response materializes the in-process response shape.
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
