package db

import "distributed-storage/internal/events"

// Event is an alias to events.Event so db can refer to it
// without pulling db-specific concepts into the events package.
type Event = events.Event
