package db

import "distributed-storage/internal/events"

// TableEvent is an alias to events. Event to avoid duplication and keep db-specific naming.
type TableEvent = events.Event
