package codec

import (
	"distributed-storage/internal/events"
	"distributed-storage/internal/pager"
	"encoding/binary"
	"fmt"
)

func EncodeEvent(event events.Event) []byte {
	var encodedEvent []byte

	switch event := event.(type) {
	case *events.StartTransaction:
		encodedEvent = encodeStartTransaction(event)
	case *events.CommitTransaction:
		encodedEvent = encodeCommitTransaction(event)
	case *events.CreateTable:
		encodedEvent = encodeCreateTable(event)
	case *events.UpdateTable:
		encodedEvent = encodeUpdateTable(event)
	case *events.DropTable:
		encodedEvent = encodeDropTable(event)
	case *events.InsertEntry:
		encodedEvent = encodeInsertEntry(event)
	case *events.UpdateEntry:
		encodedEvent = encodeUpdateEntry(event)
	case *events.DeleteEntry:
		encodedEvent = encodeDeleteEntry(event)
	case *events.UpdateDBVersion:
		encodedEvent = encodeUpdateDBVersion(event)
	case *events.FreePages:
		encodedEvent = encodeFreePages(event)
	default:
		panic("EncodeEvent: unknown event type")
	}

	header := make([]byte, 2) // 2 bytes for type
	binary.LittleEndian.PutUint16(header, event.Type())

	return append(header, encodedEvent...)
}

func DecodeEvent(data []byte) (event events.Event, err error) {
	if len(data) < 2 {
		err = fmt.Errorf("DecodeEvent: event data too short")
		return
	}

	eventType := binary.LittleEndian.Uint16(data[:2])
	encodedEvent := data[2:]

	switch eventType {
	case events.CREATE_TABLE_EVENT:
		event = decodeCreateTable(encodedEvent)
	case events.UPDATE_TABLE_EVENT:
		event = decodeUpdateTable(encodedEvent)
	case events.DROP_TABLE_EVENT:
		event = decodeDropTable(encodedEvent)
	case events.INSERT_ENTRY_EVENT:
		event = decodeInsertEntry(encodedEvent)
	case events.UPDATE_ENTRY_EVENT:
		event = decodeUpdateEntry(encodedEvent)
	case events.DELETE_ENTRY_EVENT:
		event = decodeDeleteEntry(encodedEvent)
	case events.UPDATE_DB_VERSION_EVENT:
		event = decodeUpdateDBVersion(encodedEvent)
	case events.FREE_PAGES_EVENT:
		event = decodeFreePages(encodedEvent)
	default:
		panic("DecodeEvent: unknown event type")
	}

	return event, nil
}

func encodeStartTransaction(_ *events.StartTransaction) []byte {
	return []byte{}
}

func decodeStartTransaction(_ []byte) *events.StartTransaction {
	return events.NewStartTransaction()
}

func encodeCommitTransaction(_ *events.CommitTransaction) []byte {
	return []byte{}
}

func decodeCommitTransaction(_ []byte) *events.CommitTransaction {
	return events.NewCommitTransaction()
}

func encodeCreateTable(event *events.CreateTable) []byte {
	out := make([]byte, 8)

	binary.LittleEndian.PutUint64(out, event.TableID)

	out = append(out, event.Schema...)

	return out
}

func decodeCreateTable(data []byte) *events.CreateTable {
	tableID := binary.LittleEndian.Uint64(data[:8])
	return events.NewCreateTable(tableID, data[8:])
}

func encodeDropTable(event *events.DropTable) []byte {
	out := make([]byte, 8)

	binary.LittleEndian.PutUint64(out, event.TableID)

	return out
}

func decodeDropTable(data []byte) *events.DropTable {
	tableID := binary.LittleEndian.Uint64(data[:8])
	return events.NewDropTable(tableID)
}

func encodeUpdateTable(event *events.UpdateTable) []byte {
	var out []byte

	serializedTableID := make([]byte, 8)
	oldSchemaLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)
	binary.LittleEndian.PutUint64(oldSchemaLength, uint64(len(event.OldSchema)))

	out = append(out, serializedTableID...)
	out = append(out, oldSchemaLength...)
	out = append(out, event.OldSchema...)

	return append(out, event.NewSchema...)
}

func decodeUpdateTable(data []byte) *events.UpdateTable {
	offset := 0
	tableID := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	oldSchemaLength := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	oldSchema := data[offset : offset+oldSchemaLength]
	offset += oldSchemaLength

	return events.NewUpdateTable(tableID, oldSchema, data[offset:])
}

func encodeInsertEntry(event *events.InsertEntry) []byte {
	var out []byte

	serializedTableID := make([]byte, 8)
	keyLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(event.Key)))

	out = append(out, serializedTableID...)
	out = append(out, keyLength...)
	out = append(out, event.Key...)

	return append(out, event.Value...)
}

func decodeInsertEntry(data []byte) *events.InsertEntry {
	offset := 0
	tableID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	keyLength := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	key := data[offset : offset+keyLength]
	offset += keyLength
	return events.NewInsertEntry(tableID, key, data[offset:])
}

func encodeDeleteEntry(event *events.DeleteEntry) []byte {
	var out []byte

	serializedTableID := make([]byte, 8)
	serializedKeyLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)
	binary.LittleEndian.PutUint64(serializedKeyLength, uint64(len(event.Key)))

	out = append(out, serializedTableID...)
	out = append(out, serializedKeyLength...)
	out = append(out, event.Key...)

	return append(out, event.Value...)
}

func decodeDeleteEntry(data []byte) *events.DeleteEntry {
	offset := 0
	tableID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	keyLength := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	key := data[offset : offset+keyLength]
	offset += keyLength

	return events.NewDeleteEntry(tableID, key, data[offset:])
}

func encodeUpdateEntry(event *events.UpdateEntry) []byte {
	var out []byte

	serializedTableID := make([]byte, 8)
	keyLength := make([]byte, 8)
	oldValueLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(serializedTableID, event.TableID)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(event.Key)))
	binary.LittleEndian.PutUint64(oldValueLength, uint64(len(event.OldValue)))

	out = append(out, serializedTableID...)
	out = append(out, keyLength...)
	out = append(out, event.Key...)
	out = append(out, oldValueLength...)
	out = append(out, event.OldValue...)

	return append(out, event.NewValue...)
}

func decodeUpdateEntry(data []byte) *events.UpdateEntry {
	offset := 0
	tableID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	keyLength := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	key := data[offset : offset+keyLength]
	offset += keyLength

	oldValueLength := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	oldValue := data[offset : offset+oldValueLength]
	offset += oldValueLength
	return events.NewUpdateEntry(tableID, key, oldValue, data[offset:])
}

func encodeUpdateDBVersion(event *events.UpdateDBVersion) []byte {
	out := make([]byte, 8)

	binary.LittleEndian.PutUint64(out, event.Version)

	return out
}

func decodeUpdateDBVersion(data []byte) *events.UpdateDBVersion {
	dbVersion := binary.LittleEndian.Uint64(data[:8])
	return events.NewUpdateDBVersion(dbVersion)
}

func encodeFreePages(event *events.FreePages) []byte {
	var out []byte

	version := make([]byte, 8)
	pages := make([]byte, len(event.List.Pages())*16)

	for idx, interval := range event.List.Pages() {
		binary.LittleEndian.PutUint64(pages[idx*16:], interval.Start)
		binary.LittleEndian.PutUint64(pages[idx*16+8:], interval.End)
	}

	binary.LittleEndian.PutUint64(version, event.Version)
	out = append(out, version...)

	return append(out, pages...)
}

func decodeFreePages(data []byte) *events.FreePages {
	offset := 0
	version := binary.LittleEndian.Uint64(data[offset : offset+8])
	serializedPages := data[offset+8:]

	pages := make([]pager.PageInterval, len(serializedPages)/16)

	for idx := range pages {
		pages[idx] = pager.PageInterval{
			Start: binary.LittleEndian.Uint64(serializedPages[idx*16:]),
			End:   binary.LittleEndian.Uint64(serializedPages[idx*16+8:]),
		}
	}
	return events.NewFreePages(version, pager.NewPageList(pages...))
}
