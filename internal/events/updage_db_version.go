package events

import (
	"distributed-storage/internal/helpers"
	"encoding/binary"
	"errors"
)

const UPDATE_DB_VERSION = "UPDATE_DB_VERSION"

var updateDBVersionParsingError = errors.New("UpdateDBVersion: couldn't parse event")

type UpdateDBVersion struct {
	Version uint64
}

func NewUpdateDBVersion(dbVersion uint64) *UpdateDBVersion {
	return &UpdateDBVersion{Version: dbVersion}
}

func (event *UpdateDBVersion) Name() string {
	return UPDATE_DB_VERSION
}

func (event *UpdateDBVersion) Serialize() []byte {
	serializedEvent := []byte(event.Name())
	version := make([]byte, 8)

	binary.LittleEndian.PutUint64(version, event.Version)

	serializedEvent = append(serializedEvent, ' ')

	return serializedEvent
}

func ParseUpdateDBVersion(data []byte) (*UpdateDBVersion, error) {
	parts := helpers.SplitBy(data, ' ')

	if len(parts) != 2 || string(parts[0]) != UPDATE_DB_VERSION {
		return nil, updateDBVersionParsingError
	}

	return &UpdateDBVersion{
		Version: binary.LittleEndian.Uint64(parts[1]),
	}, nil
}
