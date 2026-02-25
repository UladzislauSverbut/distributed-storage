package events

import (
	"bytes"
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
	serializedVersion := make([]byte, 8)

	binary.LittleEndian.PutUint64(serializedVersion, event.Version)

	serializedEvent = append(serializedEvent, serializedVersion...)

	return serializedEvent
}

func ParseUpdateDBVersion(data []byte) (*UpdateDBVersion, error) {
	offset := len(UPDATE_DB_VERSION)

	if !bytes.Equal(data[0:offset], []byte(UPDATE_DB_VERSION)) {
		return nil, updateDBVersionParsingError
	}

	serializedVersion := data[offset : offset+8]

	return &UpdateDBVersion{
		Version: binary.LittleEndian.Uint64(serializedVersion),
	}, nil
}
