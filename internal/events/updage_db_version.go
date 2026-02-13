package events

import (
	"strconv"
)

const UPDATE_DB_VERSION = "UPDATE_DB_VERSION"

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
	return []byte(event.Name() + "(DB_V=" + strconv.FormatUint(event.Version, 10) + ")\n")
}

func (event *UpdateDBVersion) Parse(data []byte) error {
	// Will be implemented in the future when we need to parse events from WAL.
	return nil
}
