package events

type UpdateDBVersion struct {
	Version uint64
}

func NewUpdateDBVersion(dbVersion uint64) *UpdateDBVersion {
	return &UpdateDBVersion{Version: dbVersion}
}

func (event *UpdateDBVersion) Type() EventType {
	return UPDATE_DB_VERSION_EVENT
}
