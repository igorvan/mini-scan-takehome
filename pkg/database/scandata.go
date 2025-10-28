package database

// ScanData - database table data representation
type ScanData struct {
	IP        string `sql:"ip"`
	Port      uint32 `sql:"port"`
	Service   string `sql:"service"`
	Timestamp int64  `sql:"timestamp"`
	Data      string `sql:"data"`
	Hash      uint64 `sql:"hash"`
}
