package processing

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/igorvan/scan-takehome/pkg/database"
)

const (
	// Version - supported scanning data versions
	Version = iota
	// V1 - older - base64 encoded string
	V1
	// V2 - newer - decoded string
	V2
	unknown = "unknown"
)

// Storage - scanning results storage
type Storage interface {
	Put(ctx context.Context, scan database.Scan) (int64, error)
}

// ScanResult - domain scan result,
// for now the only difference from the client's counterpart is immutability
type ScanResult struct {
	ip          string
	port        uint32
	service     string
	timestamp   int64
	rawData     any
	version     uint8
	decodedData string
}

// NewScanResult - ScanResult constructor
func NewScanResult(
	ip string,
	port uint32,
	service string,
	timestamp int64,
	rawData any,
	version uint8,
) *ScanResult {
	return &ScanResult{
		ip:        ip,
		port:      port,
		version:   version,
		rawData:   rawData,
		timestamp: timestamp,
		service:   service,
	}
}

// IP - scanned service IP address
func (s *ScanResult) IP() string {
	return s.ip
}

// Data - scanned service response
func (s *ScanResult) Data() string {
	if s.decodedData != "" {
		return s.decodedData
	}
	b, err := json.Marshal(s.rawData)
	if err != nil {
		fmt.Println(fmt.Sprintf("cannot marshal [%s] - error:", s.rawData), err)
		s.decodedData = unknown
		return s.decodedData
	}
	switch s.version {
	case V1:
		v1Data := unmarshal[V1Data](b)
		if v1Data == nil || len(v1Data.ResponseBytesUtf8) == 0 {
			s.decodedData = unknown
			break
		}
		s.decodedData = string(v1Data.ResponseBytesUtf8)
	case V2:
		v2Data := unmarshal[V2Data](b)
		if v2Data == nil || v2Data.ResponseStr == "" {
			s.decodedData = unknown
			break
		}
		s.decodedData = v2Data.ResponseStr
	default:
		return unknown
	}
	return s.decodedData
}

// Service - scanned service name
func (s *ScanResult) Service() string {
	return s.service
}

// Timestamp - the timestamp of the scanning
func (s *ScanResult) Timestamp() int64 {
	return s.timestamp
}

// Version - scanned service result format
func (s *ScanResult) Version() uint8 {
	return s.version
}

// Port - scanned service port
func (s *ScanResult) Port() uint32 {
	return s.port
}

// V1Data - bytes
type V1Data struct {
	ResponseBytesUtf8 []byte `json:"response_bytes_utf8"`
}

// V2Data - string
type V2Data struct {
	ResponseStr string `json:"response_str"`
}

func unmarshal[T any](b []byte) *T {
	var res T
	err := json.Unmarshal(b, &res)
	if err != nil {
		fmt.Println(fmt.Sprintf("error unmarshaling scan result data [%s]:", b), err)
		return nil
	}
	return &res
}
