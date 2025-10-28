package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/suite"
)

type CustomUint64Converter struct{}

func (c *CustomUint64Converter) ConvertValue(v interface{}) (driver.Value, error) {
	if val, ok := v.(uint64); ok {
		// Example: Convert uint64 to string for the driver
		return fmt.Sprintf("%d", val), nil
	}
	// Fallback to default conversion for other types
	return driver.DefaultParameterConverter.ConvertValue(v)
}

// ScanData - database table data representation
type testData struct {
	ip        string
	port      uint32
	service   string
	timestamp int64
	data      string
}

// Data - scanned service response
func (s *testData) Data() string {
	return s.data
}

// Service - scanned service name
func (s *testData) Service() string {
	return s.service
}

// IP - scanned service IP address
func (s *testData) IP() string {
	return s.ip
}

// Timestamp - the timestamp of the scanning
func (s *testData) Timestamp() int64 {
	return s.timestamp
}

// Port - scanned service port
func (s *testData) Port() uint32 {
	return s.port
}

type ClientSuite struct {
	suite.Suite
	mock sqlmock.Sqlmock
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, &ClientSuite{})
}

func (s *ClientSuite) TestNew() {
	mockDB, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	testCases := []struct {
		title         string
		db            *sql.DB
		expectedErr   error
		setupCallback func()
	}{
		{
			title:       "Error - bad underlying driver",
			db:          nil,
			expectedErr: fmt.Errorf("no database handle provided"),
		},
		{
			title: "Error - ping failed",
			db:    mockDB,
			setupCallback: func() {
				_ = mock.ExpectPing().WillReturnError(fmt.Errorf("bad ping"))
			},
			expectedErr: fmt.Errorf("bad ping"),
		},
		{
			title: "Success",
			db:    mockDB,
			setupCallback: func() {
				_ = mock.ExpectPing()
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.title, func() {
			if tc.setupCallback != nil {
				tc.setupCallback()
			}
			res, err := New(tc.db, nil)
			if tc.expectedErr == nil {
				s.NotNil(res)
			} else {
				s.Nil(res)
			}
			s.Equal(tc.expectedErr, err)
		})
	}
}

func (s *ClientSuite) TestPut() {
	mockDB, mock, err := sqlmock.New(sqlmock.ValueConverterOption(&CustomUint64Converter{}))
	s.NoError(err)
	input := &testData{
		data:      "test data",
		service:   "test service",
		ip:        "10.10.10.11",
		port:      1555,
		timestamp: time.Now().Unix(),
	}

	dbCli, err := New(mockDB, nil)
	s.NoError(err)
	s.NotNil(dbCli)

	// UPDATE
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM scan_results WHERE hash = \?\);`).
		WithArgs(Hash(input)).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))

	mock.ExpectExec("UPDATE *").WithArgs(input.timestamp, input.data, Hash(input), input.timestamp).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	n, err := dbCli.Put(context.TODO(), input)
	s.NoError(err)
	s.Equal(int64(1), n)

	// INSERT
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM scan_results WHERE hash = \?\);`).
		WithArgs(Hash(input)).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(0))

	mock.ExpectExec("INSERT INTO scan_results *").WithArgs(Hash(input),
		input.service, input.ip, input.port, input.timestamp, input.data).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	n, err = dbCli.Put(context.TODO(), input)
	s.NoError(err)
	s.Equal(int64(1), n)

	// ScanError
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM scan_results WHERE hash = \?\);`).
		WithArgs(Hash(input)).
		WillReturnError(fmt.Errorf("database is down"))

	mock.ExpectRollback()

	n, err = dbCli.Put(context.TODO(), input)
	s.Error(err)
	s.Zero(n)
}
