package processing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/igorvan/scan-takehome/pkg/database"
	"github.com/igorvan/scan-takehome/pkg/scanning"
)

type storageMock struct {
	mtx     sync.RWMutex
	data    map[uint64]database.Scan
	nextErr error
}

func (sm *storageMock) Put(_ context.Context, scan database.Scan) (int64, error) {
	if sm.nextErr != nil {
		return 0, sm.nextErr
	}
	sm.mtx.Lock()
	defer sm.mtx.Unlock()
	if existingScan, ok := sm.data[database.Hash(scan)]; ok {
		if existingScan.Timestamp() > scan.Timestamp() {
			return 0, nil
		}
	}
	sm.data[database.Hash(scan)] = scan
	return 1, nil
}

type ReceiverSuite struct {
	suite.Suite
}

func TestReceiverSuite(t *testing.T) {
	suite.Run(t, &ReceiverSuite{})
}

func (s *ReceiverSuite) TestPut() {
	scanR := NewScanResult("10.10.10.10", 99, "AWESOME", time.Now().Unix(),
		scanning.V2Data{ResponseStr: "something initial"}, scanning.V2)
	testCases := []struct {
		title                string
		expectedAffectedRows int64
		expectedErr          error
		dbErr                error
		input                *ScanResult
		dbData               map[uint64]database.Scan
	}{
		{
			title:                "Success - new row added",
			dbData:               map[uint64]database.Scan{},
			expectedAffectedRows: 1,
			input:                scanR,
		},
		{
			title:                "Failure - db error",
			dbData:               map[uint64]database.Scan{},
			expectedAffectedRows: 0,
			input:                scanR,
			dbErr:                fmt.Errorf("database internal error"),
			expectedErr:          fmt.Errorf("database internal error"),
		},
		{
			title: "Success - no rows updated",
			dbData: map[uint64]database.Scan{
				database.Hash(scanR): scanR,
			},
			expectedAffectedRows: 0,
			input: NewScanResult("10.10.10.10", 99, "AWESOME", time.Now().Add(-10*time.Second).Unix(),
				scanning.V2Data{ResponseStr: "something else"}, scanning.V2),
		},
		{
			title: "Success - one row updated",
			dbData: map[uint64]database.Scan{
				database.Hash(scanR): scanR,
			},
			expectedAffectedRows: 1,
			input: NewScanResult("10.10.10.10", 99, "AWESOME", time.Now().Add(10*time.Second).Unix(),
				scanning.V2Data{ResponseStr: "something else"}, scanning.V2),
		},
	}

	for _, tc := range testCases {
		s.Run(tc.title, func() {
			mock := &storageMock{
				data:    tc.dbData,
				nextErr: tc.dbErr,
			}

			receiver, err := New(mock)
			s.NoError(err)
			s.NotNil(receiver)

			n, err := receiver.Process(context.TODO(), tc.input)
			s.Equal(tc.expectedErr, err)
			s.Equal(tc.expectedAffectedRows, n)
			dbRow, ok := mock.data[database.Hash(tc.input)]
			if tc.expectedAffectedRows > 0 {
				// should be updated
				s.True(ok)
				s.Equal(dbRow.Data(), tc.input.Data())
				s.Equal(dbRow.Timestamp(), tc.input.Timestamp())
			} else if ok {
				// should stay the same
				s.NotEqual(dbRow.Data(), tc.input.Data())
				s.NotEqual(dbRow.Timestamp(), tc.input.Timestamp())
			}
		})
	}
}

func (s *ReceiverSuite) TestData() {
	testCases := []struct {
		title          string
		input          *ScanResult
		expectedResult string
	}{
		{
			title: "V1 GOOD data",
			input: NewScanResult("10.10.10.10", 99, "AWESOME", time.Now().Unix(),
				scanning.V1Data{ResponseBytesUtf8: []byte("something super nice")}, scanning.V1),
			expectedResult: "something super nice",
		},
		{
			title: "V2 GOOD data",
			input: NewScanResult("10.10.10.10", 99, "AWESOME", time.Now().Unix(),
				scanning.V2Data{ResponseStr: "another version of something super nice"}, scanning.V2),
			expectedResult: "another version of something super nice",
		},
		{
			title: "V1 CORRUPT data",
			input: NewScanResult("10.10.10.10", 99, "AWESOME", time.Now().Unix(),
				"", scanning.V1),
			expectedResult: unknown,
		},
		{
			title: "V2 CORRUPT data",
			input: NewScanResult("10.10.10.10", 99, "AWESOME", time.Now().Unix(),
				struct{ boolField bool }{}, scanning.V2),
			expectedResult: unknown,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.title, func() {
			s.Equal(tc.expectedResult, tc.input.Data())
		})
	}
}
