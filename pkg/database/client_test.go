package database

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/suite"
)

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
