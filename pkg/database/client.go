package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/spaolacci/murmur3"
)

const (
	pingTimeout = 5 * time.Second
)

// Scan - stored scan result
type Scan interface {
	IP() string
	Port() uint32
	Service() string
	Timestamp() int64
	Data() string
}

// ScanData - database table data representation
type ScanData struct {
	IP        string
	Port      uint32
	Service   string
	Timestamp int64
	Data      string
	Hash      uint64
}

// Client - DB client wrapper
type Client struct {
	db  *sql.DB
	log Logger
}

// New - Client constructor
func New(db *sql.DB, log Logger) (*Client, error) {
	if db == nil {
		return nil, fmt.Errorf("no database handle provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	err := db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &Client{db, &NullSafeLogger{log}}, nil
}

// Put - insert or update scan results
func (c *Client) Put(ctx context.Context, scan Scan) (int64, error) {
	// start transaction
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return 0, err
	}

	var (
		// get hashed record ID
		hash   = Hash(scan)
		exists bool
	)
	row := tx.QueryRow(getExistsQuery(), hash)

	// see if the service scan data already exists
	if err := row.Scan(&exists); err != nil {
		// bad error - fall
		_ = tx.Rollback()
		return 0, err
	}

	if !exists {
		// oh, it's the first time we got this service data - INSERT!
		_, err := tx.ExecContext(ctx, getInsertQuery(), hash, scan.Service(),
			scan.IP(), scan.Port(), scan.Timestamp(), scan.Data())

		if err == nil {
			return 1, tx.Commit()
		}
		_ = tx.Rollback()
		return 0, err
	}

	// the service record exists - do conditional update
	res, err := tx.ExecContext(ctx, getUpdateQuery(), scan.Timestamp(), scan.Data(), hash, scan.Timestamp())

	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}

	// get affected rows - to propagate whether actual update took place or not
	rowsAffected, _ := res.RowsAffected()
	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

// GetAll - get all table data (purely testing purpose)
func (c *Client) GetAll(ctx context.Context) (map[uint64]*ScanData, error) {
	rows, err := c.db.QueryContext(ctx, getSelectQuery())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	res := map[uint64]*ScanData{}
	for rows.Next() {
		row := &ScanData{}
		if err := rows.Scan(&row.Hash, &row.Service, &row.IP, &row.Port, &row.Timestamp, &row.Data); err != nil {
			return nil, err
		}

	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func getInsertQuery() string {
	return `INSERT INTO scan_results (hash, service, ip, port, timestamp, data) VALUES (?,?,?,?,?,?);`
}

func getSelectQuery() string {
	return `SELECT hash, service, ip, port, timestamp, data FROM scan_results;`
}

func getExistsQuery() string {
	return `SELECT EXISTS(SELECT 1 FROM scan_results WHERE hash = ?);`
}

func getUpdateQuery() string {
	return `UPDATE 
				scan_results 
			SET 
				timestamp = ?, data = ? 
			WHERE
			  	hash = ? AND timestamp < ?;`
}

// Hash - returns murmur3 hash of the provided Scan result
func Hash(scan Scan) uint64 {
	return murmur3.Sum64([]byte(fmt.Sprintf("%s-%s-%d", scan.Service(), scan.IP(), scan.Port())))
}
