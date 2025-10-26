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
	Version() uint8
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
func (c *Client) Put(ctx context.Context, scan Scan) error {
	_, err := c.db.Exec(getQuery(), hash(scan), scan.Service(),
		scan.IP(), scan.Port(), scan.Timestamp(), scan.Version(), scan.Data())
	return err
}

func getQuery() string {
	return `INSERT INTO scan_results (hash, service, ip, port, timestamp, ver, data)
     		  VALUES (?,?,?,?,?,?,?) 
			  ON DUPLICATE KEY UPDATE 
			  	timestamp = IF(timestamp < VALUES(timestamp), VALUES(timestamp), timestamp),
				data = IF(timestamp < VALUES(timestamp), VALUES(data), data);`
}

func hash(scan Scan) uint64 {
	return murmur3.Sum64([]byte(fmt.Sprintf("%s-%s-%d", scan.Service(), scan.IP(), scan.Port())))
}
