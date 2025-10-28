package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lmittmann/tint"

	"github.com/igorvan/scan-takehome/pkg/database"
)

func main() {
	db, err := sql.Open("mysql", "processor:password@tcp(db:3306)/processor")
	if err != nil {
		panic(err)
	}

	logger := slog.New(tint.NewHandler(os.Stdout, nil))
	storage, err := database.New(db, logger)
	if err != nil {
		panic(err)
	}

	var previousSet map[uint64]*database.ScanData
	// we will check the DB state every seconds to validate if there were any bad transitions
	// e.g., if fresher result was overridden by a previous one
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		logger.Info("Scan data validation iteration has started")
		n, res := doValidate(storage, logger, previousSet)
		if res != nil {
			previousSet = res
		}
		msg := fmt.Sprintf("Scan data validation iteration has completed: %d incorrect transitions found", n)
		if n == 0 {
			logger.Info(msg)
		} else {
			logger.Error(msg)
		}
	}

}

// do validate - fetches new table data, compares with a previous set and returns errors count and a new set
func doValidate(storage *database.Client, logger *slog.Logger, previousSet map[uint64]*database.ScanData) (int, map[uint64]*database.ScanData) {
	res, err := storage.GetAll(context.Background())
	if err != nil {
		logger.Error(fmt.Sprintf("cannot get scan result from the DB: %s", err))
		return 0, nil
	}
	if previousSet == nil {
		return 0, nil
	}
	var count = 0
	for key, row := range previousSet {
		newRow, ok := res[key]
		if !ok {
			logger.Error(fmt.Sprintf("bad news - somehow the service scan data was removed from db for key %d", key))
			continue
		}
		if newRow.Timestamp < row.Timestamp {
			logger.Error(fmt.Sprintf("bad news - fresher result has been overridden for [Service: %s, IP: %s, Port: %d]",
				newRow.Service, newRow.IP, newRow.Port))
			count++
		}
	}
	return count, res
}
