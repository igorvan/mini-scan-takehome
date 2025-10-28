package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lmittmann/tint"

	"github.com/igorvan/scan-takehome/pkg/database"
	"github.com/igorvan/scan-takehome/pkg/processing"
	"github.com/igorvan/scan-takehome/pkg/scanning"
)

func main() {
	projectID := flag.String("project", "test-project", "GCP Project ID")
	topicID := flag.String("topic", "scan-topic", "GCP PubSub Topic ID")
	subID := flag.String("subscription", "scan-sub", "GCP PubSub Subscription Name")

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}

	topic := client.Topic(*topicID)

	db, err := sql.Open("mysql", "processor:password@tcp(db:3306)/processor")
	if err != nil {
		panic(err)
	}

	logger := slog.New(tint.NewHandler(os.Stdout, nil))
	storage, err := database.New(db, logger)
	if err != nil {
		panic(err)
	}

	prcssr, err := processing.New(storage)
	if err != nil {
		panic(err)
	}

	sub, err := client.CreateSubscription(context.Background(), *subID,
		pubsub.SubscriptionConfig{Topic: topic})

	if err != nil {
		panic(err)
	}

	err = sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
		logger.Info(fmt.Sprintf("Got message: %s", m.Data))
		scanData := &scanning.Scan{}
		err := json.Unmarshal(m.Data, scanData)
		if err != nil {
			logger.Error(fmt.Sprintf("cannot parse received scan results [%s]: %s", string(m.Data), err))
			m.Ack()
			return
		}
		processingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		n, err := prcssr.Process(processingCtx, processing.NewScanResult(
			scanData.Ip,
			scanData.Port,
			scanData.Service,
			scanData.Timestamp,
			scanData.Data,
			uint8(scanData.DataVersion),
		))
		if err != nil {
			logger.Error(fmt.Sprintf("data processing error: %s, [Service: %s, IP: %s, Port: %d, Timestamp: %s, Data: %s]",
				err, scanData.Service, scanData.Ip, scanData.Port, time.Unix(scanData.Timestamp, 0).Format(time.RFC3339), string(m.Data)))
			// database write error - return without acking, let some other pod to retry
			return
		}
		logger.Info(fmt.Sprintf("[Service: %s, IP: %s, Port: %d, Timestamp: %s, Data: %s] PUT operation success - undated %d rows",
			scanData.Service, scanData.Ip, scanData.Port, time.Unix(scanData.Timestamp, 0).Format(time.RFC3339), string(m.Data), n))
		m.Ack()
	})

	if err != nil {
		panic(err)
	}
}
