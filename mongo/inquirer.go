package mongo

import (
	"context"
	"fmt"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/go-logger/v2"
	"go.mongodb.org/mongo-driver/bson"
)

type contentFinder interface {
	findContent(ctx context.Context, candidates []string) (cursor, int, error)
}

type cursor interface {
	Next(ctx context.Context) bool
	Decode(val interface{}) error
	Err() error

	Close(ctx context.Context) error
}

type Inquirer struct {
	finder contentFinder
	log    *logger.UPPLogger
}

func NewInquirer(finder contentFinder, log *logger.UPPLogger) *Inquirer {
	return &Inquirer{
		finder: finder,
		log:    log,
	}
}

func (m *Inquirer) Inquire(ctx context.Context, candidates []string) (chan *content.Stub, int, error) {
	cur, length, err := m.finder.findContent(ctx, candidates)
	if err != nil {
		return nil, 0, err
	}

	docs := make(chan *content.Stub, 8)

	go func() {
		defer func() {
			close(docs)
			_ = cur.Close(context.Background())
		}()

		counter := 0

		for cur.Next(ctx) {
			var result bson.M
			if err = cur.Decode(&result); err != nil {
				m.log.WithError(err).Warn("Failed to decode document")
				continue
			}

			counter++

			var stub *content.Stub
			stub, err = mapStub(result)
			if err != nil {
				m.log.WithError(err).Warn("Failed to map document")
				continue
			}
			docs <- stub
		}
		if err = cur.Err(); err != nil {
			m.log.WithError(err).Error("Error occurred while iterating over collection")
		}

		m.log.Infof("Processed %v docs", counter)
	}()

	return docs, length, nil
}

func mapStub(result map[string]interface{}) (*content.Stub, error) {
	docUUID, ok := result["uuid"]
	if !ok {
		return nil, fmt.Errorf("uuid not found in document: %v", result)
	}

	return &content.Stub{
		UUID:             docUUID.(string),
		Date:             content.GetDateOrDefault(result),
		CanBeDistributed: nil,
	}, nil
}
