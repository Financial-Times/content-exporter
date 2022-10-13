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

func (i *Inquirer) Inquire(ctx context.Context, candidates []string) (chan *content.Stub, int, error) {
	cur, length, err := i.finder.findContent(ctx, candidates)
	if err != nil {
		return nil, 0, err
	}

	docs := make(chan *content.Stub, 8)
	go i.processDocuments(ctx, cur, docs)

	return docs, length, nil
}

func (i *Inquirer) processDocuments(ctx context.Context, c cursor, docs chan *content.Stub) {
	defer func() {
		close(docs)
		_ = c.Close(context.Background())
	}()

	counter := 0

	for c.Next(ctx) {
		counter++

		var doc bson.M
		if err := c.Decode(&doc); err != nil {
			i.log.WithError(err).Warn("Failed to decode document")
			continue
		}

		stub, err := mapStub(doc)
		if err != nil {
			i.log.WithError(err).Warn("Failed to map document")
			continue
		}
		docs <- stub
	}
	if err := c.Err(); err != nil {
		i.log.WithError(err).Error("Error occurred while iterating over collection")
	}

	i.log.Infof("Processed %v docs", counter)
}

func mapStub(doc map[string]interface{}) (*content.Stub, error) {
	docUUID, ok := doc["uuid"]
	if !ok {
		return nil, fmt.Errorf("uuid not found in document: %v", doc)
	}

	return &content.Stub{
		UUID:             docUUID.(string),
		Date:             content.GetDateOrDefault(doc),
		CanBeDistributed: nil,
	}, nil
}
