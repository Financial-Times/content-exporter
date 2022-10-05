package db

import (
	"fmt"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/go-logger/v2"
)

type MongoInquirer struct {
	mongo Service
	log   *logger.UPPLogger
}

func NewMongoInquirer(mongo Service, log *logger.UPPLogger) *MongoInquirer {
	return &MongoInquirer{
		mongo: mongo,
		log:   log,
	}
}

func (m *MongoInquirer) Inquire(collection string, candidates []string) (chan *content.Stub, int, error) {
	tx, err := m.mongo.Open()
	if err != nil {
		return nil, 0, err
	}

	iter, length, err := tx.FindUUIDs(collection, candidates, m.log)
	if err != nil {
		tx.Close()
		return nil, 0, err
	}

	docs := make(chan *content.Stub, 8)

	go func() {
		defer close(docs)
		defer tx.Close()
		defer iter.Close()

		var result map[string]interface{}
		counter := 0
		for iter.Next(&result) {
			counter++
			stub, err := mapStub(result)
			if err != nil {
				m.log.WithError(err).Warn("Failed to map document")
				continue
			}
			docs <- stub
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
