package content

import (
	"fmt"

	"github.com/Financial-Times/content-exporter/db"
	"github.com/Financial-Times/go-logger/v2"
)

type Inquirer interface {
	Inquire(collection string, candidates []string) (chan Stub, int, error)
}

type MongoInquirer struct {
	Mongo db.Service
	log   *logger.UPPLogger
}

func NewMongoInquirer(mongo db.Service, log *logger.UPPLogger) *MongoInquirer {
	return &MongoInquirer{
		Mongo: mongo,
		log:   log,
	}
}

func (m *MongoInquirer) Inquire(collection string, candidates []string) (chan Stub, int, error) {
	tx, err := m.Mongo.Open()

	if err != nil {
		return nil, 0, err
	}
	iter, length, err := tx.FindUUIDs(collection, candidates, m.log)
	if err != nil {
		tx.Close()
		return nil, 0, err
	}

	docs := make(chan Stub, 8)

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

func mapStub(result map[string]interface{}) (Stub, error) {
	docUUID, ok := result["uuid"]
	if !ok {
		return Stub{}, fmt.Errorf("no uuid field found in iter result: %v", result)
	}

	return Stub{UUID: docUUID.(string), Date: GetDateOrDefault(result), CanBeDistributed: nil}, nil
}
