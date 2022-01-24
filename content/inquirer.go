package content

import (
	"fmt"

	"github.com/Financial-Times/content-exporter/db"
	log "github.com/sirupsen/logrus"
)

type Inquirer interface {
	Inquire(collection string, candidates []string) (chan Stub, int, error)
}

type MongoInquirer struct {
	Mongo db.Service
}

func NewMongoInquirer(mongo db.Service) *MongoInquirer {
	return &MongoInquirer{Mongo: mongo}
}

func (m *MongoInquirer) Inquire(collection string, candidates []string) (chan Stub, int, error) {
	tx, err := m.Mongo.Open()

	if err != nil {
		return nil, 0, err
	}
	iter, length, err := tx.FindUUIDs(collection, candidates)
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
				log.Warn(err)
				continue
			}
			docs <- stub
		}
		log.Infof("Processed %v docs", counter)
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
