package db

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var expectedConnections = 1
var connections = 0

// Service contains database functions
type Service interface {
	Open() (TX, error)
	Close()
}

// TX contains database transaction functions
type TX interface {
	FindUUIDs(collectionId string, candidates []string, log *logger.UPPLogger) (Iterator, int, error)
	Ping(ctx context.Context) error
	Close()
}

type Iterator interface {
	Done() bool
	Next(result interface{}) bool
	Err() error
	Close() error
}

// MongoTX wraps a mongo session
type MongoTX struct {
	session *mgo.Session
}

// MongoDB wraps a mango mongo session
type MongoDB struct {
	Urls    string
	Timeout int
	lock    *sync.Mutex
	session *mgo.Session
	log     *logger.UPPLogger
}

func NewMongoDatabase(connection string, timeout int, log *logger.UPPLogger) *MongoDB {
	return &MongoDB{
		Urls:    connection,
		Timeout: timeout,
		lock:    &sync.Mutex{},
		log:     log,
	}
}

func (db *MongoDB) Open() (TX, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.session == nil {
		session, err := mgo.DialWithTimeout(db.Urls, time.Duration(db.Timeout)*time.Millisecond)
		if err != nil {
			db.log.WithError(err).Error("Session error")
			return nil, err
		}
		session.SetSocketTimeout(10 * time.Minute)
		db.session = session
		connections++

		if connections > expectedConnections {
			db.log.Warnf("There are more MongoDB connections opened than expected! "+
				"Are you sure this is what you want? Open connections: %v, expected %v.", connections, expectedConnections)
		}
	}

	return &MongoTX{db.session.Copy()}, nil
}

func (tx *MongoTX) FindUUIDs(collectionID string, candidates []string, log *logger.UPPLogger) (Iterator, int, error) {
	collection := tx.session.DB("upp-store").C(collectionID)

	query, projection := findUUIDsQueryElements(candidates)
	queryStr, _ := json.Marshal(query)
	log.WithField("query", string(queryStr)).Debug("Generated query")

	find := collection.Find(query).Select(projection).Batch(100)

	iter := find.Iter()
	count, err := find.Count()
	return iter, count, err
}

// Ping returns a mongo ping response
func (tx *MongoTX) Ping(ctx context.Context) error {
	ping := make(chan error, 1)
	go func() {
		ping <- tx.session.Ping()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ping:
		return err
	}
}

// Close closes the transaction
func (tx *MongoTX) Close() {
	tx.session.Close()
}

// Close closes the entire database connection
func (db *MongoDB) Close() {
	db.session.Close()
}

func (db *MongoDB) CheckHealth() (string, error) {
	tx, err := db.Open()
	if err != nil {
		return "", err
	}

	defer tx.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = tx.Ping(ctx)
	if err != nil {
		return "", err
	}

	return "OK", nil
}

var fieldsProjection = bson.M{
	"uuid":               1,
	"firstPublishedDate": 1,
	"publishedDate":      1,
}

func findUUIDsQueryElements(candidates []string) (bson.M, bson.M) {
	andQuery := []bson.M{
		{"$or": []bson.M{
			{"canBeDistributed": "yes"},
			{"canBeDistributed": bson.M{"$exists": false}},
		}},
		{"$and": []bson.M{
			{"type": "Article"},
			{"$or": []bson.M{
				{"body": bson.M{"$ne": nil}},
				{"bodyXML": bson.M{"$ne": nil}},
			},
			}},
		},
	}
	if candidates != nil && len(candidates) != 0 {
		andQuery = append(andQuery, bson.M{"uuid": bson.M{"$in": candidates}})
	}

	return bson.M{"$and": andQuery}, fieldsProjection
}
