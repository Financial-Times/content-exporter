package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client struct {
	client     *mongo.Client
	database   string
	collection string
	log        *logger.UPPLogger
}

func NewClient(ctx context.Context, uri, database, collection string, timeout time.Duration, log *logger.UPPLogger) (*Client, error) {
	uri = fmt.Sprintf("mongodb://%s", uri)
	opts := options.Client().
		ApplyURI(uri).
		SetSocketTimeout(timeout)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Client{
		client:     client,
		database:   database,
		collection: collection,
		log:        log,
	}, nil
}

func (c *Client) findContent(ctx context.Context, candidates []string) (cursor, int, error) {
	collection := c.client.Database(c.database).Collection(c.collection)

	query, projection := findUUIDsQueryElements(candidates)
	queryStr, _ := json.Marshal(query)
	c.log.WithField("query", string(queryStr)).Debug("Generated query")

	opts := options.Find().
		SetProjection(projection).
		SetBatchSize(100)

	cur, err := collection.Find(ctx, query, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("error finding documents: %w", err)
	}

	count, err := collection.CountDocuments(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("error counting documents: %w", err)
	}

	return cur, int(count), nil
}

func (c *Client) CheckHealth() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := c.client.Ping(ctx, readpref.Primary())
	if err != nil {
		return "", err
	}

	return "OK", nil
}

func (c *Client) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	return c.client.Disconnect(ctx)
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
	if len(candidates) != 0 {
		andQuery = append(andQuery, bson.M{"uuid": bson.M{"$in": candidates}})
	}

	fieldsProjection := bson.M{
		"uuid":               1,
		"firstPublishedDate": 1,
		"publishedDate":      1,
	}

	return bson.M{"$and": andQuery}, fieldsProjection
}
