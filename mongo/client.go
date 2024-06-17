package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/upp-go-sdk/pkg/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client struct {
	client              *mongo.Client
	database            string
	collection          string
	allowedContentTypes []string
	allowedPublishUUIDs []string
	log                 *logger.UPPLogger
}

func NewClient(
	ctx context.Context,
	address, username, password, database, collection string,
	allowedContentTypes, allowedPublishUUIDs []string,
	log *logger.UPPLogger,
) (*Client, error) {
	client, err := mongodb.NewClient(ctx, mongodb.ConnectionParams{
		Host:     address,
		Username: username,
		Password: password,
		UseSrv:   true,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client:              client,
		database:            database,
		collection:          collection,
		allowedContentTypes: allowedContentTypes,
		allowedPublishUUIDs: allowedPublishUUIDs,
		log:                 log,
	}, nil
}

func (c *Client) findContent(ctx context.Context, candidates []string) (cursor, int, error) {
	collection := c.client.Database(c.database).Collection(c.collection)

	query, projection := findUUIDsQueryElements(candidates, c.allowedContentTypes, c.allowedPublishUUIDs)
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

func findUUIDsQueryElements(candidates, allowedContentTypes, allowedPublishUUIDs []string) (bson.M, bson.M) {
	//Mongo expects empty arrays not nil
	if allowedContentTypes == nil {
		allowedContentTypes = []string{}
	}
	if allowedPublishUUIDs == nil {
		allowedPublishUUIDs = []string{}
	}

	andQuery := []bson.M{
		{"$or": []bson.M{
			{"canBeDistributed": "yes"},
			{"canBeDistributed": bson.M{"$exists": false}},
		}},
		{"$and": []bson.M{
			{"type": bson.M{"$in": allowedContentTypes}},
			{"$or": []bson.M{
				{"body": bson.M{"$ne": nil}},
				{"bodyXML": bson.M{"$ne": nil}},
			},
			}},
		},
		{"$or": []bson.M{
			{"publication": bson.M{"$in": allowedPublishUUIDs}},
			{"publication": bson.M{"$exists": false}},
		}},
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
