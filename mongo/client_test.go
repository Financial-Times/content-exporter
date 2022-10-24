package mongo

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func setupConnection(t *testing.T) (*Client, func()) {
	if testing.Short() {
		t.Skip("Mongo integration for long tests only.")
	}

	mongoURL := os.Getenv("MONGO_TEST_URL")
	if strings.TrimSpace(mongoURL) == "" {
		t.Fatal("Please set the environment variable MONGO_TEST_URL to run mongo integration tests (e.g. MONGO_TEST_URL=localhost:27017). Alternatively, run `go test -short` to skip them.")
	}

	database := "upp-store"
	collection := "testing"
	timeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log := logger.NewUPPLogger("test", "PANIC")

	client, err := NewClient(ctx, mongoURL, database, collection, log)
	require.NoError(t, err)

	return client, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		assert.NoError(t, client.Close(ctx))
	}
}

func TestMongoClient_Ping(t *testing.T) {
	client, teardown := setupConnection(t)
	defer teardown()

	assert.NoError(t, client.client.Ping(context.Background(), nil))
}

func TestMongoClient_PingAfterShutdown(t *testing.T) {
	client, teardown := setupConnection(t)
	teardown()

	assert.Error(t, client.client.Ping(context.Background(), nil))
}

func TestClient_CheckHealth(t *testing.T) {
	client, teardown := setupConnection(t)
	defer teardown()

	output, err := client.CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "OK", output)
}

func insertTestContent(ctx context.Context, t *testing.T, client *Client, testContent map[string]interface{}) {
	_, err := client.client.
		Database("upp-store").
		Collection("testing").
		InsertOne(ctx, testContent)
	require.NoError(t, err)
}

func cleanupTestContent(ctx context.Context, t *testing.T, client *Client, testUUIDs ...string) {
	for _, uuid := range testUUIDs {
		result, err := client.client.
			Database("upp-store").
			Collection("testing").
			DeleteOne(ctx, bson.M{"uuid": uuid})
		require.NoError(t, err)
		assert.EqualValues(t, 1, result.DeletedCount)
	}
}

func TestMongo_FindUUIDs(t *testing.T) {
	emptyResult := make([]string, 0)
	type content struct {
		uuid             string
		cType            string
		canBeDistributed *string
		body             *string
		bodyXML          *string
	}
	stringAsPtr := func(s string) *string {
		return &s
	}
	tests := []struct {
		name                string
		existingContent     []content
		candidates          []string
		expectedResultUUIDs []string
	}{
		{
			name: "Test that content with irrelevant content type will not be fetched",
			existingContent: []content{
				{
					uuid:    "a164336a-7e3e-48ff-a3fe-e1bf1c8c0d4e",
					cType:   "LiveBlog",
					bodyXML: stringAsPtr("<body> Simple body </body>"),
				},
			},
			expectedResultUUIDs: emptyResult,
		},
		{
			name: "Test that content with relevant content type and no body or bodyXML will not be fetched",
			existingContent: []content{
				{
					uuid:  "2c1d77f1-c087-495b-bcd7-0680844d622b",
					cType: "Article",
				},
			},
			expectedResultUUIDs: emptyResult,
		},
		{
			name: "Test that content with relevant content type, existing bodyXML and distribution flag set to no will not be fetched",
			existingContent: []content{
				{
					uuid:             "fd1f2c02-711f-4cc2-941e-0f03a62b8406",
					cType:            "Article",
					bodyXML:          stringAsPtr("<body> Simple body </body>"),
					canBeDistributed: stringAsPtr("no"),
				},
			},
			expectedResultUUIDs: emptyResult,
		},
		{
			name: "Test that content with relevant content type, existing bodyXML and valid distribution flag will be fetched",
			existingContent: []content{
				{
					uuid:             "test-uuid-1",
					cType:            "Article",
					bodyXML:          stringAsPtr("<body> Simple body </body>"),
					canBeDistributed: stringAsPtr("yes"),
				},
			},
			expectedResultUUIDs: []string{"test-uuid-1"},
		},
		{
			name: "Test that content with relevant content type, existing body and valid distribution flag will be fetched",
			existingContent: []content{
				{
					uuid:             "test-uuid-2",
					cType:            "Article",
					body:             stringAsPtr("<body> Simple body </body>"),
					canBeDistributed: stringAsPtr("yes"),
				},
			},
			expectedResultUUIDs: []string{"test-uuid-2"},
		},
		{
			name: "Test that content with relevant content type, existing body and non-existing distribution flag will be fetched",
			existingContent: []content{
				{
					uuid:  "test-uuid-3",
					cType: "Article",
					body:  stringAsPtr("<body> Simple body </body>"),
				},
			},
			expectedResultUUIDs: []string{"test-uuid-3"},
		},
		{
			name: "Test that valid content will be fetched when it is among candidates",
			existingContent: []content{
				{
					uuid:             "test-uuid-4",
					cType:            "Article",
					body:             stringAsPtr("<body> Simple body </body>"),
					canBeDistributed: stringAsPtr("yes"),
				},
				{
					uuid:             "test-uuid-5",
					cType:            "Article",
					bodyXML:          stringAsPtr("<body> Simple body </body>"),
					canBeDistributed: stringAsPtr("yes"),
				},
				{
					uuid:             "test-uuid-6",
					cType:            "Article",
					bodyXML:          stringAsPtr("<body> Another simple body </body>"),
					canBeDistributed: stringAsPtr("yes"),
				},
				{
					uuid:             "test-uuid-7",
					cType:            "LiveBlog",
					bodyXML:          stringAsPtr("<body> Another simple body </body>"),
					canBeDistributed: stringAsPtr("yes"),
				},
			},
			candidates:          []string{"test-uuid-4", "test-uuid-6"},
			expectedResultUUIDs: []string{"test-uuid-4", "test-uuid-6"},
		},
	}
	client, teardown := setupConnection(t)
	defer teardown()

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			uuids := make([]string, 0)
			for _, content := range test.existingContent {
				uuids = append(uuids, content.uuid)
				toInsert := make(map[string]interface{})
				toInsert["uuid"] = content.uuid
				toInsert["type"] = content.cType
				toInsert["body"] = content.body
				toInsert["bodyXML"] = content.bodyXML
				if content.canBeDistributed != nil {
					toInsert["canBeDistributed"] = content.canBeDistributed
				}
				insertTestContent(ctx, t, client, toInsert)
			}
			defer cleanupTestContent(ctx, t, client, uuids...)

			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			iter, count, err := client.findContent(readCtx, test.candidates)
			require.NoError(t, err)

			defer func() {
				assert.NoError(t, iter.Close(ctx))
			}()

			require.Len(t, test.expectedResultUUIDs, count)

			for iter.Next(ctx) {
				var entry map[string]interface{}
				require.NoError(t, iter.Decode(&entry))

				assert.Contains(t, test.expectedResultUUIDs, entry["uuid"].(string))
			}

			assert.NoError(t, iter.Err())
		})
	}
}
