package db

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2/bson"
)

func TestCreateDB(t *testing.T) {
	mongo := NewMongoDatabase("test-url", 30000)
	assert.Equal(t, "test-url", mongo.Urls)
	assert.Equal(t, 30000, mongo.Timeout)
	assert.NotNil(t, mongo.lock)
}

func TestPing(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()

	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	err = tx.Ping(context.Background())
	assert.NoError(t, err)
}

func TestDBCloses(t *testing.T) {
	mongo := startMongo(t)
	tx, err := mongo.Open()
	assert.NoError(t, err)

	tx.Close()
	mongo.Close()
	assert.Panics(t, func() {
		err := mongo.(*MongoDB).session.Ping()
		if err != nil {
			return
		}
	})
}

func TestDBCheckHealth(t *testing.T) {
	mongo := startMongo(t)
	defer mongo.Close()
	tx, err := mongo.Open()
	defer tx.Close()
	assert.NoError(t, err)

	output, err := mongo.(*MongoDB).CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "OK", output)
}

func startMongo(t *testing.T) Service {
	if testing.Short() {
		t.Skip("Mongo integration for long tests only.")
	}

	mongoURL := os.Getenv("MONGO_TEST_URL")
	if strings.TrimSpace(mongoURL) == "" {
		t.Fatal("Please set the environment variable MONGO_TEST_URL to run mongo integration tests (e.g. MONGO_TEST_URL=localhost:27017). Alternatively, run `go test -short` to skip them.")
	}

	return NewMongoDatabase(mongoURL, 30000)
}

func insertTestContent(t *testing.T, mongo *MongoDB, testContent map[string]interface{}) {
	session := mongo.session.Copy()
	defer session.Close()

	err := session.DB("upp-store").C("testing").Insert(testContent)
	assert.NoError(t, err)
}

func cleanupTestContent(t *testing.T, mongo *MongoDB, testUUIDs ...string) {
	session := mongo.session.Copy()
	defer session.Close()
	for _, testUUID := range testUUIDs {
		err := session.DB("upp-store").C("testing").Remove(bson.M{"uuid": testUUID})
		assert.NoError(t, err)
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
	mongo := startMongo(t)
	defer mongo.Close()

	tx, err := mongo.Open()
	require.NoError(t, err)
	defer tx.Close()

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
				insertTestContent(t, mongo.(*MongoDB), toInsert)
			}
			defer cleanupTestContent(t, mongo.(*MongoDB), uuids...)

			iter, count, err := tx.FindUUIDs("testing", test.candidates)
			require.NoError(t, err)
			defer func(iter Iterator) {
				err := iter.Close()
				if err != nil {
					t.Logf("Failed to close iterator")
				}
			}(iter)
			require.NoError(t, iter.Err())
			require.Equal(t, len(test.expectedResultUUIDs), count)

			var resultEntry map[string]interface{}
			for iter.Next(&resultEntry) {
				resUUID := resultEntry["uuid"].(string)
				assert.Contains(t, test.expectedResultUUIDs, resUUID)
			}
		})
	}
}
