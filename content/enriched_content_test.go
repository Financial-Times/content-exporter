package content

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockHTTPClient struct {
	mock.Mock
}

func (c *mockHTTPClient) Do(req *http.Request) (resp *http.Response, err error) {
	args := c.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

type mockEnrichedContentServer struct {
	mock.Mock
}

func (m *mockEnrichedContentServer) startMockEnrichedContentServer(t *testing.T) *httptest.Server {
	router := mux.NewRouter()
	router.HandleFunc("/enrichedcontent/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		assert.Equal(t, "UPP Content Exporter", ua, "user-agent header")

		acceptHeader := r.Header.Get("Accept")
		tid := r.Header.Get("X-Request-Id")
		xPolicyHeader := r.Header.Get("X-Policy")
		authHeader := r.Header.Get("Authorization")

		uuid, ok := mux.Vars(r)["uuid"]
		assert.NotNil(t, uuid)
		assert.True(t, ok)

		respStatus, resp := m.GetRequest(authHeader, tid, acceptHeader, xPolicyHeader)
		w.WriteHeader(respStatus)
		_, err := w.Write(resp)
		assert.NoError(t, err)
	}).Methods(http.MethodGet)

	router.HandleFunc("/__gtg", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(m.GTG())
	}).Methods(http.MethodGet)

	return httptest.NewServer(router)
}

func (m *mockEnrichedContentServer) GTG() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockEnrichedContentServer) GetRequest(authHeader, tid, acceptHeader, xPolicyHeader string) (int, []byte) {
	args := m.Called(authHeader, tid, acceptHeader, xPolicyHeader)
	var resp []byte
	if len(args) > 1 {
		resp = args.Get(1).([]byte)
	}
	return args.Int(0), resp
}

func enrichedContentURL(baseURL string) string {
	return baseURL + "/enrichedcontent/"
}

func TestEnrichedContentFetcherGetValidContent(t *testing.T) {
	testUUID := uuid.New().String()
	testData := []byte(testUUID)
	mockServer := new(mockEnrichedContentServer)
	mockServer.On("GetRequest", "", "tid_1234", "application/json", "").Return(200, testData)

	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := newEnrichedContentFetcher(enrichedContentURL(server.URL), "", "")

	resp, err := fetcher.GetContent(testUUID, "tid_1234")

	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
	assert.Equal(t, len(testData), len(resp))
	assert.Equal(t, testUUID, string(resp))
}

func TestEnrichedContentFetcherGetValidContentWithAuthorizationAndXPolicy(t *testing.T) {
	testUUID := uuid.New().String()
	testData := []byte(testUUID)
	mockServer := new(mockEnrichedContentServer)
	auth := "auth-string"
	xPolicies := "xpolicies"
	mockServer.On("GetRequest", auth, "tid_1234", "application/json", xPolicies).Return(200, testData)

	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := newEnrichedContentFetcher(enrichedContentURL(server.URL), auth, xPolicies)

	resp, err := fetcher.GetContent(testUUID, "tid_1234")
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
	assert.Equal(t, len(testData), len(resp))
	assert.Equal(t, testUUID, string(resp))
}

func TestEnrichedContentFetcherGetContentWithAuthError(t *testing.T) {
	testUUID := uuid.New().String()
	mockServer := new(mockEnrichedContentServer)
	auth := "auth-string"
	xPolicies := "xpolicies"
	mockServer.On("GetRequest", auth, "tid_1234", "application/json", xPolicies).Return(http.StatusUnauthorized)

	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := newEnrichedContentFetcher(enrichedContentURL(server.URL), auth, xPolicies)

	_, err := fetcher.GetContent(testUUID, "tid_1234")
	assert.Error(t, err)
	mockServer.AssertExpectations(t)
	assert.EqualError(t, err, "fetching enriched content failed with unexpected status code: 401")
}

func TestEnrichedContentFetcherGetContentWithErrorOnNewRequest(t *testing.T) {
	fetcher := &EnrichedContentFetcher{apiClient: &http.Client{},
		enrichedContentAPIURL: "://",
	}

	_, err := fetcher.GetContent("uuid1", "tid_1234")
	assert.Error(t, err)

	var urlErr *url.Error
	assert.True(t, errors.As(err, &urlErr))
	assert.Equal(t, urlErr.Op, "parse")
}

func TestEnrichedContentFetcherGetContentErrorOnRequestDo(t *testing.T) {
	mockClient := new(mockHTTPClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(&http.Response{}, errors.New("http client err"))

	fetcher := &EnrichedContentFetcher{apiClient: mockClient,
		enrichedContentAPIURL: "http://server",
	}

	_, err := fetcher.GetContent("uuid1", "tid_1234")
	assert.Error(t, err)
	assert.EqualError(t, err, "http client err")
	mockClient.AssertExpectations(t)
}

func TestEnrichedContentFetcherCheckHealth(t *testing.T) {
	mockServer := new(mockEnrichedContentServer)
	mockServer.On("GTG").Return(200)
	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := newEnrichedContentFetcher(server.URL, "", "")

	resp, err := fetcher.CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "EnrichedContent fetcher is good to go.", resp)
	mockServer.AssertExpectations(t)
}

func TestEnrichedContentFetcherCheckHealthError(t *testing.T) {
	mockServer := new(mockEnrichedContentServer)
	mockServer.On("GTG").Return(503)
	server := mockServer.startMockEnrichedContentServer(t)

	fetcher := newEnrichedContentFetcher(server.URL, "", "")

	resp, err := fetcher.CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "", resp)
	mockServer.AssertExpectations(t)
}

func TestEnrichedContentFetcherCheckHealthErrorOnNewRequest(t *testing.T) {
	fetcher := &EnrichedContentFetcher{
		enrichedContentHealthURL: "://",
	}

	resp, err := fetcher.CheckHealth()

	assert.Error(t, err)
	assert.Equal(t, "", resp)

	var urlErr *url.Error
	assert.True(t, errors.As(err, &urlErr))
	assert.Equal(t, urlErr.Op, "parse")
}

func TestEnrichedContentFetcherCheckHealthErrorOnRequestDo(t *testing.T) {
	mockClient := new(mockHTTPClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(&http.Response{}, errors.New("http client err"))

	fetcher := &EnrichedContentFetcher{
		enrichedContentHealthURL: "http://server",
		authorization:            "some-auth",
		healthClient:             mockClient,
	}

	resp, err := fetcher.CheckHealth()
	assert.Error(t, err)
	assert.EqualError(t, err, "http client err")
	assert.Equal(t, "", resp)
	mockClient.AssertExpectations(t)
}

func newEnrichedContentFetcher(enrichedContentAPIURL, auth, xPolicies string) *EnrichedContentFetcher {
	client := &http.Client{}
	return NewEnrichedContentFetcher(client, client, enrichedContentAPIURL, enrichedContentAPIURL+"/__gtg", xPolicies, auth)
}
