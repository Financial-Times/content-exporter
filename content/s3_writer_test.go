package content

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func (m *mockS3WriterServer) startMockS3WriterServer(t *testing.T) *httptest.Server {
	router := mux.NewRouter()
	router.HandleFunc("/content/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		assert.Equal(t, "UPP Content Exporter", ua, "user-agent header")

		contentTypeHeader := r.Header.Get("Content-Type")
		tid := r.Header.Get("X-Request-Id")

		pathUuid, ok := mux.Vars(r)["uuid"]
		assert.NotNil(t, pathUuid)
		assert.True(t, ok)

		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.True(t, len(body) > 0)

		date := r.URL.Query().Get("date")

		w.WriteHeader(m.UploadRequest(pathUuid, tid, contentTypeHeader, date))

	}).Methods(http.MethodPut)

	router.HandleFunc("/content/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		assert.Equal(t, "UPP Content Exporter", ua, "user-agent header")

		tid := r.Header.Get("X-Request-Id")

		pathUuid, ok := mux.Vars(r)["uuid"]
		assert.NotNil(t, pathUuid)
		assert.True(t, ok)

		w.WriteHeader(m.DeleteRequest(pathUuid, tid))

	}).Methods(http.MethodDelete)

	router.HandleFunc("/__gtg", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(m.GTG())
	}).Methods(http.MethodGet)

	return httptest.NewServer(router)
}

func (m *mockS3WriterServer) GTG() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockS3WriterServer) UploadRequest(bodyUuid, tid, contentTypeHeader, date string) int {
	args := m.Called(bodyUuid, tid, contentTypeHeader, date)
	return args.Int(0)
}

func (m *mockS3WriterServer) DeleteRequest(bodyUuid, tid string) int {
	args := m.Called(bodyUuid, tid)
	return args.Int(0)
}

type mockS3WriterServer struct {
	mock.Mock
}

func TestS3UpdaterUploadContent(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	testData := []byte(testUUID)
	date := time.Now().UTC().Format("2006-01-02")

	mockServer := new(mockS3WriterServer)
	mockServer.On("UploadRequest", testUUID, "tid_1234", "application/json", date).Return(200)
	server := mockServer.startMockS3WriterServer(t)

	updater := newS3Updater(server.URL)

	err := updater.Upload(testData, "tid_1234", testUUID, date)
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterUploadContentErrorResponse(t *testing.T) {
	testUUID := uuid.NewUUID().String()
	testData := []byte(testUUID)
	date := time.Now().UTC().Format("2006-01-02")

	mockServer := new(mockS3WriterServer)
	mockServer.On("UploadRequest", testUUID, "tid_1234", "application/json", date).Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := newS3Updater(server.URL)

	err := updater.Upload(testData, "tid_1234", testUUID, date)
	assert.Error(t, err)
	assert.EqualError(t, err, "uploading content failed with unexpected status code: 503")
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterUploadContentWithErrorOnNewRequest(t *testing.T) {
	updater := newS3Updater("://")

	err := updater.Upload(nil, "tid_1234", "uuid1", "aDate")
	assert.Error(t, err)

	var urlErr *url.Error
	assert.True(t, errors.As(err, &urlErr))
	assert.Equal(t, urlErr.Op, "parse")
}

func TestS3UpdaterUploadContentErrorOnRequestDo(t *testing.T) {
	mockClient := new(mockHttpClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(&http.Response{}, errors.New("http client err"))

	updater := &S3Updater{apiClient: mockClient,
		writerBaseURL: "http://server",
	}

	err := updater.Upload(nil, "tid_1234", "uuid1", "aDate")
	assert.Error(t, err)
	assert.EqualError(t, err, "http client err")
	mockClient.AssertExpectations(t)
}

func TestS3UpdaterDeleteContent(t *testing.T) {
	testUUID := uuid.NewUUID().String()

	mockServer := new(mockS3WriterServer)
	mockServer.On("DeleteRequest", testUUID, "tid_1234").Return(204)
	server := mockServer.startMockS3WriterServer(t)

	updater := newS3Updater(server.URL)

	err := updater.Delete(testUUID, "tid_1234")
	assert.NoError(t, err)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterDeleteContentErrorResponse(t *testing.T) {
	testUUID := uuid.NewUUID().String()

	mockServer := new(mockS3WriterServer)
	mockServer.On("DeleteRequest", testUUID, "tid_1234").Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := newS3Updater(server.URL)

	err := updater.Delete(testUUID, "tid_1234")
	assert.Error(t, err)
	assert.EqualError(t, err, "deleting content failed with unexpected status code: 503")
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterDeleteContentErrorOnNewRequest(t *testing.T) {
	updater := newS3Updater("://")

	err := updater.Delete("uuid1", "tid_1234")
	assert.Error(t, err)

	var urlErr *url.Error
	assert.True(t, errors.As(err, &urlErr))
	assert.Equal(t, urlErr.Op, "parse")
}

func TestS3UpdaterDeleteContentErrorOnRequestDo(t *testing.T) {
	mockClient := new(mockHttpClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(&http.Response{}, errors.New("http client err"))

	updater := &S3Updater{apiClient: mockClient,
		writerBaseURL:   "http://server",
		writerHealthURL: "http://server",
	}

	err := updater.Delete("uuid1", "tid_1234")
	assert.Error(t, err)
	assert.EqualError(t, err, "http client err")
	mockClient.AssertExpectations(t)
}

func TestS3UpdaterCheckHealth(t *testing.T) {
	mockServer := new(mockS3WriterServer)
	mockServer.On("GTG").Return(200)
	server := mockServer.startMockS3WriterServer(t)

	updater := newS3Updater(server.URL)

	resp, err := updater.CheckHealth()
	assert.NoError(t, err)
	assert.Equal(t, "S3 Writer is good to go.", resp)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterCheckHealthError(t *testing.T) {
	mockServer := new(mockS3WriterServer)
	mockServer.On("GTG").Return(503)
	server := mockServer.startMockS3WriterServer(t)

	updater := newS3Updater(server.URL)

	resp, err := updater.CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "", resp)
	mockServer.AssertExpectations(t)
}

func TestS3UpdaterCheckHealthErrorOnNewRequest(t *testing.T) {
	updater := &S3Updater{
		writerHealthURL: "://",
	}

	resp, err := updater.CheckHealth()
	assert.Error(t, err)
	assert.Equal(t, "", resp)

	var urlErr *url.Error
	assert.True(t, errors.As(err, &urlErr))
	assert.Equal(t, urlErr.Op, "parse")
}

func TestS3UpdaterCheckHealthErrorOnRequestDo(t *testing.T) {
	mockClient := new(mockHttpClient)
	mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(&http.Response{}, errors.New("http client err"))

	updater := &S3Updater{
		writerBaseURL:   "http://server",
		writerHealthURL: "http://server",
		healthClient:    mockClient,
	}

	resp, err := updater.CheckHealth()
	assert.Error(t, err)
	assert.EqualError(t, err, "http client err")
	assert.Equal(t, "", resp)
	mockClient.AssertExpectations(t)
}

func newS3Updater(writerBaseURL string) *S3Updater {
	client := &http.Client{}
	return NewS3Updater(client, client, writerBaseURL, writerBaseURL+"/__gtg")
}
