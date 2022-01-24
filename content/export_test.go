package content

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestExporterHandleContentWithValidContent(t *testing.T) {
	tid := "tid_1234"
	stubUuid := "uuid1"
	date := "2017-10-09"
	testData := []byte(stubUuid)
	fetcher := &mockFetcher{t: t, expectedUuid: stubUuid, expectedTid: tid, result: testData}
	updater := &mockUpdater{t: t, expectedUuid: stubUuid, expectedTid: tid, expectedDate: date, expectedPayload: testData}

	exporter := NewExporter(fetcher, updater)
	err := exporter.HandleContent(tid, Stub{stubUuid, date, "", nil})

	assert.NoError(t, err)
	assert.True(t, fetcher.called)
	assert.True(t, updater.called)
}

func TestExporterHandleContentWithErrorFromFetcher(t *testing.T) {
	tid := "tid_1234"
	stubUuid := "uuid1"
	date := "2017-10-09"
	testData := []byte("uuid: " + stubUuid)
	fetcher := &mockFetcher{t: t, expectedUuid: stubUuid, expectedTid: tid, result: testData, err: errors.New("fetcher err")}
	updater := &mockUpdater{t: t}

	exporter := NewExporter(fetcher, updater)
	err := exporter.HandleContent(tid, Stub{stubUuid, date, "", nil})

	assert.Error(t, err)
	assert.Equal(t, "error getting content for uuid1: fetcher err", err.Error())
	assert.True(t, fetcher.called)
	assert.False(t, updater.called)
}

func TestExporterHandleContentWithErrorFromUpdater(t *testing.T) {
	tid := "tid_1234"
	stubUuid := "uuid1"
	date := "2017-10-09"
	testData := []byte("uuid: " + stubUuid)
	fetcher := &mockFetcher{t: t, expectedUuid: stubUuid, expectedTid: tid, result: testData}
	updater := &mockUpdater{t: t, expectedUuid: stubUuid, expectedTid: tid, expectedDate: date, expectedPayload: testData, err: errors.New("updater err")}

	exporter := NewExporter(fetcher, updater)
	err := exporter.HandleContent(tid, Stub{stubUuid, date, "", nil})

	assert.Error(t, err)
	assert.Equal(t, "error uploading content for uuid1: updater err", err.Error())
	assert.True(t, fetcher.called)
	assert.True(t, updater.called)
}

type mockFetcher struct {
	t                         *testing.T
	expectedUuid, expectedTid string
	result                    []byte
	err                       error
	called                    bool
}

func (f *mockFetcher) GetContent(uuid, tid string) ([]byte, error) {
	assert.Equal(f.t, f.expectedUuid, uuid)
	assert.Equal(f.t, f.expectedTid, tid)
	f.called = true
	return f.result, f.err
}

type mockUpdater struct {
	t                                       *testing.T
	expectedUuid, expectedTid, expectedDate string
	expectedPayload                         []byte
	err                                     error
	called                                  bool
}

func (u *mockUpdater) Upload(content []byte, tid, uuid, date string) error {
	assert.Equal(u.t, u.expectedUuid, uuid)
	assert.Equal(u.t, u.expectedTid, tid)
	assert.Equal(u.t, u.expectedDate, date)
	assert.Equal(u.t, u.expectedPayload, content)
	u.called = true
	return u.err
}

func (u *mockUpdater) Delete(uuid, tid string) error {
	panic("should not be called")
}

func TestGetDateWhenFirstPublishedDateIsPresent(t *testing.T) {
	expectedDate := "2006-01-02"
	firsPublishDate := expectedDate + "T15:04:05Z07:00"
	publishDate := "2017-01-19T15:04:05Z07:00"
	testData := make(map[string]interface{})
	testData["firstPublishedDate"] = firsPublishDate
	testData["publishedDate"] = publishDate

	actualDate := GetDateOrDefault(testData)
	assert.Equal(t, expectedDate, actualDate)
}

func TestGetDateWhenNoFirstPublishedDateButPublishDateIsPresent(t *testing.T) {
	expectedDate := "2017-01-19"
	publishDate := "2017-01-19T15:04:05Z07:00"
	testData := make(map[string]interface{})
	testData["publishedDate"] = publishDate

	actualDate := GetDateOrDefault(testData)
	assert.Equal(t, expectedDate, actualDate)
}

func TestGetDateWhenNeitherFirstPublishedDateNorPublishDateIsPresent(t *testing.T) {
	expectedDate := "0000-00-00"
	testData := make(map[string]interface{})

	actualDate := GetDateOrDefault(testData)
	assert.Equal(t, expectedDate, actualDate)
}
