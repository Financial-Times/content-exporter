package content

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExporterHandleContentWithValidContent(t *testing.T) {
	tid := "tid_1234"
	stubUUID := "uuid1"
	date := "2017-10-09"
	testData := []byte(stubUUID)
	fetcher := &mockFetcher{t: t, expectedUUID: stubUUID, expectedTid: tid, result: testData}
	updater := &mockUpdater{t: t, expectedUUID: stubUUID, expectedTid: tid, expectedDate: date, expectedPayload: testData}

	exporter := NewExporter(fetcher, updater)
	err := exporter.Export(tid, &Stub{stubUUID, date, "", nil})

	assert.NoError(t, err)
	assert.True(t, fetcher.called)
	assert.True(t, updater.called)
}

func TestExporterHandleContentWithErrorFromFetcher(t *testing.T) {
	tid := "tid_1234"
	stubUUID := "uuid1"
	date := "2017-10-09"
	testData := []byte("uuid: " + stubUUID)
	fetcher := &mockFetcher{t: t, expectedUUID: stubUUID, expectedTid: tid, result: testData, err: fmt.Errorf("fetcher err")}
	updater := &mockUpdater{t: t}

	exporter := NewExporter(fetcher, updater)
	err := exporter.Export(tid, &Stub{stubUUID, date, "", nil})

	assert.Error(t, err)
	assert.EqualError(t, err, "getting content: fetcher err")
	assert.True(t, fetcher.called)
	assert.False(t, updater.called)
}

func TestExporterHandleContentWithErrorFromUpdater(t *testing.T) {
	tid := "tid_1234"
	stubUUID := "uuid1"
	date := "2017-10-09"
	testData := []byte("uuid: " + stubUUID)
	fetcher := &mockFetcher{t: t, expectedUUID: stubUUID, expectedTid: tid, result: testData}
	updater := &mockUpdater{t: t, expectedUUID: stubUUID, expectedTid: tid, expectedDate: date, expectedPayload: testData, err: fmt.Errorf("updater err")}

	exporter := NewExporter(fetcher, updater)
	err := exporter.Export(tid, &Stub{stubUUID, date, "", nil})

	assert.Error(t, err)
	assert.EqualError(t, err, "uploading content: updater err")
	assert.True(t, fetcher.called)
	assert.True(t, updater.called)
}

type mockFetcher struct {
	t                         *testing.T
	expectedUUID, expectedTid string
	result                    []byte
	err                       error
	called                    bool
}

func (f *mockFetcher) GetContent(uuid, tid string) ([]byte, error) {
	assert.Equal(f.t, f.expectedUUID, uuid)
	assert.Equal(f.t, f.expectedTid, tid)
	f.called = true
	return f.result, f.err
}

type mockUpdater struct {
	t                                       *testing.T
	expectedUUID, expectedTid, expectedDate string
	expectedPayload                         []byte
	err                                     error
	called                                  bool
}

func (u *mockUpdater) Upload(content []byte, tid, uuid, date string) error {
	assert.Equal(u.t, u.expectedUUID, uuid)
	assert.Equal(u.t, u.expectedTid, tid)
	assert.Equal(u.t, u.expectedDate, date)
	assert.Equal(u.t, u.expectedPayload, content)
	u.called = true
	return u.err
}

func (u *mockUpdater) Delete(_, _ string) error {
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
