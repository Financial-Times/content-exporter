package queue

import (
	"testing"
	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/assert"
	"github.com/pkg/errors"
	"time"
)

type mockFetcher struct {
	mock.Mock
}

func (m *mockFetcher) GetContent(uuid, tid string) (map[string]interface{}, error) {
	args := m.Called(uuid, tid)
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

type mockUpdater struct {
	mock.Mock
}

func (m *mockUpdater) Upload(content map[string]interface{}, tid, uuid, date string) error {
	args := m.Called(content, tid, uuid, date)
	return args.Error(0)
}

func (m *mockUpdater) Delete(uuid, tid string) error {
	args := m.Called(uuid, tid)
	return args.Error(0)
}

func TestKafkaContentNotificationHandlerHandleUpdateSuccessfully(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", Uuid: "uuid1"}, Tid: "tid_1234", EvType: UPDATE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)
	var testData map[string]interface{}
	fetcher.On("GetContent", n.Stub.Uuid, n.Tid).Return(testData, nil)
	updater.On("Upload", testData, n.Tid, n.Stub.Uuid, n.Stub.Date).Return(nil)

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.NoError(t, err)
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleUpdateWithError(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", Uuid: "uuid1"}, Tid: "tid_1234", EvType: UPDATE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)
	var testData map[string]interface{}
	fetcher.On("GetContent", n.Stub.Uuid, n.Tid).Return(testData, errors.New("Fetcher err"))

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.Error(t, err)
	assert.Equal(t, "UPDATE ERROR: Error getting content for uuid1: Fetcher err", err.Error())
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleUpdateWithQuitSignal(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", Uuid: "uuid1"}, Tid: "tid_1234", EvType: UPDATE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 30)
	go func() {
		time.Sleep(1)
		n.Quit <- struct{}{}
	}()
	err := contentNotificationHandler.HandleContentNotification(n)

	assert.Error(t, err)
	assert.Equal(t, "Shutdown signalled, delay waiting for UPDATE event terminated abruptly", err.Error())
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleDeleteSuccessfully(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", Uuid: "uuid1"}, Tid: "tid_1234", EvType: DELETE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)
	updater.On("Delete", n.Stub.Uuid, n.Tid).Return(nil)

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.NoError(t, err)
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleDeleteWithError(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", Uuid: "uuid1"}, Tid: "tid_1234", EvType: DELETE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)
	updater.On("Delete", n.Stub.Uuid, n.Tid).Return(errors.New("Updater err"))

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.Error(t, err)
	assert.Equal(t, "DELETE ERROR: Updater err", err.Error())
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func NewContentNotificationHandler(exporter *content.Exporter, delay int) ContentNotificationHandler {
	return NewKafkaContentNotificationHandler(exporter, delay)
}
