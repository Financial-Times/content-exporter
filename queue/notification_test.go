package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockFetcher struct {
	mock.Mock
}

func (m *mockFetcher) GetContent(uuid, tid string) ([]byte, error) {
	args := m.Called(uuid, tid)
	return args.Get(0).([]byte), args.Error(1)
}

type mockUpdater struct {
	mock.Mock
}

func (m *mockUpdater) Upload(content []byte, tid, uuid, date string) error {
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
	n := &Notification{Stub: content.Stub{Date: "aDate", UUID: "uuid1"}, Tid: "tid_1234", EvType: UPDATE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)

	var testData []byte
	fetcher.On("GetContent", n.Stub.UUID, n.Tid).Return(testData, nil)
	updater.On("Upload", testData, n.Tid, n.Stub.UUID, n.Stub.Date).Return(nil)

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.NoError(t, err)
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleUpdateWithError(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", UUID: "uuid1"}, Tid: "tid_1234", EvType: UPDATE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)
	var testData []byte
	fetcher.On("GetContent", n.Stub.UUID, n.Tid).Return(testData, fmt.Errorf("fetcher err"))

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.Error(t, err)
	assert.EqualError(t, err, "UPDATE ERROR: error getting content for uuid1: fetcher err")
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleUpdateWithQuitSignal(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", UUID: "uuid1"}, Tid: "tid_1234", EvType: UPDATE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 30)
	go func() {
		time.Sleep(500 * time.Millisecond)
		n.Quit <- struct{}{}
	}()
	err := contentNotificationHandler.HandleContentNotification(n)

	assert.Error(t, err)
	assert.EqualError(t, err, "shutdown signalled, delay waiting for UPDATE event terminated abruptly")
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleDeleteSuccessfully(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", UUID: "uuid1"}, Tid: "tid_1234", EvType: DELETE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)
	updater.On("Delete", n.Stub.UUID, n.Tid).Return(nil)

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.NoError(t, err)
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func TestKafkaContentNotificationHandlerHandleDeleteWithError(t *testing.T) {
	fetcher := new(mockFetcher)
	updater := new(mockUpdater)
	n := &Notification{Stub: content.Stub{Date: "aDate", UUID: "uuid1"}, Tid: "tid_1234", EvType: DELETE, Terminator: export.NewTerminator()}
	contentNotificationHandler := NewContentNotificationHandler(content.NewExporter(fetcher, updater), 0)
	updater.On("Delete", n.Stub.UUID, n.Tid).Return(fmt.Errorf("updater err"))

	err := contentNotificationHandler.HandleContentNotification(n)

	assert.Error(t, err)
	assert.EqualError(t, err, "DELETE ERROR: updater err")
	fetcher.AssertExpectations(t)
	updater.AssertExpectations(t)
}

func NewContentNotificationHandler(exporter *content.Exporter, delay int) ContentNotificationHandler {
	log := logger.NewUPPLogger("test", "PANIC")
	return NewKafkaContentNotificationHandler(exporter, delay, log)
}
