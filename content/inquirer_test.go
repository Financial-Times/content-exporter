package content

import (
	"context"
	"testing"
	"time"

	"github.com/Financial-Times/content-exporter/db"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDbService struct {
	mock.Mock
}

func (m *mockDbService) Open() (db.TX, error) {
	args := m.Called()
	return args.Get(0).(db.TX), args.Error(1)
}

func (m *mockDbService) Close() {
	m.Called()
}

type mockTX struct {
	mock.Mock
}

func (tx *mockTX) FindUUIDs(collectionID string, candidates []string) (db.Iterator, int, error) {
	args := tx.Called(collectionID, candidates)
	return args.Get(0).(db.Iterator), args.Int(1), args.Error(2)
}

func (tx *mockTX) Ping(ctx context.Context) error {
	panic("implement me")
}

func (tx *mockTX) Close() {
	tx.Called()
}

type MockDBIter struct {
	mock.Mock
}

func (m *MockDBIter) Done() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockDBIter) Next(result interface{}) bool {
	args := m.Called(result)
	return args.Bool(0)
}

func (m *MockDBIter) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDBIter) Timeout() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockDBIter) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestMongoInquirerInquireSuccessfully(t *testing.T) {
	mockDb := new(mockDbService)
	mockTx := new(mockTX)
	mockIter := new(MockDBIter)

	testCollection := "testing"
	testUUID := "uuid1"

	mockDb.On("Open").Return(mockTx, nil)
	mockTx.On("Close")
	mockTx.On("FindUUIDs", testCollection, mock.AnythingOfType("[]string")).Return(mockIter, 1, nil)
	mockIter.On("Next", mock.AnythingOfType("*map[string]interface {}")).Return(true).Run(func(args mock.Arguments) {
		arg := args.Get(0).(*map[string]interface{})
		*arg = make(map[string]interface{})
		(*arg)["uuid"] = testUUID
	}).Once()
	mockIter.On("Next", mock.AnythingOfType("*map[string]interface {}")).Return(false)
	mockIter.On("Close").Return(nil)
	inquirer := NewInquirer(mockDb)

	docCh, count, err := inquirer.Inquire(testCollection, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
waitLoop:
	for {
		select {
		case doc, open := <-docCh:
			if !open {
				break waitLoop
			}
			assert.Equal(t, testUUID, doc.UUID)
			assert.Equal(t, DefaultDate, doc.Date)

		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
	mockDb.AssertExpectations(t)
	mockTx.AssertExpectations(t)
	mockIter.AssertExpectations(t)
}

func TestMongoInquirerInquireWithoutValidContent(t *testing.T) {
	mockDb := new(mockDbService)
	mockTx := new(mockTX)
	mockIter := new(MockDBIter)

	testCollection := "testing"
	testUUID := "uuid1"
	candidates := []string{testUUID}

	mockDb.On("Open").Return(mockTx, nil)
	mockTx.On("Close")
	mockTx.On("FindUUIDs", testCollection, candidates).Return(mockIter, 1, nil)
	mockIter.On("Next", mock.AnythingOfType("*map[string]interface {}")).Return(true).Run(func(args mock.Arguments) {
		arg := args.Get(0).(*map[string]interface{})
		*arg = make(map[string]interface{})
	}).Once()
	mockIter.On("Next", mock.AnythingOfType("*map[string]interface {}")).Return(false)
	mockIter.On("Close").Return(nil)
	inquirer := NewInquirer(mockDb)

	docCh, count, err := inquirer.Inquire(testCollection, candidates)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
waitLoop:
	for {
		select {
		case _, open := <-docCh:
			if !open {
				break waitLoop
			}
			t.FailNow()
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
	mockDb.AssertExpectations(t)
	mockTx.AssertExpectations(t)
	mockIter.AssertExpectations(t)
}

func TestMongoInquirerInquireErrorFindingUUIDs(t *testing.T) {
	mockDb := new(mockDbService)
	mockTx := new(mockTX)
	mockIter := new(MockDBIter)

	testCollection := "testing"
	testUUID := "uuid1"
	candidates := []string{testUUID}

	mockDb.On("Open").Return(mockTx, nil)
	mockTx.On("Close")
	mockTx.On("FindUUIDs", testCollection, candidates).Return(mockIter, 0, errors.New("Mongo err"))

	inquirer := NewInquirer(mockDb)

	docCh, count, err := inquirer.Inquire(testCollection, candidates)
	assert.Error(t, err)
	assert.Equal(t, "Mongo err", err.Error())
	assert.Equal(t, 0, count)
	assert.Nil(t, docCh)

	mockDb.AssertExpectations(t)
	mockTx.AssertExpectations(t)
	mockIter.AssertExpectations(t)
}

func TestMongoInquirerInquireErrorOpenMongo(t *testing.T) {
	mockDb := new(mockDbService)
	mockTx := new(mockTX)
	mockIter := new(MockDBIter)

	testCollection := "testing"
	testUUID := "uuid1"
	candidates := []string{testUUID}

	mockDb.On("Open").Return(mockTx, errors.New("Mongo err"))

	inquirer := NewInquirer(mockDb)

	docCh, count, err := inquirer.Inquire(testCollection, candidates)
	assert.Error(t, err)
	assert.Equal(t, "Mongo err", err.Error())
	assert.Equal(t, 0, count)
	assert.Nil(t, docCh)

	mockDb.AssertExpectations(t)
	mockTx.AssertExpectations(t)
	mockIter.AssertExpectations(t)
}

func NewInquirer(svc *mockDbService) Inquirer {
	return NewMongoInquirer(svc)
}
