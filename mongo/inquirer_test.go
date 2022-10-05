package mongo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type mockFinder struct {
	mock.Mock
}

func (f *mockFinder) findContent(ctx context.Context, candidates []string) (cursor, int, error) {
	args := f.Called(ctx, candidates)
	return args.Get(0).(cursor), args.Int(1), args.Error(2)
}

type mockCursor struct {
	mock.Mock
}

func (c *mockCursor) Next(ctx context.Context) bool {
	args := c.Called(ctx)
	return args.Bool(0)
}

func (c *mockCursor) Decode(val interface{}) error {
	args := c.Called(val)
	return args.Error(0)
}

func (c *mockCursor) Err() error {
	args := c.Called()
	return args.Error(0)
}

func (c *mockCursor) Close(ctx context.Context) error {
	args := c.Called(ctx)
	return args.Error(0)
}

func TestInquirer_InquireSuccessfully(t *testing.T) {
	finder := new(mockFinder)
	cursor := new(mockCursor)

	testUUID := "uuid1"
	ctx := context.Background()
	log := logger.NewUPPLogger("test", "PANIC")

	finder.On("findContent", ctx, mock.AnythingOfType("[]string")).Return(cursor, 1, nil)
	cursor.On("Next", ctx).Return(true).Once()
	cursor.On("Decode", mock.AnythingOfType("*primitive.M")).Return(nil).
		Run(func(args mock.Arguments) {
			arg := args.Get(0).(*primitive.M)
			*arg = make(map[string]interface{})
			(*arg)["uuid"] = testUUID
		}).Once()
	cursor.On("Next", ctx).Return(false)
	cursor.On("Err").Return(nil)
	cursor.On("Close", ctx).Return(nil)
	inquirer := NewInquirer(finder, log)

	docCh, count, err := inquirer.Inquire(ctx, nil)
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
			assert.Equal(t, content.DefaultDate, doc.Date)

		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
	finder.AssertExpectations(t)
	cursor.AssertExpectations(t)
}

func TestInquirer_InquireWithoutValidContent(t *testing.T) {
	finder := new(mockFinder)
	cursor := new(mockCursor)

	testUUID := "uuid1"
	candidates := []string{testUUID}
	log := logger.NewUPPLogger("test", "PANIC")
	ctx := context.Background()

	finder.On("findContent", ctx, candidates).Return(cursor, 1, nil)
	cursor.On("Next", ctx).Return(true).Once()
	cursor.On("Decode", mock.AnythingOfType("*primitive.M")).Return(nil).
		Run(func(args mock.Arguments) {
			arg := args.Get(0).(*primitive.M)
			*arg = make(map[string]interface{})
		}).Once()
	cursor.On("Next", ctx).Return(false)
	cursor.On("Err").Return(nil)
	cursor.On("Close", ctx).Return(nil)
	inquirer := NewInquirer(finder, log)

	docCh, count, err := inquirer.Inquire(ctx, candidates)
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
	finder.AssertExpectations(t)
	cursor.AssertExpectations(t)
}

func TestInquirer_InquireErrorFindingUUIDs(t *testing.T) {
	finder := new(mockFinder)
	cursor := new(mockCursor)

	testUUID := "uuid1"
	candidates := []string{testUUID}
	log := logger.NewUPPLogger("test", "PANIC")
	ctx := context.Background()

	finder.On("findContent", ctx, candidates).Return(cursor, 0, fmt.Errorf("mongo err"))

	inquirer := NewInquirer(finder, log)

	docCh, count, err := inquirer.Inquire(ctx, candidates)
	assert.Error(t, err)
	assert.EqualError(t, err, "mongo err")
	assert.Equal(t, 0, count)
	assert.Nil(t, docCh)

	finder.AssertExpectations(t)
	cursor.AssertExpectations(t)
}

func TestInquirer_InquireErrorOnDecoding(t *testing.T) {
	finder := new(mockFinder)
	cursor := new(mockCursor)

	testUUID := "uuid1"
	candidates := []string{testUUID}
	log := logger.NewUPPLogger("test", "PANIC")
	ctx := context.Background()

	finder.On("findContent", ctx, candidates).Return(cursor, 1, nil)
	cursor.On("Next", ctx).Return(true).Once()
	cursor.On("Decode", mock.AnythingOfType("*primitive.M")).Return(fmt.Errorf("decode error"))
	cursor.On("Next", ctx).Return(false)
	cursor.On("Err").Return(nil)
	cursor.On("Close", ctx).Return(nil)
	inquirer := NewInquirer(finder, log)

	docCh, count, err := inquirer.Inquire(ctx, candidates)
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
	finder.AssertExpectations(t)
	cursor.AssertExpectations(t)
}
