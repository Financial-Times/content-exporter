package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/gorilla/mux"
)

type exporterMock struct {
	getJobF         func(jobID string) (export.Job, error)
	getRunningJobsF func() []export.Job
	exportF         func(tid string, doc *content.Stub) error
	getWorkerCountF func() int
}

func (e *exporterMock) GetJob(jobID string) (export.Job, error) {
	if e.getJobF != nil {
		return e.getJobF(jobID)
	}
	panic("exporterMock.GetJob is not implemented")
}
func (e *exporterMock) GetRunningJobs() []export.Job {
	if e.getRunningJobsF != nil {
		return e.getRunningJobsF()
	}
	panic("exporterMock.GetRunningJobs is not implemented")
}
func (e *exporterMock) AddJob(_ *export.Job) {
	// Function doesn't return anything so a facade would do
}
func (e *exporterMock) Export(tid string, doc *content.Stub) error {
	if e.exportF != nil {
		return e.exportF(tid, doc)
	}
	panic("exporterMock.Export is not implemented")
}
func (e *exporterMock) GetWorkerCount() int {
	if e.getWorkerCountF != nil {
		return e.getWorkerCountF()
	}
	panic("exporterMock.GetWorkerCount is not implemented")
}

type inquirerMock struct {
	inquireF func(ctx context.Context, candidates []string) (chan *content.Stub, int, error)
}

func (i *inquirerMock) Inquire(ctx context.Context, candidates []string) (chan *content.Stub, int, error) {
	if i.inquireF != nil {
		return i.inquireF(ctx, candidates)
	}
	panic("inquirerMock.Inquire is not implemented")
}

func TestRequestHandler_Export(t *testing.T) {
	tests := []struct {
		name             string
		exporter         *exporterMock
		inquirer         *inquirerMock
		locker           *export.Locker
		incExportEnabled bool
		throttle         int
		getHTTPRequest   func() *http.Request
		expectedBody     string
		expectedStatus   int
	}{
		{
			name: "test that not passing a full export flag or ids results in an error",
			exporter: &exporterMock{
				getRunningJobsF: func() []export.Job {
					return []export.Job{}
				},
			},
			inquirer:         &inquirerMock{},
			locker:           export.NewLocker(),
			incExportEnabled: false,
			throttle:         10,
			getHTTPRequest: func() *http.Request {
				body := strings.NewReader(``)
				req, _ := http.NewRequest("POST", "/export", body)
				return req
			},
			expectedBody:   "{\"error\":\"Pass a list of ids or trigger a full export flag\"}\n",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "test that not passing ids and a false full export flag results in an error",
			exporter: &exporterMock{
				getRunningJobsF: func() []export.Job {
					return []export.Job{}
				},
			},
			inquirer:         &inquirerMock{},
			locker:           export.NewLocker(),
			incExportEnabled: false,
			throttle:         10,
			getHTTPRequest: func() *http.Request {
				body := strings.NewReader(``)
				req, _ := http.NewRequest("POST", "/export?fullExport=false", body)
				return req
			},
			expectedBody:   "{\"error\":\"Pass a list of ids or trigger a full export flag\"}\n",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "test that passing both ids and a full export flag results in an error",
			exporter: &exporterMock{
				getRunningJobsF: func() []export.Job {
					return []export.Job{}
				},
			},
			inquirer:         &inquirerMock{},
			locker:           export.NewLocker(),
			incExportEnabled: false,
			throttle:         10,
			getHTTPRequest: func() *http.Request {
				body := strings.NewReader(`{"ids":"some-valid-uuids"}`)
				req, _ := http.NewRequest("POST", "/export?fullExport=true", body)
				return req
			},
			expectedBody:   "{\"error\":\"Pass either a list of ids or the full export flag, not both\"}\n",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "test that passing a full export flag triggers an export",
			exporter: &exporterMock{
				getRunningJobsF: func() []export.Job {
					return []export.Job{}
				},
				getWorkerCountF: func() int {
					return 1
				},
				exportF: func(tid string, doc *content.Stub) error {
					return nil
				},
			},
			inquirer: &inquirerMock{
				inquireF: func(ctx context.Context, candidates []string) (chan *content.Stub, int, error) {
					c := make(chan *content.Stub)
					return c, 0, nil
				},
			},
			locker:           export.NewLocker(),
			incExportEnabled: false,
			throttle:         0,
			getHTTPRequest: func() *http.Request {
				body := strings.NewReader(``)
				req, _ := http.NewRequest("POST", "/export?fullExport=true", body)
				return req
			},
			expectedStatus: http.StatusAccepted,
		},
		{
			name: "test that passing a list of ids triggers a targeted export",
			exporter: &exporterMock{
				getRunningJobsF: func() []export.Job {
					return []export.Job{}
				},
				getWorkerCountF: func() int {
					return 1
				},
			},
			inquirer: &inquirerMock{
				inquireF: func(ctx context.Context, candidates []string) (chan *content.Stub, int, error) {
					c := make(chan *content.Stub)
					return c, 0, nil
				},
			},
			locker:           export.NewLocker(),
			incExportEnabled: false,
			throttle:         10,
			getHTTPRequest: func() *http.Request {
				body := strings.NewReader(`{"ids":"some-valid-uuids"}`)
				req, _ := http.NewRequest("POST", "/export", body)
				return req
			},
			expectedStatus: http.StatusAccepted,
		},
	}

	log := logger.NewUPPLogger("test", "PANIC")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := NewRequestHandler(test.exporter, test.inquirer, test.locker, test.incExportEnabled, test.throttle, log)
			rr := httptest.NewRecorder()
			r := mux.NewRouter()
			req := test.getHTTPRequest()

			r.HandleFunc("/export", h.Export).Methods("POST")
			r.ServeHTTP(rr, req)

			if status := rr.Code; status != test.expectedStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, test.expectedStatus)
			}

			if test.expectedBody != "" {
				if rr.Body.String() != test.expectedBody {
					t.Errorf("handler returned unexpected body: got %v want %v",
						rr.Body.String(), test.expectedBody)
				}
			}
		})
	}
}
