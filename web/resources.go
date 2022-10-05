package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
)

type exporter interface {
	GetJob(jobID string) (export.Job, error)
	GetRunningJobs() []export.Job
	AddJob(job *export.Job)
	Export(tid string, doc *content.Stub) error
	GetWorkerCount() int
}

type inquirer interface {
	Inquire(ctx context.Context, candidates []string) (chan *content.Stub, int, error)
}

type RequestHandler struct {
	fullExporter             exporter
	inquirer                 inquirer
	contentRetrievalThrottle int
	locker                   *export.Locker
	isIncExportEnabled       bool
	log                      *logger.UPPLogger
}

func NewRequestHandler(fullExporter exporter, inquirer inquirer, locker *export.Locker, isIncExportEnabled bool, contentRetrievalThrottle int, log *logger.UPPLogger) *RequestHandler {
	return &RequestHandler{
		fullExporter:             fullExporter,
		inquirer:                 inquirer,
		locker:                   locker,
		isIncExportEnabled:       isIncExportEnabled,
		contentRetrievalThrottle: contentRetrievalThrottle,
		log:                      log,
	}
}

func (handler *RequestHandler) Export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	tid := transactionidutils.GetTransactionIDFromRequest(request)

	jobs := handler.fullExporter.GetRunningJobs()
	if len(jobs) > 0 {
		http.Error(writer, "There are already running export jobs. Please wait them to finish", http.StatusBadRequest)
		return
	}

	if handler.isIncExportEnabled {
		select {
		case handler.locker.Locked <- true:
			handler.log.Info("Lock initiated")
		case <-time.After(time.Second * 3):
			msg := "Lock initiation timed out"
			handler.log.Infof(msg)
			http.Error(writer, msg, http.StatusServiceUnavailable)
			return
		}

		select {
		case <-handler.locker.Acked:
			handler.log.Info("Locker acquired")
		case <-time.After(time.Second * 20):
			msg := "Stopping kafka consumption timed out"
			handler.log.Infof(msg)
			http.Error(writer, msg, http.StatusServiceUnavailable)
			return
		}
	}
	candidates := getCandidateUUIDs(request, handler.log)
	isFullExport := request.URL.Query().Get("fullExport") == "true"

	if len(candidates) == 0 && !isFullExport {
		handler.log.Warn("Can't trigger a non-full export without ids")
		sendFailedExportResponse(writer, "Pass a list of ids or trigger a full export flag", handler.log)
		return
	}

	if len(candidates) > 0 && isFullExport {
		handler.log.Warn("Can't trigger a full export with ids")
		sendFailedExportResponse(writer, "Pass either a list of ids or the full export flag, not both", handler.log)
		return
	}

	jobID := uuid.New()
	job := &export.Job{
		ID:                       jobID,
		NrWorker:                 handler.fullExporter.GetWorkerCount(),
		Status:                   export.STARTING,
		ContentRetrievalThrottle: handler.contentRetrievalThrottle,
		FullExport:               isFullExport,
		Log:                      handler.log,
	}
	handler.fullExporter.AddJob(job)
	response := map[string]string{
		"ID":     job.ID,
		"Status": string(job.Status),
	}

	go func() {
		if handler.isIncExportEnabled {
			defer func() {
				handler.log.Info("Locker released")
				handler.locker.Locked <- false
			}()
		}
		handler.log.Infoln("Calling mongo")
		docs, count, err := handler.inquirer.Inquire(context.Background(), candidates)
		if err != nil {
			msg := fmt.Sprintf(`Failed to read IDs from mongo for %v!`, "content")
			handler.log.WithError(err).Info(msg)
			job.ErrorMessage = msg
			job.Status = export.FINISHED
			return
		}
		handler.log.Infof("Nr of UUIDs found: %v", count)
		job.DocIds = docs
		job.Count = count

		job.RunFullExport(tid, handler.fullExporter.Export)
	}()

	writer.WriteHeader(http.StatusAccepted)
	writer.Header().Add("Content-Type", "application/json")

	err := json.NewEncoder(writer).Encode(response)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer`, job.ID)
		handler.log.WithError(err).Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func sendFailedExportResponse(writer http.ResponseWriter, msg string, log *logger.UPPLogger) {
	response := map[string]string{
		"error": msg,
	}

	writer.WriteHeader(http.StatusBadRequest)
	writer.Header().Add("Content-Type", "application/json")
	err := json.NewEncoder(writer).Encode(response)
	if err != nil {
		log.WithError(err).Warn("Could not stringify failed export response")
	}
}

func getCandidateUUIDs(request *http.Request, log *logger.UPPLogger) (candidates []string) {
	var result map[string]interface{}
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		log.WithError(err).Debug("No valid POST body found, thus no candidate ids to export")
		return
	}

	if err = json.Unmarshal(body, &result); err != nil {
		log.WithError(err).Debug("No valid json body found, thus no candidate ids to export")
		return
	}
	log.Infof("DEBUG Parsing request body: %v", result)
	ids, ok := result["ids"]
	if !ok {
		log.Infof("No ids field found in json body, thus no candidate ids to export.")
		return
	}
	idsString, ok := ids.(string)
	if ok {
		candidates = strings.Split(idsString, " ")
	} else {
		log.Infof("The ids field found in json body is not a string as expected.")
	}

	return
}

func (handler *RequestHandler) GetJob(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	vars := mux.Vars(request)
	jobID := vars["jobID"]

	writer.Header().Add("Content-Type", "application/json")

	job, err := handler.fullExporter.GetJob(jobID)
	if err != nil {
		msg := fmt.Sprintf(`{"message":"%v"}`, err)
		handler.log.WithError(err).Info("Failed to retrieve job")
		http.Error(writer, msg, http.StatusNotFound) // TODO: test
		return
	}

	err = json.NewEncoder(writer).Encode(job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer`, job.ID)
		handler.log.WithError(err).Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func (handler *RequestHandler) GetRunningJobs(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	writer.Header().Add("Content-Type", "application/json")

	jobs := handler.fullExporter.GetRunningJobs()

	err := json.NewEncoder(writer).Encode(jobs)
	if err != nil {
		handler.log.WithError(err).Warn("Failed to get running jobs")
		fmt.Fprintf(writer, "{\"Jobs\": \"%v\"}", jobs)
		return
	}
}
