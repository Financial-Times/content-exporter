package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/content-exporter/export"
	"github.com/Financial-Times/go-logger/v2"
	transactionidutils "github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/mux"
)

const targetedExportTimeout = 30 * time.Second

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

func (h *RequestHandler) Export(w http.ResponseWriter, r *http.Request) {
	tid := transactionidutils.GetTransactionIDFromRequest(r)

	jobs := h.fullExporter.GetRunningJobs()
	if len(jobs) > 0 {
		h.sendErrorResponse(w, http.StatusBadRequest, "There are already running export jobs. Please wait them to finish")
		return
	}

	if h.isIncExportEnabled {
		select {
		case h.locker.Locked <- true:
			h.log.Info("Lock initiated")
		case <-time.After(time.Second * 3):
			msg := "Lock initiation timed out"
			h.log.Infof(msg)
			h.sendErrorResponse(w, http.StatusServiceUnavailable, msg)
			return
		}

		select {
		case <-h.locker.Acked:
			h.log.Info("Locker acquired")
		case <-time.After(time.Second * 20):
			msg := "Stopping kafka consumption timed out"
			h.log.Infof(msg)
			h.sendErrorResponse(w, http.StatusServiceUnavailable, msg)
			return
		}
	}

	isFullExport := r.URL.Query().Get("fullExport") == "true"

	candidates, err := getCandidateUUIDs(r)
	if err != nil {
		if !isFullExport {
			h.log.WithError(err).Warn("Can't trigger a non-full export without ids")
			h.sendErrorResponse(w, http.StatusBadRequest, "Pass a list of ids or trigger a full export flag")
			return
		}
	} else if isFullExport {
		h.log.Warn("Can't trigger a full export with ids")
		h.sendErrorResponse(w, http.StatusBadRequest, "Pass either a list of ids or the full export flag, not both")
		return
	}

	job := export.NewJob(h.fullExporter.GetWorkerCount(), h.contentRetrievalThrottle, isFullExport, h.log)
	h.fullExporter.AddJob(job)
	response := map[string]string{
		"ID":     job.ID,
		"Status": string(job.Status),
	}

	go h.startExport(job, isFullExport, candidates, tid)

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		msg := fmt.Sprintf("Failed to parse response for new job with ID: %s", job.ID)
		h.log.WithError(err).Warn(msg)
		h.sendErrorResponse(w, http.StatusInternalServerError, msg)
	}
}

func (h *RequestHandler) startExport(job *export.Job, isFullExport bool, candidates []string, tid string) {
	if h.isIncExportEnabled {
		defer func() {
			h.log.Info("Locker released")
			h.locker.Locked <- false
		}()
	}
	h.log.Info("Calling mongo")

	ctx := context.Background()
	if !isFullExport {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, targetedExportTimeout)
		defer cancel()
	}

	docs, count, err := h.inquirer.Inquire(ctx, candidates)
	if err != nil {
		msg := "Failed to read content from mongo"
		h.log.WithError(err).Warn(msg)
		job.ErrorMessage = msg
		job.Status = export.FINISHED
		return
	}
	h.log.Infof("Nr of UUIDs found: %v", count)
	job.Count = count

	job.RunExport(tid, docs, h.fullExporter.Export)
}

func (h *RequestHandler) sendErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	response := map[string]string{
		"error": message,
	}

	resp, err := json.Marshal(response)
	if err != nil {
		h.log.WithError(err).Error("Failed to stringify response")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, err = w.Write(resp)
	if err != nil {
		h.log.WithError(err).Error("Failed to write response")
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func getCandidateUUIDs(request *http.Request) ([]string, error) {
	var result map[string]interface{}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, fmt.Errorf("reading request body: %w", err)
	}

	if err = json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshaling request body: %w", err)
	}

	ids, ok := result["ids"]
	if !ok {
		return nil, fmt.Errorf("'ids' field in request body not found")
	}
	idsString, ok := ids.(string)
	if !ok {
		return nil, fmt.Errorf("'ids' field is not a string")
	}

	return strings.Split(idsString, ","), nil
}

func (h *RequestHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobID"]

	job, err := h.fullExporter.GetJob(jobID)
	if err != nil {
		msg := "Failed to retrieve job"
		h.log.
			WithField("jobID", job.ID).
			WithError(err).
			Warn(msg)

		if errors.Is(err, export.ErrJobNotFound) {
			h.sendErrorResponse(w, http.StatusNotFound, "Job not found")
		} else {
			h.sendErrorResponse(w, http.StatusInternalServerError, msg)
		}
		return
	}

	w.Header().Add("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(job)
	if err != nil {
		h.log.
			WithField("jobID", job.ID).
			WithError(err).
			Warn("Failed to marshal job")

		h.sendErrorResponse(w, http.StatusInternalServerError, "Failed to parse job response")
	}
}

func (h *RequestHandler) GetRunningJobs(w http.ResponseWriter, r *http.Request) {
	jobs := h.fullExporter.GetRunningJobs()

	w.Header().Add("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(jobs)
	if err != nil {
		h.log.WithError(err).Warn("Failed to marshal jobs")

		h.sendErrorResponse(w, http.StatusInternalServerError, "Failed to parse jobs response")
	}
}
