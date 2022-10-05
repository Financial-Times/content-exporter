package export

import (
	"fmt"
	"sync"
	"time"

	"github.com/Financial-Times/content-exporter/content"
	"github.com/Financial-Times/go-logger/v2"
)

type Service struct {
	sync.RWMutex
	jobs                  map[string]*Job
	nrOfConcurrentWorkers int
	*content.Exporter
}

type State string

const (
	STARTING State = "Starting"
	RUNNING  State = "Running"
	FINISHED State = "Finished"
)

type Job struct {
	sync.RWMutex
	wg                       sync.WaitGroup
	Log                      *logger.UPPLogger
	NrWorker                 int                `json:"-"`
	DocIds                   chan *content.Stub `json:"-"`
	ID                       string             `json:"ID"`
	Count                    int                `json:"Count,omitempty"`
	Progress                 int                `json:"Progress,omitempty"`
	Failed                   []string           `json:"Failed,omitempty"`
	Status                   State              `json:"Status"`
	ErrorMessage             string             `json:"ErrorMessage,omitempty"`
	ContentRetrievalThrottle int                `json:"-"`
	FullExport               bool               `json:"-"`
}

func NewFullExporter(nrOfWorkers int, exporter *content.Exporter) *Service {
	return &Service{
		jobs:                  make(map[string]*Job),
		nrOfConcurrentWorkers: nrOfWorkers,
		Exporter:              exporter,
	}
}

func (fe *Service) GetRunningJobs() []Job {
	fe.RLock()
	defer fe.RUnlock()
	var jobs []Job
	for _, job := range fe.jobs {
		if job.Status == RUNNING {
			jobs = append(jobs, job.Copy())
		}
	}
	return jobs
}

func (fe *Service) GetJob(jobID string) (Job, error) {
	fe.RLock()
	defer fe.RUnlock()
	job, ok := fe.jobs[jobID]
	if !ok {
		return Job{}, fmt.Errorf("job %v not found", jobID)
	}
	return job.Copy(), nil
}

func (fe *Service) AddJob(job *Job) {
	if job != nil {
		fe.Lock()
		fe.jobs[job.ID] = job
		fe.Unlock()
	}
}

func (fe *Service) GetWorkerCount() int {
	return fe.nrOfConcurrentWorkers
}

func (fe *Service) IsFullExportRunning() bool {
	fe.RLock()
	defer fe.RUnlock()
	for _, job := range fe.jobs {
		if job.FullExport && job.Status != FINISHED {
			return true
		}
	}
	return false
}

func (job *Job) Copy() Job {
	job.Lock()
	defer job.Unlock()
	return Job{
		Progress: job.Progress,
		Status:   job.Status,
		ID:       job.ID,
		Count:    job.Count,
		Failed:   job.Failed,
	}
}

func (job *Job) RunFullExport(tid string, export func(string, *content.Stub) error) {
	job.Log.Infof("Job started: %v", job.ID)
	job.Status = RUNNING
	worker := make(chan struct{}, job.NrWorker)
	for {
		doc, ok := <-job.DocIds
		if !ok {
			job.wg.Wait()
			job.Status = FINISHED
			job.Log.Infof("Finished job %v with %v failure(s), progress: %v", job.ID, len(job.Failed), job.Progress)
			close(worker)
			return
		}

		worker <- struct{}{} // Will block until worker is available to span up new goroutines

		job.Progress++
		job.wg.Add(1)
		go func() {
			defer job.wg.Done()
			time.Sleep(time.Duration(job.ContentRetrievalThrottle) * time.Millisecond)
			if err := export(tid, doc); err != nil {
				job.Log.
					WithTransactionID(tid).
					WithUUID(doc.UUID).
					WithError(err).
					Error("Failed to process document")

				job.Lock()
				job.Failed = append(job.Failed, doc.UUID)
				job.Unlock()
			}
			<-worker
		}()
	}
}
