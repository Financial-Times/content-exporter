package content

import (
	"fmt"
	"strings"
)

const DefaultDate = "0000-00-00"

type Stub struct {
	UUID, Date, ContentType string
	CanBeDistributed        string
	Publication             []string
	EditorialDesk           string
}

type Exporter struct {
	fetcher fetcher
	updater updater
}

func NewExporter(fetcher fetcher, updater updater) *Exporter {
	return &Exporter{
		fetcher: fetcher,
		updater: updater,
	}
}

func (e *Exporter) Export(tid string, doc *Stub) error {
	payload, err := e.fetcher.GetContent(doc.UUID, tid)
	if err != nil {
		return fmt.Errorf("getting content: %w", err)
	}

	err = e.updater.Upload(payload, tid, doc.UUID, doc.Date)
	if err != nil {
		return fmt.Errorf("uploading content: %w", err)
	}
	return nil
}

func (e *Exporter) Delete(uuid, tid string) error {
	return e.updater.Delete(uuid, tid)
}

func GetDateOrDefault(payload map[string]interface{}) string {
	if firstPublishedDate, ok := payload["firstPublishedDate"].(string); ok {
		if date := strings.Split(firstPublishedDate, "T")[0]; date != "" {
			return date
		}
	}

	if publishedDate, ok := payload["publishedDate"].(string); ok {
		if date := strings.Split(publishedDate, "T")[0]; date != "" {
			return date
		}
	}

	return DefaultDate
}
