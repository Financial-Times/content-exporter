package content

import (
	"fmt"
	"strings"
)

const DefaultDate = "0000-00-00"

type Stub struct {
	UUID, Date, ContentType string
	CanBeDistributed        *string
}

type Exporter struct {
	Fetcher Fetcher
	Updater updater
}

func NewExporter(fetcher Fetcher, updater updater) *Exporter {
	return &Exporter{
		Fetcher: fetcher,
		Updater: updater,
	}
}

func (e *Exporter) HandleContent(tid string, doc *Stub) error {
	payload, err := e.Fetcher.GetContent(doc.UUID, tid)
	if err != nil {
		return fmt.Errorf("error getting content for %v: %v", doc.UUID, err)
	}

	err = e.Updater.Upload(payload, tid, doc.UUID, doc.Date)
	if err != nil {
		return fmt.Errorf("error uploading content for %v: %v", doc.UUID, err)
	}
	return nil
}

func GetDateOrDefault(payload map[string]interface{}) (date string) {
	docFirstPublishedDate, _ := payload["firstPublishedDate"]
	d, ok := docFirstPublishedDate.(string)
	if ok {
		date = strings.Split(d, "T")[0]
	}
	if date != "" {
		return
	}
	docPublishedDate, _ := payload["publishedDate"]
	d, ok = docPublishedDate.(string)
	if ok {
		date = strings.Split(d, "T")[0]
	}
	if date != "" {
		return
	}

	return DefaultDate
}
