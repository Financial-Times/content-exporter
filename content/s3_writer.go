package content

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
)

const s3WriterPath = "/content/"

var ErrNotFound = errors.New("Content RW S3 returned HTTP 404 with message")

type Updater interface {
	Upload(content []byte, tid, uuid, date string) error
	Delete(uuid, tid string) error
}

type S3Updater struct {
	Client            Client
	S3WriterBaseURL   string
	S3WriterHealthURL string
}

func (u *S3Updater) Delete(uuid, tid string) error {
	req, err := http.NewRequest("DELETE", u.S3WriterBaseURL+s3WriterPath+uuid, nil)
	if err != nil {
		return err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if resp.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("content RW S3 returned HTTP %v with message: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (u *S3Updater) Upload(content []byte, tid, uuid, date string) error {
	buf := new(bytes.Buffer)
	_, err := buf.Write(content)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", u.S3WriterBaseURL+s3WriterPath+uuid+"?date="+date, buf)
	if err != nil {
		return err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Request-Id", tid)

	resp, err := u.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Content RW S3 returned HTTP %v", resp.StatusCode)
	}

	return nil
}

func (u *S3Updater) CheckHealth(client Client) (string, error) {
	req, err := http.NewRequest("GET", u.S3WriterHealthURL, nil)
	if err != nil {
		return "Error in building request to check if the S3 Writer is good to go", err
	}

	resp, err := client.Do(req)
	if err != nil {
		return "Error in getting request to check if S3 Writer is good to go.", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "S3 Writer is not good to go.", fmt.Errorf("GTG HTTP status code is %v", resp.StatusCode)
	}
	return "S3 Writer is good to go.", nil
}
