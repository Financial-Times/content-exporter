package content

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type Client interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

const enrichedContentPath = "/enrichedcontent/"

type Fetcher interface {
	GetContent(uuid, tid string) ([]byte, error)
}

type EnrichedContentFetcher struct {
	Client                   Client
	EnrichedContentBaseURL   string
	EnrichedContentHealthURL string
	XPolicyHeaderValues      string
	Authorization            string
}

func (e *EnrichedContentFetcher) GetContent(uuid, tid string) ([]byte, error) {
	req, err := http.NewRequest("GET", e.EnrichedContentBaseURL+enrichedContentPath+uuid, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Request-Id", tid)

	if e.XPolicyHeaderValues != "" {
		req.Header.Add("X-Policy", e.XPolicyHeaderValues)
	}
	if e.Authorization != "" {
		req.Header.Add("Authorization", e.Authorization)
	}

	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("access to content is forbidden. Skipping")
		}
		return nil, fmt.Errorf("EnrichedContent returned HTTP %v", resp.StatusCode)
	}

	return ioutil.ReadAll(resp.Body)
}

func (e *EnrichedContentFetcher) CheckHealth(client Client) (string, error) {
	req, err := http.NewRequest("GET", e.EnrichedContentHealthURL, nil)
	if err != nil {
		return "Error in building request to check if the enrichedContent fetcher is good to go", err
	}

	if e.Authorization != "" {
		req.Header.Add("Authorization", e.Authorization)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "Error in getting request to check if the enrichedContent fetcher is good to go", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "EnrichedContent fetcher is not good to go.", fmt.Errorf("GTG HTTP status code is %v", resp.StatusCode)
	}
	return "EnrichedContent fetcher is good to go.", nil
}
