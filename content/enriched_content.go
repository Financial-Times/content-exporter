package content

import (
	"fmt"
	"io"
	"net/http"
)

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type fetcher interface {
	GetContent(uuid, tid string) ([]byte, error)
}

type EnrichedContentFetcher struct {
	client                   httpClient
	enrichedContentAPIURL    string
	enrichedContentHealthURL string
	xPolicyHeaderValues      string
	authorization            string
}

func NewEnrichedContentFetcher(client httpClient, enrichedContentAPIURL, enrichedContentHealthURL, xPolicyHeaderValues, authorization string) *EnrichedContentFetcher {
	return &EnrichedContentFetcher{
		client:                   client,
		enrichedContentAPIURL:    enrichedContentAPIURL,
		enrichedContentHealthURL: enrichedContentHealthURL,
		xPolicyHeaderValues:      xPolicyHeaderValues,
		authorization:            authorization,
	}
}

func (e *EnrichedContentFetcher) GetContent(uuid, tid string) ([]byte, error) {
	req, err := http.NewRequest("GET", e.enrichedContentAPIURL+uuid, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "UPP Content Exporter")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Request-Id", tid)

	if e.xPolicyHeaderValues != "" {
		req.Header.Add("X-Policy", e.xPolicyHeaderValues)
	}
	if e.authorization != "" {
		req.Header.Add("Authorization", e.authorization)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching enriched content failed with unexpected status code: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (e *EnrichedContentFetcher) CheckHealth(client httpClient) (string, error) {
	req, err := http.NewRequest("GET", e.enrichedContentHealthURL, nil)
	if err != nil {
		return "", err
	}

	if e.authorization != "" {
		req.Header.Add("Authorization", e.authorization)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GTG failed with unexpected status code: %d", resp.StatusCode)
	}
	return "EnrichedContent fetcher is good to go.", nil
}
