package policy

import (
	"errors"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/opa-client-go"
)

var ErrEvaluatePolicy = errors.New("error evaluating policy")

const (
	FilterSVContent = "content_msg_evaluator"
)

type ContentPolicyResult struct {
	Skip    bool     `json:"skip"`
	Reasons []string `json:"reasons"`
}

type OpenPolicyAgent struct {
	client *opa.OpenPolicyAgentClient
	log    *logger.UPPLogger
}

func NewOpenPolicyAgent(c *opa.OpenPolicyAgentClient, l *logger.UPPLogger) *OpenPolicyAgent {
	return &OpenPolicyAgent{
		client: c,
		log:    l,
	}
}

func (o *OpenPolicyAgent) EvaluateContentPolicy(query map[string]interface{}) (*ContentPolicyResult, error) {
	r := &ContentPolicyResult{}
	decisionID, err := o.client.DoQuery(query, FilterSVContent, r)
	if err != nil {
		return nil, errors.Join(ErrEvaluatePolicy, err)
	}

	log := o.log.WithField("result", r)
	if decisionID != "" {
		log.WithField("decisionID", decisionID)
	}
	log.Debug("Evaluated Special Content Policy")

	return r, nil
}
