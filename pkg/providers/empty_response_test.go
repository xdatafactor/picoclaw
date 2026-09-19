package providers

import (
	"context"
	"testing"
)

func TestEmptyAnswerFallsBackWithoutExposingReasoning(t *testing.T) {
	for _, first := range []*LLMResponse{nil, {}, {Content: " \n"}, {ReasoningContent: "internal-only", FinishReason: "length"}} {
		for _, image := range []bool{false, true} {
			chain := NewFallbackChain(NewCooldownTracker(), nil)
			calls := 0
			run := func(context.Context, FallbackCandidate) (*LLMResponse, error) {
				calls++
				if calls == 1 {
					return first, ValidateLLMResponse(first)
				}
				return &LLMResponse{Content: "Resposta recuperada"}, nil
			}
			candidates := []FallbackCandidate{makeCandidate("first", "first"), makeCandidate("second", "second")}
			var result *FallbackResult
			var err error
			if image {
				result, err = chain.ExecuteImageCandidate(context.Background(), candidates, run)
			} else {
				result, err = chain.ExecuteCandidate(context.Background(), candidates, run)
			}
			if err != nil || calls != 2 || result.Response.Content != "Resposta recuperada" {
				t.Fatalf("fallback failed: result=%+v calls=%d error=%v", result, calls, err)
			}
		}
	}
}

func TestToolOnlyAndFilteredResponsesDoNotTriggerFallback(t *testing.T) {
	for _, response := range []*LLMResponse{
		{ToolCalls: []ToolCall{{ID: "booking-call"}}},
		{FinishReason: "content_filter"},
		{Content: "Resposta normal"},
	} {
		if err := ValidateLLMResponse(response); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAllEmptyAnswersExhaustBoundedCandidates(t *testing.T) {
	chain := NewFallbackChain(NewCooldownTracker(), nil)
	calls := 0
	_, err := chain.Execute(context.Background(), []FallbackCandidate{makeCandidate("first", "first"), makeCandidate("second", "second")}, func(context.Context, string, string) (*LLMResponse, error) {
		calls++
		response := &LLMResponse{FinishReason: "length"}
		return response, ValidateLLMResponse(response)
	})
	if err == nil || calls != 2 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}
