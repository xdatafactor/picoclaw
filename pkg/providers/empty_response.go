package providers

import (
	"fmt"
	"strings"
)

// ValidateLLMResponse rejects successful transports without a usable answer.
// Reasoning is internal and cannot substitute for either content or tool calls.
func ValidateLLMResponse(response *LLMResponse) error {
	if response != nil &&
		(strings.TrimSpace(response.Content) != "" || len(response.ToolCalls) > 0 || response.FinishReason == "content_filter") {
		return nil
	}
	finish := "missing"
	if response != nil {
		finish = response.FinishReason
	}
	return &FailoverError{
		Reason:  FailoverEmptyResponse,
		Wrapped: fmt.Errorf("provider returned no answer or tool calls (finish_reason=%s)", finish),
	}
}
