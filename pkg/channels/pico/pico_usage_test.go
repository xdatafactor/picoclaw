package pico

import "testing"

func TestSetTurnUsagePayload(t *testing.T) {
	t.Run("populates usage block when counts present", func(t *testing.T) {
		payload := map[string]any{PayloadKeyContent: "hi"}
		setTurnUsagePayload(payload, 1234, 567)

		raw, ok := payload[PayloadKeyUsage]
		if !ok {
			t.Fatalf("expected %q key in payload", PayloadKeyUsage)
		}
		usage, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("usage block is not a map: %T", raw)
		}
		if usage["input_tokens"] != 1234 {
			t.Errorf("input_tokens = %v, want 1234", usage["input_tokens"])
		}
		if usage["output_tokens"] != 567 {
			t.Errorf("output_tokens = %v, want 567", usage["output_tokens"])
		}
		if usage["total_tokens"] != 1801 {
			t.Errorf("total_tokens = %v, want 1801", usage["total_tokens"])
		}
	})

	t.Run("omits usage block when both counts zero", func(t *testing.T) {
		payload := map[string]any{PayloadKeyContent: "hi"}
		setTurnUsagePayload(payload, 0, 0)
		if _, ok := payload[PayloadKeyUsage]; ok {
			t.Errorf("expected no %q key when counts are zero", PayloadKeyUsage)
		}
	})
}
