package tools

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestXDFCalendarGuestsRemainSeparateFromPrimary(t *testing.T) {
	root := t.TempDir()
	token := filepath.Join(root, "token")
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	tool, err := NewXDFIntakeTool(defaultXDFEngineURL, token, root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	var received map[string]any
	tool.client.Transport = xdfRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ok":true}`)), Header: make(http.Header)}, nil
	})
	result := tool.Execute(WithToolContext(context.Background(), "whatsapp_native", "client@lid"), map[string]any{"action": "calendar_create", "attendee_email": "client@example.com", "guest_emails": []any{"guest@example.com"}, "selection_text": "18/09/2026 as 14h"})
	if result.IsError || received["attendee_email"] != "client@example.com" {
		t.Fatalf("result=%+v payload=%v", result, received)
	}
	guests, ok := received["guest_emails"].([]any)
	if !ok || len(guests) != 1 || guests[0] != "guest@example.com" {
		t.Fatalf("missing guests: %v", received)
	}
}
