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

func TestXDFPJeBindsConversationAndPreservesSafeErrors(t *testing.T) {
	for _, status := range []int{200, 403, 404, 429, 502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("test-token"), 0600); err != nil {
				t.Fatal(err)
			}
			tool, err := NewXDFIntakeTool("", tokenFile, "", 0)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			tool.client.Transport = xdfRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/api/v1/pje/tjce/consulta" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Fatalf("wrong request: %s %s", r.Method, r.URL.Path)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if len(body) != 4 || body["channel"] != "whatsapp_native" || body["chat_id"] != "current@lid" || body["grau"] != "G1" || body["numero_cnj"] != "00000013220268060001" {
					t.Fatalf("unscoped payload: %#v", body)
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{"status":"example","response":"Informação pública pode estar defasada.","unexpected":"private"}`)), Header: make(http.Header)}, nil
			})
			args := map[string]any{"action": "pje_consulta", "numero_cnj": "00000013220268060001", "channel": "telegram", "chat_id": "victim", "url": "https://evil.invalid", "card_id": 900, "atendimento_id": "other"}
			result := tool.Execute(WithToolContext(context.Background(), "whatsapp_native", "current@lid"), args)
			if result.IsError != (status != 200) || !strings.Contains(result.ForLLM, "defasada") {
				t.Fatalf("safe information lost: %+v", result)
			}
			if status != 200 && strings.Contains(result.ForLLM, "private") {
				t.Fatal("error leaked extra data")
			}
			blocked := tool.Execute(WithToolContext(context.Background(), "telegram", "operator"), args)
			if !blocked.IsError || calls != 1 {
				t.Fatal("outside conversation was allowed")
			}
		})
	}
}
