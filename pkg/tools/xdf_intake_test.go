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

type xdfRoundTripFunc func(*http.Request) (*http.Response, error)

func (f xdfRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestXDFIntakeToolRejectsNonLoopbackEndpoint(t *testing.T) {
	if _, err := NewXDFIntakeTool("https://example.com", "", "", 1024); err == nil {
		t.Fatal("non-loopback endpoint should be rejected")
	}
}

func TestXDFIntakeToolBindsIdentityToWhatsAppContext(t *testing.T) {
	var received map[string]any
	transport := xdfRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer controlled-token" {
			t.Fatalf("missing bearer token")
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("controlled-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool, err := NewXDFIntakeTool(defaultXDFEngineURL, tokenFile, t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	tool.client.Transport = transport

	ctx := WithToolContext(context.Background(), "whatsapp_native", "client@lid")
	result := tool.Execute(ctx, map[string]any{"action": "process", "text": "consulta"})
	if result.IsError {
		t.Fatalf("Execute() error=%s", result.ForLLM)
	}
	if received["channel"] != "whatsapp_native" || received["chat_id"] != "client@lid" ||
		received["sender_id"] != "client@lid" {
		t.Fatalf("identity was not bound to context: %#v", received)
	}

	blocked := tool.Execute(
		WithToolContext(context.Background(), "telegram", "admin"),
		map[string]any{"action": "process", "text": "consulta"},
	)
	if !blocked.IsError {
		t.Fatal("non-WhatsApp execution should be blocked")
	}
}

func TestXDFIntakeToolMediaPathCannotEscapeRoot(t *testing.T) {
	root := t.TempDir()
	tool, err := NewXDFIntakeTool(defaultXDFEngineURL, filepath.Join(root, "token"), root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret.pdf")
	if err := os.WriteFile(outside, []byte("%PDF-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tool.readMediaPath(outside, "secret.pdf", 1024); err == nil {
		t.Fatal("media path outside root should be rejected")
	}
}

func TestXDFIntakeToolRejectsDocumentLabelWithoutFile(t *testing.T) {
	root := t.TempDir()
	tool, err := NewXDFIntakeTool(defaultXDFEngineURL, filepath.Join(root, "token"), root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	result := tool.Execute(WithToolContext(context.Background(), "whatsapp_native", "client@lid"), map[string]any{
		"action": "process",
		"text":   "segue RG",
		"media":  []any{"RG"},
	})
	if !result.IsError || !strings.Contains(result.ForLLM, "actual inbound file") {
		t.Fatalf("label-only media was accepted: %+v", result)
	}
}

func TestXDFIntakeToolAddsSignedContractContextForExplicitCard(t *testing.T) {
	var received map[string]any
	transport := xdfRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})

	root := t.TempDir()
	tokenFile := filepath.Join(root, "token")
	if err := os.WriteFile(tokenFile, []byte("controlled-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	contractPath := filepath.Join(root, "card_14_contrato_honorarios_assinado_rfreire_assinado.pdf")
	if err := os.WriteFile(contractPath, []byte("%PDF-signed-contract"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool, err := NewXDFIntakeTool(defaultXDFEngineURL, tokenFile, root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	tool.client.Transport = transport

	result := tool.Execute(WithToolContext(context.Background(), "whatsapp_native", "client@lid"), map[string]any{
		"action":  "process",
		"card_id": float64(14),
		"media":   []any{contractPath},
	})
	if result.IsError {
		t.Fatalf("Execute() error=%s", result.ForLLM)
	}
	if received["card_id"] != float64(14) {
		t.Fatalf("card_id was not preserved: %#v", received["card_id"])
	}
	text, _ := received["text"].(string)
	if !strings.Contains(text, "contrato de honorarios assinado via Gov.BR") || !strings.Contains(text, "card 14") {
		t.Fatalf("signed contract context was not added: %q", text)
	}
	uploads, _ := received["media_uploads"].([]any)
	if len(uploads) != 1 {
		t.Fatalf("media upload missing: %#v", received["media_uploads"])
	}
	upload, _ := uploads[0].(map[string]any)
	if upload["filename"] != filepath.Base(contractPath) {
		t.Fatalf("filename was not preserved: %#v", upload["filename"])
	}
}

func TestXDFIntakeToolDoesNotRelabelGenericCardMediaAsContract(t *testing.T) {
	root := t.TempDir()
	if xdfSignedContractHint([]string{filepath.Join(root, "laudo_medico.pdf")}, nil, nil) {
		t.Fatal("generic card media must not be treated as a signed contract")
	}
}
