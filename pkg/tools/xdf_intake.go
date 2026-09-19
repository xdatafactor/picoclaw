package tools

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/media"
)

const (
	defaultXDFEngineURL = "http://127.0.0.1:18080"
	defaultXDFTokenFile = "/home/xdf/.config/xdf/internal-api-token"
	maxXDFResponseBytes = 8 << 20
)

// XDFIntakeTool is a structured, channel-bound bridge to the XDF Go Engine.
// It deliberately exposes no URL, shell command, filesystem path, channel or
// chat selector to the model.
type XDFIntakeTool struct {
	baseURL       string
	tokenFile     string
	mediaRoot     string
	maxMediaBytes int64
	client        *http.Client
	mediaStore    media.MediaStore
}

func NewXDFIntakeTool(baseURL, tokenFile, mediaRoot string, maxMediaBytes int64) (*XDFIntakeTool, error) {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultXDFEngineURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("xdf intake endpoint must be a plain loopback HTTP URL")
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("xdf intake endpoint must use a loopback host")
	}
	if strings.TrimSpace(tokenFile) == "" {
		tokenFile = defaultXDFTokenFile
	}
	if strings.TrimSpace(mediaRoot) == "" {
		mediaRoot = media.TempDir()
	}
	if maxMediaBytes <= 0 {
		maxMediaBytes = 20 << 20
	}
	return &XDFIntakeTool{
		baseURL:       strings.TrimRight(parsed.String(), "/"),
		tokenFile:     tokenFile,
		mediaRoot:     mediaRoot,
		maxMediaBytes: maxMediaBytes,
		client: &http.Client{
			Timeout: 120 * time.Second,
			Transport: &http.Transport{
				Proxy: nil,
				DialContext: (&net.Dialer{
					Timeout: 5 * time.Second,
				}).DialContext,
			},
		},
	}, nil
}

func (t *XDFIntakeTool) SetMediaStore(store media.MediaStore) {
	t.mediaStore = store
}

func (t *XDFIntakeTool) Name() string { return "xdf_intake" }

func (t *XDFIntakeTool) Description() string {
	return "Process the current WhatsApp client's intake, documents, reset, slot selection, and meeting scheduling through the authenticated XDF Go Engine. The channel and client identity are always taken from the current message context."
}

func (t *XDFIntakeTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type": "string",
				"enum": []string{
					"process",
					"reset",
					"choose_slot",
					"calendar_slots",
					"calendar_create",
					"list_categories",
					"add_keyword",
				},
			},
			"text": map[string]any{"type": "string", "description": "Customer message or an accurate intake summary"},
			"media": map[string]any{
				"type": "array", "maxItems": 12,
				"items": map[string]any{"type": "string", "description": "An inbound media:// reference or a PicoClaw media path; labels without file bytes are rejected"},
			},
			"category":        map[string]any{"type": "string", "description": "Target case category slug or title (e.g. pensao_alimenticia, divorcio)"},
			"keyword":         map[string]any{"type": "string", "description": "New keyword or synonym to register for the case category"},
			"card_id":         map[string]any{"type": "integer", "minimum": 0},
			"chosen_slot":     map[string]any{"type": "string"},
			"start_date":      map[string]any{"type": "string"},
			"days":            map[string]any{"type": "integer", "minimum": 1, "maximum": 30},
			"slot_minutes":    map[string]any{"type": "integer", "minimum": 15, "maximum": 240},
			"limit":           map[string]any{"type": "integer", "minimum": 1, "maximum": 50},
			"tax_id":          map[string]any{"type": "string"},
			"client_email":    map[string]any{"type": "string"},
			"meeting_format":  map[string]any{"type": "string", "enum": []string{"virtual", "presencial"}},
			"selection_text":  map[string]any{"type": "string"},
			"slot_batch_id":   map[string]any{"type": "string", "description": "Use the ID returned with the offered slots; never invent one"},
			"refresh":         map[string]any{"type": "boolean", "description": "Only true when the customer explicitly requests different slots, abandoning the saved selection"},
			"option":          map[string]any{"type": "integer", "minimum": 1, "maximum": 50},
			"attendee_email":  map[string]any{"type": "string"},
			"guest_emails":    map[string]any{"type": "array", "maxItems": 10, "items": map[string]any{"type": "string"}, "description": "Additional meeting invitees explicitly requested by the customer. Never put them in client_email or attendee_email."},
			"expires_minutes": map[string]any{"type": "integer", "minimum": 5, "maximum": 180},
		},
		"required": []string{"action"},
	}
}

func (t *XDFIntakeTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	channel := strings.TrimSpace(ToolChannel(ctx))
	chatID := strings.TrimSpace(ToolChatID(ctx))
	action := strings.TrimSpace(xdfArgString(args, "action"))
	if action != "list_categories" && action != "add_keyword" {
		if channel != "whatsapp_native" || chatID == "" {
			return ErrorResult("xdf_intake is restricted to the current WhatsApp Native conversation")
		}
	}
	var path string
	var payload map[string]any

	switch action {
	case "process":
		text := xdfArgString(args, "text")
		if len([]byte(text)) > 65536 {
			return ErrorResult("text exceeds 65536 bytes")
		}
		mediaValues := xdfArgStrings(args, "media")
		cardID := xdfArgInt(args, "card_id")
		labels, uploads, err := t.prepareMedia(mediaValues)
		if err != nil {
			return ErrorResult(err.Error()).WithError(err)
		}
		// WhatsApp providers may replace the original attachment name with a
		// generic media name. Preserve the signed-contract intent in structured
		// text whenever the model supplied an explicit card and either the input
		// reference or resolved upload filename identifies a signed contract.
		// The Go Engine still performs the authoritative Gov.BR verification.
		if text == "" && cardID > 0 && xdfSignedContractHint(mediaValues, labels, uploads) {
			text = fmt.Sprintf("Cliente enviou contrato de honorarios assinado via Gov.BR para o card %d", cardID)
		}
		if strings.TrimSpace(text) == "" && len(labels) == 0 && len(uploads) == 0 {
			return ErrorResult("process requires text or media")
		}
		payload = map[string]any{
			"channel":       channel,
			"chat_id":       chatID,
			"sender_id":     chatID,
			"text":          text,
			"media":         labels,
			"media_uploads": uploads,
			"card_id":       cardID,
		}
		path = "/channels/whatsapp/message"
	case "reset":
		payload = map[string]any{"channel": channel, "chat_id": chatID, "reset_case": true}
		path = "/intake/start"
	case "choose_slot":
		chosen := strings.TrimSpace(xdfArgString(args, "chosen_slot"))
		if chosen == "" {
			return ErrorResult("choose_slot requires chosen_slot")
		}
		payload = map[string]any{"channel": channel, "chat_id": chatID, "chosen_slot": chosen, "card_id": xdfArgInt(args, "card_id")}
		path = "/intake/" + url.PathEscape(channel) + "/" + url.PathEscape(chatID) + "/chosen-slot"
	case "calendar_slots":
		payload = map[string]any{
			"channel": channel, "chat_id": chatID,
			"start_date": xdfArgString(args, "start_date"),
			"days":       xdfBoundedInt(args, "days", 1, 30, 14), "slot_minutes": xdfBoundedInt(args, "slot_minutes", 15, 240, 60),
			"limit": xdfBoundedInt(args, "limit", 1, 50, 50), "tax_id": xdfArgString(args, "tax_id"),
			"client_email": xdfArgString(args, "client_email"), "meeting_format": xdfMeetingFormat(args),
			"expires_minutes": xdfBoundedInt(args, "expires_minutes", 5, 180, 30),
		}
		payload["refresh"], _ = args["refresh"].(bool)
		path = "/calendar/slots"
	case "calendar_create":
		email := strings.TrimSpace(xdfArgString(args, "attendee_email"))
		if email != "" && !strings.Contains(email, "@") {
			return ErrorResult("attendee_email must be a valid email when supplied")
		}
		payload = map[string]any{
			"channel": channel, "chat_id": chatID, "summary": "Reuniao R. Freire",
			"description": "Reuniao solicitada no atendimento WhatsApp", "conference": true,
			"attendee_email": email, "selection_text": xdfArgString(args, "selection_text"),
			"guest_emails": xdfArgStrings(args, "guest_emails"),
			"chosen_slot":  xdfArgString(args, "chosen_slot"), "option": xdfBoundedInt(args, "option", 1, 50, 0),
			"tax_id": xdfArgString(args, "tax_id"), "meeting_format": xdfMeetingFormat(args),
			"slot_batch_id": xdfArgString(args, "slot_batch_id"),
		}
		path = "/calendar/create"
	case "list_categories":
		result, err := t.getJSON(ctx, "/intake/categories")
		if err != nil {
			return ErrorResult(err.Error()).WithError(err)
		}
		return NewToolResult(result)
	case "add_keyword":
		category := strings.TrimSpace(xdfArgString(args, "category"))
		keyword := strings.TrimSpace(xdfArgString(args, "keyword"))
		if category == "" || keyword == "" {
			return ErrorResult("add_keyword requires category and keyword")
		}
		payload = map[string]any{
			"category": category,
			"keyword":  keyword,
			"source":   "llm",
		}
		path = "/intake/categories/keywords"
	default:
		return ErrorResult("unknown xdf_intake action")
	}

	result, err := t.postJSON(ctx, path, payload)
	if err != nil {
		if result != "" {
			return ErrorResult(result).WithError(err)
		}
		return ErrorResult(err.Error()).WithError(err)
	}
	return NewToolResult(result)
}

func (t *XDFIntakeTool) getJSON(ctx context.Context, path string) (string, error) {
	tokenBytes, err := os.ReadFile(t.tokenFile)
	if err != nil || strings.TrimSpace(string(tokenBytes)) == "" {
		return "", fmt.Errorf("XDF internal API credential is unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.baseURL+path, nil)
	if err != nil {
		return "", fmt.Errorf("create XDF request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tokenBytes)))
	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("call XDF Go Engine: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxXDFResponseBytes+1))
	if err != nil {
		return "", fmt.Errorf("read XDF response: %w", err)
	}
	if len(data) > maxXDFResponseBytes {
		return "", fmt.Errorf("XDF response exceeds size limit")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var errResp struct {
			Error    string `json:"error"`
			Response string `json:"response"`
		}
		if json.Unmarshal(data, &errResp) == nil && strings.TrimSpace(errResp.Error) != "" {
			return "", fmt.Errorf("XDF Go Engine returned HTTP %d: %s", resp.StatusCode, errResp.Error)
		}
		return "", fmt.Errorf("XDF Go Engine returned HTTP %d", resp.StatusCode)
	}
	return string(data), nil
}

func (t *XDFIntakeTool) postJSON(ctx context.Context, path string, payload map[string]any) (string, error) {
	tokenBytes, err := os.ReadFile(t.tokenFile)
	if err != nil || strings.TrimSpace(string(tokenBytes)) == "" {
		return "", fmt.Errorf("XDF internal API credential is unavailable")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode XDF request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create XDF request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tokenBytes)))
	resp, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("call XDF Go Engine: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxXDFResponseBytes+1))
	if err != nil {
		return "", fmt.Errorf("read XDF response: %w", err)
	}
	if len(data) > maxXDFResponseBytes {
		return "", fmt.Errorf("XDF response exceeds size limit")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Calendar failures contain actionable, conversation-bound information.
		// Preserve only these safe fields so ambiguity and a missing email do
		// not collapse into an opaque HTTP error and restart slot selection.
		if path == "/calendar/create" {
			var body map[string]json.RawMessage
			if json.Unmarshal(data, &body) == nil && body["status"] != nil {
				safe := make(map[string]json.RawMessage)
				for _, key := range []string{"ok", "status", "message", "candidates", "error", "slot_batch_id", "chosen_slot"} {
					if value, exists := body[key]; exists {
						safe[key] = value
					}
				}
				encoded, _ := json.Marshal(safe)
				return string(encoded), fmt.Errorf("XDF Go Engine returned HTTP %d", resp.StatusCode)
			}
		}
		var errResp struct {
			Error    string `json:"error"`
			Response string `json:"response"`
		}
		if json.Unmarshal(data, &errResp) == nil && strings.TrimSpace(errResp.Error) != "" {
			return "", fmt.Errorf("XDF Go Engine returned HTTP %d: %s", resp.StatusCode, errResp.Error)
		}
		return "", fmt.Errorf("XDF Go Engine returned HTTP %d", resp.StatusCode)
	}
	return string(data), nil
}

func (t *XDFIntakeTool) prepareMedia(values []string) ([]string, []map[string]any, error) {
	if len(values) > 12 {
		return nil, nil, fmt.Errorf("too many media items")
	}
	labels := make([]string, 0, len(values))
	uploads := make([]map[string]any, 0, len(values))
	var total int64
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		path := value
		meta := media.MediaMeta{}
		if strings.HasPrefix(value, "media://") {
			if t.mediaStore == nil {
				return nil, nil, fmt.Errorf("media store is unavailable")
			}
			var err error
			path, meta, err = t.mediaStore.ResolveWithMeta(value)
			if err != nil {
				return nil, nil, fmt.Errorf("resolve inbound media: %w", err)
			}
		} else if !filepath.IsAbs(value) {
			return nil, nil, fmt.Errorf("document media must reference the actual inbound file, not a label")
		}

		data, filename, err := t.readMediaPath(path, meta.Filename, t.maxMediaBytes-total)
		if err != nil {
			return nil, nil, err
		}
		total += int64(len(data))
		contentType := strings.TrimSpace(meta.ContentType)
		if contentType == "" {
			contentType = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
		}
		if contentType == "" {
			contentType = http.DetectContentType(data)
		}
		uploads = append(uploads, map[string]any{
			"filename": filename, "content_type": contentType,
			"data_base64": base64.StdEncoding.EncodeToString(data),
		})
	}
	return labels, uploads, nil
}

func (t *XDFIntakeTool) readMediaPath(path, preferredName string, maxBytes int64) ([]byte, string, error) {
	if maxBytes <= 0 {
		return nil, "", fmt.Errorf("media exceeds the per-request limit")
	}
	rootAbs, err := filepath.Abs(t.mediaRoot)
	if err != nil {
		return nil, "", fmt.Errorf("resolve media root: %w", err)
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return nil, "", fmt.Errorf("resolve media path: %w", err)
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || !filepath.IsLocal(rel) {
		return nil, "", fmt.Errorf("media path is outside the PicoClaw media directory")
	}
	root, err := os.OpenRoot(rootAbs)
	if err != nil {
		return nil, "", fmt.Errorf("open media root: %w", err)
	}
	defer root.Close()
	f, err := root.Open(rel)
	if err != nil {
		return nil, "", fmt.Errorf("open inbound media: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, "", fmt.Errorf("inbound media is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("read inbound media: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, "", fmt.Errorf("media exceeds the per-request limit")
	}
	name := filepath.Base(strings.TrimSpace(preferredName))
	if !xdfSafeMediaLabel(name) {
		name = filepath.Base(pathAbs)
	}
	if !xdfSafeMediaLabel(name) {
		return nil, "", fmt.Errorf("invalid inbound media filename")
	}
	return data, name, nil
}

func xdfSafeMediaLabel(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && value != "." && value != ".." && len(value) <= 255 && filepath.Base(value) == value &&
		!strings.ContainsAny(value, "/\\\x00\r\n") && !strings.Contains(value, "://")
}

func xdfSignedContractHint(values, labels []string, uploads []map[string]any) bool {
	candidates := make([]string, 0, len(values)+len(labels)+len(uploads))
	candidates = append(candidates, values...)
	candidates = append(candidates, labels...)
	for _, upload := range uploads {
		if filename, ok := upload["filename"].(string); ok {
			candidates = append(candidates, filename)
		}
	}
	for _, candidate := range candidates {
		normalized := strings.ToLower(filepath.Base(strings.TrimSpace(candidate)))
		normalized = strings.NewReplacer("_", " ", "-", " ", ".", " ").Replace(normalized)
		if strings.Contains(normalized, "contrato") && strings.Contains(normalized, "assinad") {
			return true
		}
	}
	return false
}

func xdfArgString(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return strings.TrimSpace(value)
}

func xdfArgStrings(args map[string]any, key string) []string {
	value, ok := args[key]
	if !ok {
		return nil
	}
	switch list := value.(type) {
	case []string:
		return list
	case []any:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func xdfArgInt(args map[string]any, key string) int {
	switch value := args[key].(type) {
	case int:
		return value
	case float64:
		return int(value)
	case json.Number:
		parsed, _ := strconv.Atoi(value.String())
		return parsed
	default:
		return 0
	}
}

func xdfBoundedInt(args map[string]any, key string, minimum, maximum, fallback int) int {
	value := xdfArgInt(args, key)
	if value == 0 {
		return fallback
	}
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func xdfMeetingFormat(args map[string]any) string {
	value := xdfArgString(args, "meeting_format")
	if value == "presencial" {
		return value
	}
	return "virtual"
}
