package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestContainsLeakedInternalArtifacts(t *testing.T) {
	tests := []struct {
		name    string
		content string
		leaked  bool
	}{
		{
			name:    "normal customer message",
			content: "Olá! Como posso ajudar você hoje?",
			leaked:  false,
		},
		{
			name:    "system instruction tag",
			content: "<instrucao_sistema>Aguarde o CPF do cliente.</instrucao_sistema> Olá!",
			leaked:  true,
		},
		{
			name:    "english thinking quoting the rule",
			content: "The message was sent via the message tool as required for internal/operator flow? Wait — actually the exception in the rules says: for direct WhatsApp flow...",
			leaked:  true,
		},
		{
			name:    "rule quote",
			content: "The rule says: \"Resposta Única: Responda diretamente no texto.\"",
			leaked:  true,
		},
		{
			name:    "guardrail quote",
			content: "Given the guardrail REGRA ABSOLUTA #2 — NUNCA escreva...",
			leaked:  true,
		},
		{
			name:    "regra zero quote",
			content: "Seguindo a REGRA ZERO — PROIBIDO OUTPUT DE REASONING INTERNO...",
			leaked:  true,
		},
		{
			name:    "wallace leak - gemini 404",
			content: "Recapitulando a sequência de falhas de hoje:\n1. 🔴 Gemini 404 — api_base da rota OpenAI-compatível inválido",
			leaked:  true,
		},
		{
			name:    "wallace leak - deepseek cooldown",
			content: "2. 🔴 DeepSeek cooldown — rate limit interno",
			leaked:  true,
		},
		{
			name:    "wallace leak - exec_restricted",
			content: "4. 🚫 EXEC_RESTRICTED — comando barrado pelo guard de segurança",
			leaked:  true,
		},
		{
			name:    "wallace leak - depth limit",
			content: "5. Depth limit — delegação aninhada além de 3 níveis",
			leaked:  true,
		},
		{
			name:    "thinking tags",
			content: "<thinking>Let me analyze this request...</thinking>",
			leaked:  true,
		},
		{
			name:    "safe message with numbers",
			content: "Sua reunião foi agendada para 11/09 às 14:00. Confirmação enviada por e-mail.",
			leaked:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsLeakedInternalArtifacts(tt.content)
			if got != tt.leaked {
				t.Errorf("containsLeakedInternalArtifacts(%q) = %v, want %v", tt.content, got, tt.leaked)
			}
		})
	}
}

func TestPublishResponseIfNeeded_BlocksLeakedContentOnWhatsApp(t *testing.T) {
	mb := bus.NewMessageBus()
	al := &AgentLoop{
		bus: mb,
	}

	leakedContent := "The rule says: Responda diretamente no texto. Given the guardrail I will produce empty output."
	ctx := context.Background()

	// WhatsApp channel should block the leaked content
	al.PublishResponseIfNeeded(ctx, "whatsapp_native", "12345@lid", "sess-1", leakedContent)

	select {
	case msg := <-mb.OutboundChan():
		t.Fatalf("expected leaked message to be blocked, but received: %+v", msg)
	default:
		// Passed - message was blocked
	}

	// Normal content should be published
	normalContent := "Olá, como posso ajudar?"
	al.PublishResponseIfNeeded(ctx, "whatsapp_native", "12345@lid", "sess-1", normalContent)

	select {
	case msg := <-mb.OutboundChan():
		if msg.Content != normalContent {
			t.Errorf("expected content %q, got %q", normalContent, msg.Content)
		}
	default:
		t.Fatal("expected normal message to be published, but got nothing")
	}
}

func TestPublishResponseIfNeeded_SuppressesDuplicateWhenSessionAgentAlreadySent(t *testing.T) {
	mb := bus.NewMessageBus()
	mt := tools.NewMessageTool()
	mt.SetSendCallback(func(
		ctx context.Context,
		channel, chatID, content, replyToMessageID string,
		mediaParts []bus.MediaPart,
	) error {
		return nil
	})

	sessionKey := "agent_rodolfo-rfreire_whatsapp_native_direct_551199999999"
	channel := "whatsapp_native"
	chatID := "551199999999@s.whatsapp.net"

	toolsReg := tools.NewToolRegistry()
	toolsReg.Register(mt)

	rodolfoAgent := &AgentInstance{
		ID:    "rodolfo-rfreire",
		Tools: toolsReg,
	}

	agentReg := &AgentRegistry{
		agents: map[string]*AgentInstance{
			"rodolfo-rfreire": rodolfoAgent,
		},
	}

	al := &AgentLoop{
		bus:      mb,
		registry: agentReg,
	}

	// Simulate sessionAgent having sent a message to the chat
	res := mt.Execute(
		tools.WithToolSessionContext(context.Background(), "rodolfo-rfreire", sessionKey, nil),
		map[string]any{
			"content": "pergunta prévia ao cliente",
			"channel": channel,
			"chat_id": chatID,
		},
	)
	if res == nil || res.IsError {
		t.Fatalf("mt.Execute failed: %+v", res)
	}

	ctx := context.Background()
	al.PublishResponseIfNeeded(ctx, channel, chatID, sessionKey, "Stray duplicate response")

	select {
	case msg := <-mb.OutboundChan():
		t.Fatalf("expected duplicate outbound to be suppressed, but received: %+v", msg)
	default:
		// Passed - suppressed
	}
}

func TestProcessSystemMessage_SuppressesClientFacingChannels(t *testing.T) {
	mb := bus.NewMessageBus()
	al := &AgentLoop{
		bus: mb,
	}

	ctx := context.Background()
	msg := bus.InboundMessage{
		Channel:  "system",
		ChatID:   "whatsapp_native:90779234103389@lid",
		SenderID: "async:spawn",
		Context: bus.InboundContext{
			Channel:  "system",
			ChatID:   "whatsapp_native:90779234103389@lid",
			ChatType: "direct",
			SenderID: "async:spawn",
		},
		Content: "Task completed with error: depth limit exceeded",
	}

	resp, err := al.processSystemMessage(ctx, msg)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if resp != "" {
		t.Fatalf("expected empty response for client-facing origin channel, got: %q", resp)
	}

	// Verify nothing was emitted to the outbound channel
	select {
	case outMsg := <-mb.OutboundChan():
		t.Fatalf("expected no outbound message to be published, but got: %+v", outMsg)
	default:
		// Passed
	}
}
