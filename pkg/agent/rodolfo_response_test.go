package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestAgentMaxTokensOverrideDoesNotChangeOtherAgents(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.MaxTokens = 8192
	rodolfo := NewAgentInstance(&config.AgentConfig{ID: "rodolfo-rfreire", MaxTokens: 16384}, &cfg.Agents.Defaults, cfg, &mockProvider{})
	defer rodolfo.Close()
	other := NewAgentInstance(nil, &cfg.Agents.Defaults, cfg, &mockProvider{})
	defer other.Close()
	if rodolfo.MaxTokens != 16384 || other.MaxTokens != 8192 || cfg.Agents.Defaults.MaxTokens != 8192 {
		t.Fatal("agent-specific token budget was not isolated")
	}
}

func TestRodolfoAllEmptyProvidersReturnCustomerMessage(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{{ReasoningContent: "internal", FinishReason: "length"}, {FinishReason: "length"}}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	agent.ID = "rodolfo-rfreire"
	agent.Candidates = []providers.FallbackCandidate{{Provider: "openai", Model: "primary"}, {Provider: "openai", Model: "secondary"}}
	al.fallback = providers.NewFallbackChain(providers.NewCooldownTracker(), nil)
	opts := makeTestProcessOpts("empty-rodolfo")
	opts.Channel = "whatsapp_native"
	opts = normalizeProcessOptions(opts)
	pipeline := NewPipeline(al)
	ts := newTurnState(agent, opts, turnEventScope{turnID: "empty", context: newTurnContext(nil, nil, nil)})
	exec, err := pipeline.SetupTurn(context.Background(), ts)
	if err != nil {
		t.Fatal(err)
	}
	ctrl, err := pipeline.CallLLM(context.Background(), context.Background(), ts, exec, 1)
	if err != nil || ctrl != ControlBreak || exec.finalContent != rodolfoUnavailableResponse {
		t.Fatalf("ctrl=%v error=%v content=%q", ctrl, err, exec.finalContent)
	}
}

func TestRodolfoEmptyResponseUsesNextProvider(t *testing.T) {
	provider := &sequenceProvider{responses: []*providers.LLMResponse{{ReasoningContent: "internal", FinishReason: "length"}, {Content: "Resposta recuperada", FinishReason: "stop"}}}
	al, agent, cleanup := newTurnCoordTestLoop(t, provider)
	defer cleanup()
	agent.ID = "rodolfo-rfreire"
	agent.Candidates = []providers.FallbackCandidate{{Provider: "openai", Model: "primary"}, {Provider: "openai", Model: "secondary"}}
	al.fallback = providers.NewFallbackChain(providers.NewCooldownTracker(), nil)
	opts := makeTestProcessOpts("recovered-rodolfo")
	opts.Channel = "whatsapp_native"
	opts = normalizeProcessOptions(opts)
	pipeline := NewPipeline(al)
	ts := newTurnState(agent, opts, turnEventScope{turnID: "recover", context: newTurnContext(nil, nil, nil)})
	exec, err := pipeline.SetupTurn(context.Background(), ts)
	if err != nil {
		t.Fatal(err)
	}
	ctrl, err := pipeline.CallLLM(context.Background(), context.Background(), ts, exec, 1)
	if err != nil || ctrl != ControlBreak || exec.finalContent != "Resposta recuperada" {
		t.Fatalf("ctrl=%v error=%v content=%q", ctrl, err, exec.finalContent)
	}
}
