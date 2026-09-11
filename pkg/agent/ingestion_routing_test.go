package agent

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestIngestionOverridesKeepTextAndMediaOnConfiguredModel(t *testing.T) {
	vision := "gemini-ingestion"
	cfg := testCfg(nil)
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.ImageModel = "other-vision"
	cfg.Agents.Defaults.Routing = &config.RoutingConfig{Enabled: true, LightModel: "light", Threshold: 0.35}
	cfg.ModelList = []*config.ModelConfig{
		{ModelName: vision, Model: "gemini/gemini-3.8-flash", ThinkingLevel: "medium"},
		{ModelName: "other-vision", Model: "openai/other-vision"},
		{ModelName: "light", Model: "openai/light"},
	}
	a := NewAgentInstance(&config.AgentConfig{
		ID: "ingestion-multimodal", Model: &config.AgentModelConfig{Primary: vision},
		Routing: &config.RoutingConfig{Enabled: false}, ImageModel: &vision,
	}, &cfg.Agents.Defaults, cfg, &mockRegistryProvider{})
	if a.Router != nil || len(a.LightCandidates) != 0 {
		t.Fatal("specialist still inherits light model routing")
	}
	if a.ImageModel != vision || len(a.ImageCandidates) != 1 || a.ImageCandidates[0].Model != "gemini-3.8-flash" {
		t.Fatalf("incorrect media configuration: %s %+v", a.ImageModel, a.ImageCandidates)
	}
	if a.ThinkingLevel != ThinkingMedium || !a.ThinkingLevelConfigured {
		t.Fatalf("thinking level = %v, want medium", a.ThinkingLevel)
	}
	if !cfg.Agents.Defaults.Routing.Enabled || cfg.Agents.Defaults.ImageModel != "other-vision" {
		t.Fatal("specialist mutated other agents' defaults")
	}
	candidates, model, light := (&AgentLoop{}).selectCandidates(a, "Read this PDF", nil)
	if light || model != "gemini-3.8-flash" || len(candidates) == 0 {
		t.Fatal("text delegation selected the wrong model")
	}
	a.CandidateProviders[providers.ModelKey("gemini", "gemini-3.8-flash")] = &mockRegistryProvider{}
	exec := &turnExecution{callMessages: []providers.Message{{Role: "user", Media: []string{"data:image/png;base64,test"}}}}
	if err := (&Pipeline{Cfg: cfg}).routeMediaTurn(&turnState{agent: a}, exec); err != nil {
		t.Fatal(err)
	}
	if exec.llmModelName != vision || exec.activeModel != "gemini-3.8-flash" {
		t.Fatalf("media routed to %s/%s", exec.llmModelName, exec.activeModel)
	}
}
