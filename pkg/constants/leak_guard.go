package constants

import "strings"

// IsClientFacingChannel reports whether the channel directly reaches external
// customers or end users (e.g. WhatsApp).
func IsClientFacingChannel(channel string) bool {
	switch strings.ToLower(strings.TrimSpace(channel)) {
	case "whatsapp", "whatsapp_native":
		return true
	default:
		return false
	}
}

// ContainsLeakedInternalArtifacts returns true if the content contains internal
// system prompts, rules, Chain-of-Thought (CoT) reasoning, or internal diagnostics
// that must NEVER be leaked to clients.
func ContainsLeakedInternalArtifacts(content string) bool {
	lower := strings.ToLower(content)
	markers := []string{
		// System prompt / XML envelope markers
		"<instrucao_sistema>",
		"</instrucao_sistema>",
		"<mensagem_cliente>",
		"</mensagem_cliente>",
		"<thinking>",
		"</thinking>",

		// Internal rule references & guardrail directives
		"the rule says:",
		"regra absoluta",
		"regra zero",
		"proibido output de reasoning",
		"given the guardrail",
		"silêncio operacional",
		"the message was sent via the message tool",
		"se você não tem nada a dizer ao cliente",

		// Infrastructure & provider diagnostic markers (from internal incidents)
		"gemini 404",
		"deepseek cooldown",
		"rate limit interno",
		"exec_restricted",
		"depth limit",
		"delegação aninhada",
		"instabilidade de provider",
		"api_base da rota",
		"sequência de falhas de hoje",
		"sub-turn depth limit exceeded",

		// Generic CoT / reasoning trace patterns
		"chain of thought",
		"reasoning trace",
		"internal diagnostic",
	}
	for _, m := range markers {
		if strings.Contains(lower, m) {
			return true
		}
	}
	return false
}
