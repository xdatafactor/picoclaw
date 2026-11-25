# Gemini 3.8 Flash document ingestion

A document specialist can override global light-model routing and the global
image model without affecting other agents. For Gemini 3.8 Flash ingestion:

```json
{
  "agents": {
    "list": [{
      "id": "ingestion-multimodal",
      "model": "gemini-3.8-flash",
      "routing": {"enabled": false},
      "image_model": "gemini-3.8-flash"
    }]
  },
  "model_list": [{
    "model_name": "gemini-3.8-flash",
    "provider": "gemini",
    "model": "gemini-3.8-flash",
    "thinking_level": "medium"
  }]
}
```

This is a configuration fragment, not a replacement configuration. Preserve
existing workspaces, credentials, channels and other model entries.

Omitted agent routing and image settings inherit the global defaults. An explicit
empty image model disables the global image-model override for that agent.
Setting an agent image model replaces its inherited image fallback list; optional
`image_model_fallbacks` can be provided explicitly.

Gemini 3.8 rejects `minimal` thinking. The provider preserves explicit `medium`
and omits an unsupported default/off level. It also omits `temperature` for 3.8.
PDF content must be supplied as `application/pdf` via the native Gemini media
path; a PDF must not be relabeled as a PNG for an image tool. This configuration
does not repair damaged images or change document classification in XDF Go.

References:
- https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash
- https://ai.google.dev/gemini-api/docs/latest-model
