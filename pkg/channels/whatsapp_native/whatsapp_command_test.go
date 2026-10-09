//go:build whatsapp_native

package whatsapp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/media"
)

func TestHandleIncoming_DoesNotConsumeGenericCommandsLocally(t *testing.T) {
	messageBus := bus.NewMessageBus()
	ch := &WhatsAppNativeChannel{
		BaseChannel: channels.NewBaseChannel("whatsapp_native", config.WhatsAppSettings{}, messageBus, nil),
		runCtx:      context.Background(),
	}

	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: types.NewJID("1001", types.DefaultUserServer),
				Chat:   types.NewJID("1001", types.DefaultUserServer),
			},
			ID:       "mid1",
			PushName: "Alice",
		},
		Message: &waE2E.Message{
			Conversation: proto.String("/new"),
		},
	}

	ch.handleIncoming(evt)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatal("timeout waiting for message to be forwarded")
		return
	case inbound, ok := <-messageBus.InboundChan():
		if !ok {
			t.Fatal("expected inbound message to be forwarded")
		}
		if inbound.Channel != "whatsapp_native" {
			t.Fatalf("channel=%q", inbound.Channel)
		}
		if inbound.Content != "/new" {
			t.Fatalf("content=%q", inbound.Content)
		}
	}
}

func TestHandleIncoming_IgnoresStatusBroadcast(t *testing.T) {
	messageBus := bus.NewMessageBus()
	ch := &WhatsAppNativeChannel{
		BaseChannel: channels.NewBaseChannel("whatsapp_native", config.WhatsAppSettings{}, messageBus, nil),
		runCtx:      context.Background(),
	}

	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: types.NewJID("1001", types.DefaultUserServer),
				Chat:   types.StatusBroadcastJID,
			},
			ID:       "status1",
			PushName: "Alice",
		},
		Message: &waE2E.Message{
			Conversation: proto.String("story content"),
		},
	}

	ch.handleIncoming(evt)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	select {
	case inbound, ok := <-messageBus.InboundChan():
		if ok {
			t.Fatalf("status broadcast must not be forwarded as a conversation, got channel=%q content=%q", inbound.Channel, inbound.Content)
		}
	case <-ctx.Done():
		// Expected: no inbound message was forwarded for status broadcasts.
	}
}

func TestAppendLocalMediaPathsExposesOnlyPrivateDownloads(t *testing.T) {
	mediaPath := filepath.Join(media.TempDir(), "wa_3AB0C767_document.pdf")
	got := appendLocalMediaPaths("[📄 contrato_assinado.pdf]", []string{mediaPath, "/etc/passwd"})

	if !strings.Contains(got, "[local_path: "+mediaPath+"]") {
		t.Fatalf("annotation missing trusted media path: %q", got)
	}
	if strings.Contains(got, "/etc/passwd") {
		t.Fatalf("annotation exposed path outside media directory: %q", got)
	}
}

func TestDownloadedMediaFilenamePreservesSanitizedOriginalCardName(t *testing.T) {
	got := downloadedMediaFilename(
		"3AB0C7673F302A84C33B",
		"document",
		".pdf",
		"../../card_183_contrato honorários_assinado.pdf",
	)
	want := "wa_3AB0C767_card_183_contrato_honorários_assinado.pdf"
	if got != want {
		t.Fatalf("downloaded filename=%q, want %q", got, want)
	}
	if strings.Contains(got, "/") || strings.Contains(got, "..") {
		t.Fatalf("downloaded filename is not path-safe: %q", got)
	}
}

func TestDownloadedMediaFilenameFallsBackToMediaType(t *testing.T) {
	got := downloadedMediaFilename("ABC123", "document", ".pdf", "")
	if got != "wa_ABC123_document.pdf" {
		t.Fatalf("downloaded filename=%q, want fallback name", got)
	}
}

func TestHandleIncoming_GroupMessage_IgnoredWhenNotMentioned(t *testing.T) {
	messageBus := bus.NewMessageBus()
	gt := config.GroupTriggerConfig{MentionOnly: true}
	ch := &WhatsAppNativeChannel{
		BaseChannel: channels.NewBaseChannel("whatsapp_native", config.WhatsAppSettings{}, messageBus, nil, channels.WithGroupTrigger(gt)),
		runCtx:      context.Background(),
	}

	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: types.NewJID("1001", types.DefaultUserServer),
				Chat:   types.NewJID("group123", types.GroupServer),
			},
			ID:       "mid_group_1",
			PushName: "Alice",
		},
		Message: &waE2E.Message{
			Conversation: proto.String("conversa normal no grupo sem marcar o bot"),
		},
	}

	ch.handleIncoming(evt)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	select {
	case inbound, ok := <-messageBus.InboundChan():
		if ok {
			t.Fatalf("group message without mention must not be forwarded, got content=%q", inbound.Content)
		}
	case <-ctx.Done():
		// Expected: absolute silence
	}
}

func TestHandleIncoming_GroupMessage_RespondsWhenMentioned(t *testing.T) {
	messageBus := bus.NewMessageBus()
	gt := config.GroupTriggerConfig{MentionOnly: true}
	ch := &WhatsAppNativeChannel{
		BaseChannel: channels.NewBaseChannel("whatsapp_native", config.WhatsAppSettings{}, messageBus, nil, channels.WithGroupTrigger(gt)),
		runCtx:      context.Background(),
	}

	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: types.NewJID("1001", types.DefaultUserServer),
				Chat:   types.NewJID("group123", types.GroupServer),
			},
			ID:       "mid_group_2",
			PushName: "Alice",
		},
		Message: &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String("@5585996081332 qual o status do processo?"),
				ContextInfo: &waE2E.ContextInfo{
					MentionedJID: []string{"5585996081332@s.whatsapp.net"},
				},
			},
		},
	}

	ch.handleIncoming(evt)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Fatal("timeout waiting for mentioned group message to be forwarded")
	case inbound, ok := <-messageBus.InboundChan():
		if !ok {
			t.Fatal("expected inbound message to be forwarded")
		}
		if inbound.Content != "qual o status do processo?" {
			t.Fatalf("expected mention stripped, got content=%q", inbound.Content)
		}
	}
}

func TestHandleIncoming_GroupMessage_RespondsWhenQuoted(t *testing.T) {
	messageBus := bus.NewMessageBus()
	gt := config.GroupTriggerConfig{MentionOnly: true}
	ch := &WhatsAppNativeChannel{
		BaseChannel: channels.NewBaseChannel("whatsapp_native", config.WhatsAppSettings{}, messageBus, nil, channels.WithGroupTrigger(gt)),
		runCtx:      context.Background(),
	}

	quotedBotParticipant := "5585996081332@s.whatsapp.net"
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: types.NewJID("1001", types.DefaultUserServer),
				Chat:   types.NewJID("group123", types.GroupServer),
			},
			ID:       "mid_group_3",
			PushName: "Alice",
		},
		Message: &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String("sim, confirmo"),
				ContextInfo: &waE2E.ContextInfo{
					Participant: &quotedBotParticipant,
				},
			},
		},
	}

	ch.handleIncoming(evt)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Fatal("timeout waiting for quoted group message to be forwarded")
	case inbound, ok := <-messageBus.InboundChan():
		if !ok {
			t.Fatal("expected inbound message to be forwarded")
		}
		if inbound.Content != "sim, confirmo" {
			t.Fatalf("expected content 'sim, confirmo', got content=%q", inbound.Content)
		}
	}
}

func TestHandleIncoming_IgnoresOutboundIsFromMeMessages(t *testing.T) {
	messageBus := bus.NewMessageBus()
	ch := &WhatsAppNativeChannel{
		BaseChannel:     channels.NewBaseChannel("whatsapp_native", config.WhatsAppSettings{}, messageBus, nil),
		runCtx:          context.Background(),
		recentBotMsgIDs: make(map[string]time.Time),
		lastHumanMsgAt:  make(map[string]time.Time),
	}

	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender:   types.NewJID("5585996081332", types.DefaultUserServer),
				Chat:     types.NewJID("1001", types.DefaultUserServer),
				IsFromMe: true,
			},
			ID:       "outbound_1",
			PushName: "Rodolfo",
		},
		Message: &waE2E.Message{
			Conversation: proto.String("Mensagem enviada pelo operador humano no WhatsApp Web"),
		},
	}

	ch.handleIncoming(evt)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	select {
	case inbound, ok := <-messageBus.InboundChan():
		if ok {
			t.Fatalf("IsFromMe messages must never be forwarded to agent bus, got channel=%q content=%q", inbound.Channel, inbound.Content)
		}
	case <-ctx.Done():
		// Expected: dropped without forwarding
	}

	// Verify human intervention timestamp was recorded for this chat
	ch.botMsgMu.Lock()
	lastHuman, recorded := ch.lastHumanMsgAt["1001@s.whatsapp.net"]
	ch.botMsgMu.Unlock()
	if !recorded || time.Since(lastHuman) > 5*time.Second {
		t.Fatalf("expected human intervention recorded for chat, got recorded=%v time=%v", recorded, lastHuman)
	}
}

func TestHandleIncoming_NormalizesSenderAndChatDeviceIDs(t *testing.T) {
	messageBus := bus.NewMessageBus()
	ch := &WhatsAppNativeChannel{
		BaseChannel:     channels.NewBaseChannel("whatsapp_native", config.WhatsAppSettings{}, messageBus, nil),
		runCtx:          context.Background(),
		recentBotMsgIDs: make(map[string]time.Time),
		lastHumanMsgAt:  make(map[string]time.Time),
	}

	// Sender with device :1 (e.g. WhatsApp Web)
	senderWithDevice := types.JID{
		User:   "1001",
		Server: types.DefaultUserServer,
		Device: 1,
	}
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: senderWithDevice,
				Chat:   types.NewJID("1001", types.DefaultUserServer),
			},
			ID:       "mid_dev_1",
			PushName: "Alice",
		},
		Message: &waE2E.Message{
			Conversation: proto.String("Ola de dispositivo pareado"),
		},
	}

	ch.handleIncoming(evt)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Fatal("timeout waiting for normalized message")
	case inbound, ok := <-messageBus.InboundChan():
		if !ok {
			t.Fatal("expected inbound message")
		}
		if inbound.SenderID != "1001@s.whatsapp.net" {
			t.Fatalf("expected normalized senderID without device '1001@s.whatsapp.net', got %q", inbound.SenderID)
		}
		if inbound.ChatID != "1001@s.whatsapp.net" {
			t.Fatalf("expected normalized chatID '1001@s.whatsapp.net', got %q", inbound.ChatID)
		}
	}
}

