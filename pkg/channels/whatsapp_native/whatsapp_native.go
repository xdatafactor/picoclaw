//go:build whatsapp_native

// PicoClaw - Ultra-lightweight personal AI agent
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

package whatsapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/identity"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const (
	sqliteDriver   = "sqlite"
	whatsappDBName = "store.db"

	reconnectInitial    = 5 * time.Second
	reconnectMax        = 5 * time.Minute
	reconnectMultiplier = 2.0
	// A successful websocket handshake is not enough: WhatsApp emits Connected
	// only after the stored device has authenticated. Do not advertise a paired
	// channel as ready until that event arrives.
	whatsappAuthenticationTimeout = 30 * time.Second
	// Media downloads happen in the event-handler path. Keep them bounded so a
	// single unavailable attachment cannot stall every later WhatsApp message.
	whatsappMediaDownloadTimeout = 60 * time.Second
	// Keep the agent working immediately, but wait briefly before exposing the
	// composing presence so a reply does not look mechanically instantaneous.
	whatsappTypingInitialDelay    = 5 * time.Second
	whatsappTypingRefreshInterval = 15 * time.Second
	whatsappTypingMaxDuration     = 5 * time.Minute
)

// WhatsAppNativeChannel implements the WhatsApp channel using whatsmeow (in-process, no external bridge).
type WhatsAppNativeChannel struct {
	*channels.BaseChannel
	config       *config.WhatsAppSettings
	storePath    string
	client       *whatsmeow.Client
	container    *sqlstore.Container
	mu           sync.Mutex
	runCtx       context.Context
	runCancel    context.CancelFunc
	authReady    chan struct{}
	authFailed   chan error
	reconnectMu  sync.Mutex
	reconnecting bool
	stopping     atomic.Bool    // set once Stop begins; prevents new wg.Add calls
	wg           sync.WaitGroup // tracks background goroutines (QR handler, reconnect)
}

// NewWhatsAppNativeChannel creates a WhatsApp channel that uses whatsmeow for connection.
// storePath is the directory for the SQLite session store (e.g. workspace/whatsapp).
func NewWhatsAppNativeChannel(
	bc *config.Channel,
	name string,
	cfg *config.WhatsAppSettings,
	bus *bus.MessageBus,
	storePath string,
) (channels.Channel, error) {
	base := channels.NewBaseChannel(name, cfg, bus, bc.AllowFrom, channels.WithMaxMessageLength(65536))
	if storePath == "" {
		storePath = "whatsapp"
	}
	c := &WhatsAppNativeChannel{
		BaseChannel: base,
		config:      cfg,
		storePath:   storePath,
	}
	return c, nil
}

func (c *WhatsAppNativeChannel) Start(ctx context.Context) error {
	logger.InfoCF("whatsapp", "Starting WhatsApp native channel (whatsmeow)", map[string]any{"store": c.storePath})

	// Reset lifecycle state from any previous Stop() so a restarted channel
	// behaves correctly.  Use reconnectMu to be consistent with eventHandler
	// and Stop() which coordinate under the same lock.
	c.reconnectMu.Lock()
	c.stopping.Store(false)
	c.reconnecting = false
	c.reconnectMu.Unlock()

	if err := os.MkdirAll(c.storePath, 0o700); err != nil {
		return fmt.Errorf("create session store dir: %w", err)
	}

	dbPath := filepath.Join(c.storePath, whatsappDBName)
	connStr := "file:" + dbPath + "?_foreign_keys=on"

	db, err := sql.Open(sqliteDriver, connStr)
	if err != nil {
		return fmt.Errorf("open whatsapp store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err = db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		return fmt.Errorf("enable foreign keys: %w", err)
	}

	waLogger := waLog.Stdout("WhatsApp", "WARN", true)
	container := sqlstore.NewWithDB(db, sqliteDriver, waLogger)
	if err = container.Upgrade(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("open whatsapp store: %w", err)
	}

	deviceStore, err := container.GetFirstDevice(ctx)
	if err != nil {
		_ = container.Close()
		return fmt.Errorf("get device store: %w", err)
	}

	client := whatsmeow.NewClient(deviceStore, waLogger)

	// Create runCtx/runCancel BEFORE registering event handler and starting
	// goroutines so that Stop() can cancel them at any time, including during
	// the QR-login flow.
	c.runCtx, c.runCancel = context.WithCancel(ctx)
	c.authReady = make(chan struct{}, 1)
	c.authFailed = make(chan error, 1)

	client.AddEventHandler(c.eventHandler)

	c.mu.Lock()
	c.container = container
	c.client = client
	c.mu.Unlock()

	// cleanupOnError clears struct references and releases resources when
	// Start() fails after fields are already assigned.  This prevents
	// Stop() from operating on stale references (double-close, disconnect
	// of a partially-initialized client, or stray event handler callbacks).
	startOK := false
	defer func() {
		if startOK {
			return
		}
		c.runCancel()
		client.Disconnect()
		c.mu.Lock()
		c.client = nil
		c.container = nil
		c.mu.Unlock()
		_ = container.Close()
		c.SetRunning(false)
	}()

	if client.Store.ID == nil {
		qrChan, err := client.GetQRChannel(c.runCtx)
		if err != nil {
			return fmt.Errorf("get QR channel: %w", err)
		}
		if err := client.Connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		// Handle QR events in a background goroutine so Start() returns
		// promptly.  The goroutine is tracked via c.wg and respects
		// c.runCtx for cancellation.
		// Guard wg.Add with reconnectMu + stopping check (same protocol
		// as eventHandler) so a concurrent Stop() cannot enter wg.Wait()
		// while we call wg.Add(1).
		c.reconnectMu.Lock()
		if c.stopping.Load() {
			c.reconnectMu.Unlock()
			return fmt.Errorf("channel stopped during QR setup")
		}
		c.wg.Add(1)
		c.reconnectMu.Unlock()
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-c.runCtx.Done():
					return
				case evt, ok := <-qrChan:
					if !ok {
						return
					}
					if evt.Event == "code" {
						logger.InfoCF("whatsapp", "Scan this QR code with WhatsApp (Linked Devices):", nil)
						qrterminal.GenerateWithConfig(evt.Code, qrterminal.Config{
							Level:      qrterminal.L,
							Writer:     os.Stdout,
							HalfBlocks: true,
						})
					} else {
						logger.InfoCF("whatsapp", "WhatsApp login event", map[string]any{"event": evt.Event})
					}
				}
			}
		}()
	} else {
		if err := client.Connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}

		authTimer := time.NewTimer(whatsappAuthenticationTimeout)
		defer authTimer.Stop()
		select {
		case <-c.authReady:
			// Authentication completed and the event handler marked the channel ready.
		case authErr := <-c.authFailed:
			return fmt.Errorf("authenticate stored WhatsApp session: %w", authErr)
		case <-authTimer.C:
			// Avoid a false timeout if Connected raced with the timer delivery.
			if !client.IsLoggedIn() {
				return fmt.Errorf("authenticate stored WhatsApp session: timed out after %s", whatsappAuthenticationTimeout)
			}
		case <-c.runCtx.Done():
			return fmt.Errorf("authenticate stored WhatsApp session: %w", c.runCtx.Err())
		}
	}

	startOK = true
	c.SetRunning(true)
	logger.InfoC("whatsapp", "WhatsApp native channel connected")
	return nil
}

func (c *WhatsAppNativeChannel) Stop(ctx context.Context) error {
	logger.InfoC("whatsapp", "Stopping WhatsApp native channel")

	// Mark as stopping under reconnectMu so the flag is visible to
	// eventHandler atomically with respect to its wg.Add(1) call.
	// This closes the TOCTOU window where eventHandler could check
	// stopping (false), then Stop sets it true + enters wg.Wait,
	// then eventHandler calls wg.Add(1) — causing a panic.
	c.reconnectMu.Lock()
	c.stopping.Store(true)
	c.reconnectMu.Unlock()

	if c.runCancel != nil {
		c.runCancel()
	}

	// Disconnect the client first so any blocking Connect()/reconnect loops
	// can be interrupted before we wait on the goroutines.
	c.mu.Lock()
	client := c.client
	container := c.container
	c.mu.Unlock()

	if client != nil {
		client.Disconnect()
	}

	// Wait for background goroutines (QR handler, reconnect) to finish in a
	// context-aware way so Stop can be bounded by ctx.
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines have finished.
	case <-ctx.Done():
		// Context canceled or timed out; log and proceed with best-effort cleanup.
		logger.WarnC("whatsapp", fmt.Sprintf("Stop context canceled before all goroutines finished: %v", ctx.Err()))
	}

	// Now it is safe to clear and close resources.
	c.mu.Lock()
	c.client = nil
	c.container = nil
	c.mu.Unlock()

	if container != nil {
		_ = container.Close()
	}
	c.SetRunning(false)
	return nil
}

func (c *WhatsAppNativeChannel) eventHandler(evt any) {
	switch v := evt.(type) {
	case *events.Message:
		c.handleIncoming(v)
	case *events.Connected:
		c.SetRunning(true)
		logger.InfoC("whatsapp", "WhatsApp native channel authenticated")
		c.signalAuthenticationReady()
	case *events.Disconnected:
		c.SetRunning(false)
		// whatsmeow has its own serialized automatic reconnect loop. Starting a
		// second Connect loop here races that loop and can leave a websocket open
		// without a completed login.
		logger.InfoC("whatsapp", "WhatsApp disconnected; library auto-reconnect is active")
	case events.PermanentDisconnect:
		c.SetRunning(false)
		err := fmt.Errorf("%s", v.PermanentDisconnectDescription())
		logger.ErrorCF("whatsapp", "WhatsApp permanent disconnect", map[string]any{"error": err.Error()})
		c.signalAuthenticationFailure(err)
	}
}

func (c *WhatsAppNativeChannel) signalAuthenticationReady() {
	c.mu.Lock()
	authReady := c.authReady
	c.mu.Unlock()
	if authReady == nil {
		return
	}
	select {
	case authReady <- struct{}{}:
	default:
	}
}

func (c *WhatsAppNativeChannel) signalAuthenticationFailure(err error) {
	c.mu.Lock()
	authFailed := c.authFailed
	c.mu.Unlock()
	if authFailed == nil {
		return
	}
	select {
	case authFailed <- err:
	default:
	}
}

func (c *WhatsAppNativeChannel) reconnectWithBackoff() {
	defer func() {
		c.reconnectMu.Lock()
		c.reconnecting = false
		c.reconnectMu.Unlock()
	}()

	backoff := reconnectInitial
	for {
		select {
		case <-c.runCtx.Done():
			return
		default:
		}

		c.mu.Lock()
		client := c.client
		c.mu.Unlock()
		if client == nil {
			return
		}

		// If we are already connected (e.g. a Disconnected event raced with a
		// successful reconnect, or Connect() succeeded on a previous iteration),
		// there is nothing to do. Calling Connect() while connected would fail
		// with "websocket is already connected" and trigger a reconnect loop.
		if client.IsConnected() {
			logger.InfoC("whatsapp", "WhatsApp already connected, skipping reconnect")
			return
		}

		logger.InfoCF("whatsapp", "WhatsApp reconnecting", map[string]any{"backoff": backoff.String()})
		err := client.Connect()
		if err == nil {
			logger.InfoC("whatsapp", "WhatsApp reconnected")
			return
		}

		logger.WarnCF("whatsapp", "WhatsApp reconnect failed", map[string]any{"error": err.Error()})

		select {
		case <-c.runCtx.Done():
			return
		case <-time.After(backoff):
			if backoff < reconnectMax {
				next := time.Duration(float64(backoff) * reconnectMultiplier)
				if next > reconnectMax {
					next = reconnectMax
				}
				backoff = next
			}
		}
	}
}

// StartTyping implements channels.TypingCapable using WhatsApp chat presence.
// BaseChannel calls this automatically when an inbound WhatsApp message starts
// processing, so the sender sees the "typing..." indicator while the agent
// thinks and writes a reply.
func (c *WhatsAppNativeChannel) StartTyping(ctx context.Context, chatID string) (func(), error) {
	if !c.IsRunning() {
		return func() {}, channels.ErrNotRunning
	}

	to, err := parseJID(chatID)
	if err != nil {
		return func() {}, fmt.Errorf("invalid chat id %q: %w", chatID, err)
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()

	if client == nil || !client.IsConnected() || !client.IsLoggedIn() || client.Store.ID == nil {
		return func() {}, fmt.Errorf("whatsapp connection not established: %w", channels.ErrTemporary)
	}

	typingCtx, cancel := context.WithCancel(ctx)
	maxCtx, maxCancel := context.WithTimeout(typingCtx, whatsappTypingInitialDelay+whatsappTypingMaxDuration)
	var stopOnce sync.Once
	var typingStarted atomic.Bool

	go func() {
		defer maxCancel()
		delay := time.NewTimer(whatsappTypingInitialDelay)
		defer delay.Stop()

		// This goroutine deliberately delays only the visual presence signal;
		// HandleInboundContext continues and starts agent processing immediately.
		select {
		case <-maxCtx.Done():
			return
		case <-delay.C:
		}

		typingStarted.Store(true)
		if err := client.SendChatPresence(maxCtx, to, types.ChatPresenceComposing, types.ChatPresenceMediaText); err != nil {
			logger.WarnCF("whatsapp", "WhatsApp typing indicator failed", map[string]any{"error": err.Error()})
			return
		}

		ticker := time.NewTicker(whatsappTypingRefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-maxCtx.Done():
				return
			case <-ticker.C:
				if err := client.SendChatPresence(typingCtx, to, types.ChatPresenceComposing, types.ChatPresenceMediaText); err != nil {
					logger.WarnCF("whatsapp", "WhatsApp typing refresh failed", map[string]any{"error": err.Error()})
				}
			}
		}
	}()

	stop := func() {
		stopOnce.Do(func() {
			cancel()
			if !typingStarted.Load() {
				return
			}
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer stopCancel()
			if err := client.SendChatPresence(stopCtx, to, types.ChatPresencePaused, types.ChatPresenceMediaText); err != nil {
				logger.WarnCF("whatsapp", "WhatsApp typing stop failed", map[string]any{"error": err.Error()})
			}
		})
	}

	return stop, nil
}

// markRead sends a read receipt for an inbound message so the sender sees blue
// double-checks immediately when the message arrives.
func (c *WhatsAppNativeChannel) markRead(evt *events.Message) {
	if evt == nil || evt.Info.ID == "" {
		return
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil || !client.IsConnected() {
		return
	}

	ctx := c.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.MarkRead(
		readCtx,
		[]types.MessageID{evt.Info.ID},
		time.Now(),
		evt.Info.Chat,
		evt.Info.Sender,
	); err != nil {
		logger.WarnCF("whatsapp", "WhatsApp mark read failed", map[string]any{
			"chat_id":    evt.Info.Chat.String(),
			"message_id": evt.Info.ID,
			"error":      err.Error(),
		})
	}
}

func (c *WhatsAppNativeChannel) handleIncoming(evt *events.Message) {
	if evt.Message == nil {
		return
	}
	// Never treat status/story broadcasts as conversations. WhatsApp status
	// updates arrive with Chat == status@broadcast; processing them would let
	// the agent reply to the status broadcast, which publishes a story on the
	// account. Stories are inbound-only passive content and must never enter
	// the agent pipeline.
	if evt.Info.Chat == types.StatusBroadcastJID || evt.Info.Chat.Server == types.BroadcastServer {
		logger.InfoC("whatsapp", "Ignoring WhatsApp status/broadcast event (stories are not conversations)")
		return
	}
	// Mark the message as read immediately so the sender sees blue double-checks.
	c.markRead(evt)
	senderID := evt.Info.Sender.String()
	chatID := evt.Info.Chat.String()
	content := evt.Message.GetConversation()
	if content == "" && evt.Message.ExtendedTextMessage != nil {
		content = evt.Message.ExtendedTextMessage.GetText()
	}
	content = utils.SanitizeMessageContent(content)

	// Download any media attachments (PDF, images, audio, etc.)
	var mediaPaths []string

	hasImage := evt.Message.GetImageMessage() != nil
	hasDocument := evt.Message.GetDocumentMessage() != nil
	hasAudio := evt.Message.GetAudioMessage() != nil
	hasVideo := evt.Message.GetVideoMessage() != nil
	hasMedia := hasImage || hasDocument || hasAudio || hasVideo

	// If there's neither text nor media, skip (e.g. ephemeral, reaction, etc.)
	if content == "" && !hasMedia {
		return
	}

	if hasMedia {
		c.mu.Lock()
		client := c.client
		c.mu.Unlock()

		if client != nil {
			downloadedPaths := c.downloadMedia(client, evt, chatID, evt.Info.ID)
			mediaPaths = append(mediaPaths, downloadedPaths...)
		}
	}

	// If content is empty but we have media, add a descriptive placeholder
	if content == "" && len(mediaPaths) > 0 {
		// Build a description of what was received
		var descParts []string
		if hasDocument {
			doc := evt.Message.GetDocumentMessage()
			fn := doc.GetFileName()
			if fn == "" {
				fn = "documento"
			}
			descParts = append(descParts, fmt.Sprintf("[📄 %s]", fn))
		}
		if hasImage {
			descParts = append(descParts, "[📷 Imagem]")
		}
		if hasAudio {
			descParts = append(descParts, "[🎵 Áudio]")
		}
		if hasVideo {
			descParts = append(descParts, "[🎬 Vídeo]")
		}
		content = strings.Join(descParts, " ")
	}
	content = appendLocalMediaPaths(content, mediaPaths)

	metadata := make(map[string]string)
	metadata["message_id"] = evt.Info.ID
	if evt.Info.PushName != "" {
		metadata["user_name"] = evt.Info.PushName
	}
	if evt.Info.Chat.Server == types.GroupServer {
		metadata["peer_kind"] = "group"
		metadata["peer_id"] = chatID
	} else {
		metadata["peer_kind"] = "direct"
		metadata["peer_id"] = senderID
	}

	peerKind := "direct"
	if evt.Info.Chat.Server == types.GroupServer {
		peerKind = "group"
	}
	messageID := evt.Info.ID
	sender := bus.SenderInfo{
		Platform:    "whatsapp",
		PlatformID:  senderID,
		CanonicalID: identity.BuildCanonicalID("whatsapp", senderID),
		DisplayName: evt.Info.PushName,
	}

	if !c.IsAllowedSender(sender) {
		return
	}

	mediaScope := channels.BuildMediaScope("whatsapp", chatID, messageID)
	logger.DebugCF(
		"whatsapp",
		"WhatsApp message received",
		map[string]any{
			"sender_id":       senderID,
			"content_preview": utils.Truncate(content, 50),
			"media_paths":     len(mediaPaths),
			"media_scope":     mediaScope,
		},
	)

	inboundCtx := bus.InboundContext{
		Channel:   "whatsapp",
		ChatID:    chatID,
		SenderID:  senderID,
		MessageID: messageID,
		ChatType:  peerKind,
		Raw:       metadata,
	}

	c.HandleInboundContext(c.runCtx, chatID, content, mediaPaths, inboundCtx, sender)
}

// appendLocalMediaPaths exposes the trusted download path to the routed agent.
// The provider media field alone does not give a text model a filesystem path,
// which previously led it to invoke downstream document processing with only
// the original filename. Only files inside PicoClaw's private media directory
// are disclosed in the annotation.
func appendLocalMediaPaths(content string, mediaPaths []string) string {
	mediaRoot := filepath.Clean(media.TempDir())
	annotations := make([]string, 0, len(mediaPaths))
	for _, mediaPath := range mediaPaths {
		cleanPath := filepath.Clean(strings.TrimSpace(mediaPath))
		relative, err := filepath.Rel(mediaRoot, cleanPath)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		annotations = append(annotations, fmt.Sprintf("[local_path: %s]", cleanPath))
	}
	if len(annotations) == 0 {
		return content
	}
	if strings.TrimSpace(content) == "" {
		return strings.Join(annotations, " ")
	}
	return strings.TrimSpace(content) + "\n" + strings.Join(annotations, " ")
}

// downloadMedia downloads all media attachments from a WhatsApp message,
// saves them to the picoclaw_media temp directory, registers them in the
// MediaStore, and returns the local file paths.
func (c *WhatsAppNativeChannel) downloadMedia(client *whatsmeow.Client, evt *events.Message, chatID, messageID string) []string {
	store := c.GetMediaStore()
	mediaDir := media.TempDir()
	var paths []string

	downloadAndSave := func(mediaMsg whatsmeow.DownloadableMessage, ext, description, originalName string) {
		downloadParent := c.runCtx
		if downloadParent == nil {
			downloadParent = context.Background()
		}
		downloadCtx, cancel := context.WithTimeout(downloadParent, whatsappMediaDownloadTimeout)
		defer cancel()
		data, err := client.Download(downloadCtx, mediaMsg)
		if err != nil {
			logger.WarnCF("whatsapp", "media download failed", map[string]any{
				"type":  description,
				"error": err.Error(),
			})
			return
		}

		if err := os.MkdirAll(mediaDir, 0o700); err != nil {
			logger.WarnCF("whatsapp", "media mkdir failed", map[string]any{"error": err.Error()})
			return
		}

		// Preserve a sanitized original document name so downstream contract
		// routing can recover deterministic hints such as card_183. The unique
		// WhatsApp message prefix still prevents collisions.
		filename := downloadedMediaFilename(messageID, description, ext, originalName)
		filePath := filepath.Join(mediaDir, filename)

		if err := os.WriteFile(filePath, data, 0o600); err != nil {
			logger.WarnCF("whatsapp", "media write failed", map[string]any{
				"path":  filePath,
				"error": err.Error(),
			})
			return
		}

		// Register in MediaStore
		scope := channels.BuildMediaScope("whatsapp", chatID, messageID)
		if store != nil {
			if _, err := store.Store(filePath, media.MediaMeta{
				Filename:      filename,
				ContentType:   description,
				Source:        "whatsapp",
				CleanupPolicy: media.CleanupPolicyDeleteOnCleanup,
			}, scope); err != nil {
				logger.WarnCF("whatsapp", "media store registration failed", map[string]any{
					"path":  filePath,
					"error": err.Error(),
				})
			}
		}

		paths = append(paths, filePath)
		logger.InfoCF("whatsapp", "media downloaded", map[string]any{
			"type": description,
			"path": filePath,
			"size": len(data),
		})
	}

	if doc := evt.Message.GetDocumentMessage(); doc != nil {
		originalName := doc.GetFileName()
		ext := strings.ToLower(filepath.Ext(originalName))
		if ext == "" {
			ext = ".bin"
		}
		downloadAndSave(doc, ext, "document", originalName)
	}
	if img := evt.Message.GetImageMessage(); img != nil {
		downloadAndSave(img, ".jpg", "image", "")
	}
	if audio := evt.Message.GetAudioMessage(); audio != nil {
		ext := ".ogg"
		if audio.GetMimetype() == "audio/mp3" {
			ext = ".mp3"
		} else if audio.GetMimetype() == "audio/mp4" || audio.GetMimetype() == "audio/m4a" {
			ext = ".m4a"
		}
		downloadAndSave(audio, ext, "audio", "")
	}
	if video := evt.Message.GetVideoMessage(); video != nil {
		downloadAndSave(video, ".mp4", "video", "")
	}

	return paths
}

func downloadedMediaFilename(messageID, description, ext, originalName string) string {
	cleanMsgID := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, messageID)
	if len(cleanMsgID) > 8 {
		cleanMsgID = cleanMsgID[:8]
	}
	if cleanMsgID == "" {
		cleanMsgID = "media"
	}

	safeOriginal := sanitizeDownloadedMediaName(originalName)
	if safeOriginal == "" {
		safeOriginal = strings.Trim(strings.TrimSpace(description), "._-") + ext
	} else if filepath.Ext(safeOriginal) == "" && ext != "" {
		safeOriginal += ext
	}
	return fmt.Sprintf("wa_%s_%s", cleanMsgID, safeOriginal)
}

func sanitizeDownloadedMediaName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "" || name == "." {
		return ""
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range name {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-':
			b.WriteRune(r)
			lastUnderscore = false
		case r == '_' || unicode.IsSpace(r):
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		default:
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	safe := strings.Trim(b.String(), "._-")
	if len(safe) <= 180 {
		return safe
	}
	extension := filepath.Ext(safe)
	base := strings.TrimSuffix(safe, extension)
	maxBase := 180 - len(extension)
	if maxBase < 1 {
		return truncateUTF8Bytes(safe, 180)
	}
	return truncateUTF8Bytes(base, maxBase) + extension
}

func truncateUTF8Bytes(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	var truncated strings.Builder
	for _, r := range value {
		if truncated.Len()+utf8.RuneLen(r) > maxBytes {
			break
		}
		truncated.WriteRune(r)
	}
	return truncated.String()
}

func (c *WhatsAppNativeChannel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	client := c.client
	c.mu.Unlock()

	if client == nil || !client.IsConnected() || !client.IsLoggedIn() {
		return nil, fmt.Errorf("whatsapp connection not established: %w", channels.ErrTemporary)
	}

	// Detect unpaired state: the client is connected (to WhatsApp servers)
	// but has not completed QR-login yet, so sending would fail.
	if client.Store.ID == nil {
		return nil, fmt.Errorf("whatsapp not yet paired (QR login pending): %w", channels.ErrTemporary)
	}

	to, err := parseJID(msg.ChatID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat id %q: %w", msg.ChatID, err)
	}

	// Hard block: the WhatsApp channel must never publish status/stories.
	// Sending to status@broadcast (or any broadcast JID) would post a story
	// on the account. This is an explicit, non-negotiable safety rule.
	if to.Server == types.BroadcastServer || to == types.StatusBroadcastJID {
		return nil, fmt.Errorf("whatsapp send blocked: refusing to send to broadcast/status JID %q (stories are disabled)", to.String())
	}

	waMsg := &waE2E.Message{
		Conversation: proto.String(msg.Content),
	}

	if _, err = client.SendMessage(ctx, to, waMsg); err != nil {
		return nil, fmt.Errorf("whatsapp send: %w", channels.ErrTemporary)
	}
	return nil, nil
}

// parseJID converts a chat ID (phone number or JID string) to types.JID.
func parseJID(s string) (types.JID, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return types.JID{}, fmt.Errorf("empty chat id")
	}
	if strings.Contains(s, "@") {
		return types.ParseJID(s)
	}
	return types.NewJID(s, types.DefaultUserServer), nil
}
