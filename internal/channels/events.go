package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

// HandleAgentEvent routes agent lifecycle events to streaming/reaction channels.
// Called from the bus event subscriber — must be non-blocking.
// eventType: "run.started", "chunk", "tool.call", "tool.result", "run.completed", "run.failed", "run.cancelled"
func (m *Manager) HandleAgentEvent(eventType, runID string, payload any) {
	val, ok := m.runs.Load(runID)
	if !ok {
		return
	}
	rc := val.(*RunContext)

	m.mu.RLock()
	ch, exists := m.channels[rc.ChannelName]
	m.mu.RUnlock()
	if !exists {
		return
	}

	ctx := context.Background()
	// Use RunContext's TenantID directly (set at RegisterRun time from channel instance)
	// rather than querying the channel interface - more direct and future-proof for
	// channels that might serve multiple tenants.
	if rc.TenantID != uuid.Nil {
		ctx = store.WithTenantID(ctx, rc.TenantID)
	}

	// Forward to StreamingChannel (only when streaming is enabled for this run).
	// Without this gate, channels that implement StreamingChannel but have streaming
	// disabled (e.g. group_stream=false) would create stream messages AND emit
	// block.reply outbound messages, causing duplicate delivery.
	if sc, ok := ch.(StreamingChannel); ok && rc.Streaming {
		switch eventType {
		case protocol.AgentEventRunStarted:
			// Defer stream creation — the first content event (thinking,
			// tool.call, or chunk) will lazily create the stream card.
			// This avoids a placeholder "..." card on DingTalk that stays
			// empty when the agent only executes tools before producing text.
		case protocol.ChatEventThinking:
			// Accumulate thinking/reasoning content and route to the current stream.
			// Lazily creates the reasoning stream on first thinking event.
			// When the first chunk arrives, this stream is stopped (reasoning message stays
			// visible) and a new stream is created for the answer lane.
			// Gated by ReasoningStreamEnabled() — channels can opt out (e.g. Slack, DingTalk).
			if !sc.ReasoningStreamEnabled() {
				break
			}
			content := extractPayloadString(payload, "content")
			if content != "" {
				rc.mu.Lock()
				rc.thinkingBuffer += content
				rc.hasThinking = true
				thinkText := rc.thinkingBuffer
				currentStream := rc.stream
				if currentStream == nil {
					rc.mu.Unlock()
					stream, err := sc.CreateStream(ctx, rc.ChatID, true)
					if err != nil {
						slog.Error("stream start failed (thinking)", "channel", rc.ChannelName, "error", err)
						break
					}
					rc.mu.Lock()
					rc.stream = stream
					rc.streamCreated = true
					currentStream = stream
				}
				rc.mu.Unlock()
				currentStream.Update(ctx, formatReasoningPreview(thinkText))
			}
		case protocol.AgentEventToolCall:
			// Agent is executing a tool — mark tool phase so the next chunk
			// (new LLM iteration) resets the stream buffer.
			// Stop the current stream (reasoning or answer) and finalize only
			// the answer stream (reasoning messages stay visible).
			//
			// If no content was ever streamed (streamBuffer empty and no thinking),
			// keep the stream alive so the next chunk iteration reuses it instead
			// of creating a new one. This prevents orphaned "..." placeholder
			// cards on DingTalk when the agent calls tools before producing text.
			rc.mu.Lock()
			currentStream := rc.stream
			hasStreamContent := (rc.streamBuffer != "" && !rc.toolStatusOnly) || rc.hasThinking
			if hasStreamContent {
				rc.stream = nil
			}
			// else: keep rc.stream alive — no content was streamed yet (e.g. card
			// only shows initial placeholder). The next chunk will reuse it.
			rc.inToolPhase = true
			rc.thinkingDone = false    // allow new thinking in next iteration
			rc.thinkingBuffer = ""     // reset thinking buffer for new iteration
			rc.hasThinking = false     // new iteration starts fresh
			rc.tagParseSkipped = false // re-enable tag parsing for next iteration
			rc.mu.Unlock()
			if currentStream != nil && hasStreamContent {
				if err := currentStream.Stop(ctx); err != nil {
					slog.Debug("stream tool-phase stop failed", "channel", rc.ChannelName, "error", err)
				}
				// Don't finalize mid-run streams — their messageID must NOT go
				// into placeholders. Otherwise tool_status placeholder_update
				// overwrites streamed content, and subsequent FinalizeStream
				// calls overwrite the placeholder key, leaving earlier messages
				// stuck at tool status text. Only run.completed finalizes.
			}

			// Show tool status by editing placeholder message (non-streaming only).
			// Streaming channels show tool status via reaction emoji instead —
			// editing the placeholder would overwrite streamed content.
			toolName := extractPayloadString(payload, "name")
			if toolName != "" && rc.ToolStatusEnabled && !rc.Streaming {
				statusText := formatToolStatus(toolName)
				outMeta := copyRoutingMeta(rc.Metadata)
				outMeta["placeholder_update"] = "true"
				m.bus.PublishOutbound(bus.OutboundMessage{
					Channel:  rc.ChannelName,
					ChatID:   rc.ChatID,
					Content:  statusText,
					Metadata: outMeta,
					TenantID: rc.TenantID,
				})
			}

			// Show tool status in streaming card. Accumulate tool names so
			// the card shows all tools called (persists after finalization).
			if toolName != "" && rc.ToolStatusEnabled && rc.Streaming && sc != nil {
				statusText := formatToolStatus(toolName)
				if argsStr := extractPayloadJSON(payload, "arguments"); argsStr != "" && argsStr != "{}" {
					statusText += fmt.Sprintf("\n> **Args:**\n> ```json\n> %s\n> ```", strings.ReplaceAll(argsStr, "\n", "\n> "))
				}

				rc.mu.Lock()
				if rc.toolStatusOnly {
					rc.streamBuffer += "\n\n" + statusText + "\n"
				} else {
					rc.streamBuffer = statusText + "\n"
				}
				rc.toolStatusOnly = true
				fullStatus := rc.streamBuffer
				currentStream = rc.stream
				if currentStream == nil {
					// No stream yet — lazily create one for tool status.
					isFirst := !rc.streamCreated
					rc.streamCreated = true
					rc.mu.Unlock()
					stream, err := sc.CreateStream(ctx, rc.ChatID, isFirst)
					if err != nil {
						slog.Debug("stream tool-status create failed", "channel", rc.ChannelName, "error", err)
					} else {
						rc.mu.Lock()
						rc.stream = stream
						currentStream = stream
						rc.mu.Unlock()
					}
				} else {
					rc.mu.Unlock()
				}
				if currentStream != nil {
					currentStream.Update(ctx, fullStatus)
				}
			}
		case protocol.AgentEventToolResult:
			result := extractPayloadString(payload, "result")
			isError, _ := extractPayloadBool(payload, "is_error")
			if result != "" && rc.ToolStatusEnabled && rc.Streaming && sc != nil {
				rc.mu.Lock()
				if rc.toolStatusOnly {
					title := "Result"
					if isError {
						title = "Error"
					}
					rc.streamBuffer += fmt.Sprintf("\n> **%s:**\n> ```text\n> %s\n> ```\n", title, strings.ReplaceAll(result, "\n", "\n> "))
					fullStatus := rc.streamBuffer
					currentStream := rc.stream
					rc.mu.Unlock()
					if currentStream != nil {
						currentStream.Update(ctx, fullStatus)
					}
				} else {
					rc.mu.Unlock()
				}
			}
		case protocol.ChatEventChunk:
			// Accumulate chunk deltas into full text.
			content := extractPayloadString(payload, "content")
			if content != "" {
				rc.mu.Lock()
				// Transition from tool phase to text streaming.
				// When the current card shows tool status, keep it alive and
				// append response text below the status lines (same card).
				// This avoids orphan empty cards when the model emits tiny
				// whitespace chunks between tool iterations.
				needNewStream := rc.inToolPhase && rc.stream == nil
				if rc.inToolPhase {
					if rc.toolStatusOnly && rc.stream != nil {
						// Keep tool-status card — text will appear below status.
						rc.streamBuffer += "\n\n---\n\n"
					} else {
						rc.streamBuffer = ""
					}
					rc.inToolPhase = false
					rc.toolStatusOnly = false
				}

				// Lazy stream creation: first chunk with no prior stream
				// (deferred from run.started to avoid empty placeholder cards).
				if rc.stream == nil && !needNewStream {
					needNewStream = true
				}

				// Fallback <think> tag parsing: for providers that embed thinking
				// in the content stream (DeepSeek-via-OpenRouter, Qwen, some Ollama models).
				// Only activates when no native ChatEventThinking was received.
				if !rc.hasThinking && !rc.thinkingDone && !rc.tagParseSkipped {
					candidate := rc.streamBuffer + content
					split := SplitThinkTags(candidate)
					if split.Thinking != "" {
						// Found think tags — commit to buffer and route to reasoning lane
						rc.streamBuffer = candidate
						rc.hasThinking = true
						rc.thinkingBuffer = split.Thinking
						thinkText := rc.thinkingBuffer
						currentStream := rc.stream
						if split.Partial {
							// Still inside <think> — update reasoning stream, wait for close
							rc.mu.Unlock()
							if currentStream != nil {
								currentStream.Update(ctx, formatReasoningPreview(thinkText))
							}
							break
						}
						// Tag closed — transition to answer
						rc.thinkingDone = true
						rc.streamBuffer = split.Answer
						reasoningStream := currentStream
						rc.mu.Unlock()

						// Stop reasoning stream
						if reasoningStream != nil {
							_ = reasoningStream.Stop(ctx)
						}
						// Create answer stream
						stream, err := sc.CreateStream(ctx, rc.ChatID, false)
						if err != nil {
							slog.Debug("stream restart after think-tag failed", "channel", rc.ChannelName, "error", err)
						} else {
							rc.mu.Lock()
							rc.stream = stream
							rc.mu.Unlock()
						}
						// Update answer stream with extracted answer content
						if split.Answer != "" {
							rc.mu.Lock()
							currentStream = rc.stream
							rc.mu.Unlock()
							if currentStream != nil {
								currentStream.Update(ctx, split.Answer)
							}
						}
						break
					}
					// No think tags found — mark as skipped so we don't re-parse.
					// Don't commit to streamBuffer here — the normal flow below appends content.
					rc.tagParseSkipped = true
				}

				// Reasoning→answer transition: first chunk after native thinking events.
				// Stop the reasoning stream (keep message visible) and create a
				// new stream for the answer lane.
				needTransition := rc.hasThinking && !rc.thinkingDone
				if needTransition {
					rc.thinkingDone = true
					rc.streamBuffer = "" // fresh answer buffer
				}
				reasoningStream := rc.stream
				rc.mu.Unlock()

				// Finalize reasoning stream (stop editing, keep message)
				if needTransition && reasoningStream != nil {
					_ = reasoningStream.Stop(ctx)
					// Don't call FinalizeStream — reasoning messageID should NOT
					// go into placeholders. Send() must edit the answer message.
				}

				// Create fresh stream for answer (or new tool iteration)
				if needNewStream || needTransition {
					rc.mu.Lock()
					isFirst := !rc.streamCreated
					rc.streamCreated = true
					rc.mu.Unlock()
					stream, err := sc.CreateStream(ctx, rc.ChatID, isFirst)
					if err != nil {
						slog.Debug("stream restart failed", "channel", rc.ChannelName, "error", err)
					} else {
						rc.mu.Lock()
						rc.stream = stream
						rc.mu.Unlock()
					}
				}

				rc.mu.Lock()
				rc.streamBuffer += content
				fullText := rc.streamBuffer
				currentStream := rc.stream
				rc.mu.Unlock()
				if currentStream != nil {
					currentStream.Update(ctx, fullText)
				}
			}
		case protocol.AgentEventRunCompleted:
			rc.mu.Lock()
			currentStream := rc.stream
			rc.stream = nil
			rc.mu.Unlock()
			if currentStream != nil {
				if err := currentStream.Stop(ctx); err != nil {
					slog.Debug("stream end failed", "channel", rc.ChannelName, "error", err)
				}
				sc.FinalizeStream(ctx, rc.ChatID, currentStream)
			}
		case protocol.AgentEventRunFailed:
			// Clean up streaming state on failure
			rc.mu.Lock()
			currentStream := rc.stream
			rc.stream = nil
			rc.mu.Unlock()
			if currentStream != nil {
				_ = currentStream.Stop(ctx)
			}
			// Issue 958: Send user-friendly error message instead of silent "..."
			errStr := extractPayloadString(payload, "error")
			if friendlyMsg := FormatAgentError(errStr); friendlyMsg != "" {
				m.bus.PublishOutbound(bus.OutboundMessage{
					Channel:  rc.ChannelName,
					ChatID:   rc.ChatID,
					Content:  friendlyMsg,
					TenantID: rc.TenantID,
				})
			}
		case protocol.AgentEventRunCancelled:
			// Clean up streaming state on cancellation
			rc.mu.Lock()
			currentStream := rc.stream
			rc.stream = nil
			rc.mu.Unlock()
			if currentStream != nil {
				_ = currentStream.Stop(ctx)
			}
		}
	}

	// Handle block.reply: deliver intermediate assistant text to non-streaming channels.
	// Gated by BlockReplyEnabled (resolved from gateway + per-channel config at RegisterRun time).
	// Streaming channels already deliver via chunks, so skip to avoid double-delivery.
	if eventType == protocol.AgentEventBlockReply {
		if !rc.BlockReplyEnabled {
			return
		}
		content := extractPayloadString(payload, "content")
		if content == "" {
			return
		}
		rc.mu.Lock()
		streaming := rc.Streaming
		rc.mu.Unlock()

		if streaming {
			return // streaming already delivered via chunks
		}

		// Build outbound metadata: copy routing fields but strip reply_to_message_id
		// (block replies are standalone) and placeholder_key (reserve for final message).
		// feishu_reply_target_id MUST be preserved so intermediate block replies for
		// threaded Lark messages also land inside the same thread.
		var outMeta map[string]string
		if rc.Metadata != nil {
			outMeta = make(map[string]string)
			for _, k := range routingMetaKeys {
				if v := rc.Metadata[k]; v != "" {
					outMeta[k] = v
				}
			}
			if len(outMeta) == 0 {
				outMeta = nil
			}
		}

		m.bus.PublishOutbound(bus.OutboundMessage{
			Channel:  rc.ChannelName,
			ChatID:   rc.ChatID,
			Content:  content,
			Metadata: outMeta,
			TenantID: rc.TenantID,
		})
		return
	}

	// Handle LLM retry: update placeholder to notify user
	if eventType == protocol.AgentEventRunRetrying {
		attempt := extractPayloadString(payload, "attempt")
		maxAttempts := extractPayloadString(payload, "maxAttempts")
		retryMsg := fmt.Sprintf("Provider busy, retrying... (%s/%s)", attempt, maxAttempts)
		m.bus.PublishOutbound(bus.OutboundMessage{
			Channel:  rc.ChannelName,
			ChatID:   rc.ChatID,
			Content:  retryMsg,
			TenantID: rc.TenantID,
			Metadata: map[string]string{
				"placeholder_update": "true",
			},
		})
	}

	// Forward to ReactionChannel
	if reactionCh, ok := ch.(ReactionChannel); ok {
		status := ""
		switch eventType {
		case protocol.AgentEventRunStarted:
			status = "thinking"
		case protocol.AgentEventToolCall:
			// Use tool-specific reaction statuses to activate existing variants
			// (web → ⚡, coding → 👨‍💻) that are already defined in channel reaction maps.
			toolName := extractPayloadString(payload, "name")
			status = resolveToolReactionStatus(toolName)
		case protocol.AgentEventRunCompleted:
			status = "done"
		case protocol.AgentEventRunFailed:
			status = "error"
		case protocol.AgentEventRunCancelled:
			status = "done"
		}
		if status != "" {
			if err := reactionCh.OnReactionEvent(ctx, rc.ChatID, rc.MessageID, status); err != nil {
				slog.Debug("reaction event failed", "channel", rc.ChannelName, "status", status, "error", err)
			}
		}
	}

	// Clean up on terminal events
	if eventType == protocol.AgentEventRunCompleted || eventType == protocol.AgentEventRunFailed || eventType == protocol.AgentEventRunCancelled {
		m.runs.Delete(runID)
	}
}

// extractPayloadString extracts a string field from a payload (map[string]string or map[string]interface{}).
func extractPayloadString(payload any, key string) string {
	switch p := payload.(type) {
	case map[string]string:
		return p[key]
	case map[string]any:
		if v, ok := p[key].(string); ok {
			return v
		}
	}
	return ""
}

// extractPayloadJSON extracts and formats a field as JSON.
func extractPayloadJSON(payload any, key string) string {
	var val any
	switch p := payload.(type) {
	case map[string]any:
		val = p[key]
	}
	if val == nil {
		return ""
	}
	b, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		return ""
	}
	return string(b)
}

// extractPayloadBool extracts a boolean field.
func extractPayloadBool(payload any, key string) (bool, bool) {
	switch p := payload.(type) {
	case map[string]any:
		if v, ok := p[key].(bool); ok {
			return v, true
		}
	}
	return false, false
}


// toolStatusMap maps builtin tool names to user-friendly status messages.
var toolStatusMap = map[string]string{
	// Filesystem
	"read_file":  "📝 Reading file...",
	"write_file": "📝 Writing file...",
	"list_files": "📝 Listing files...",
	"edit":       "📝 Editing file...",
	// Runtime
	"exec": "⚡ Running code...",
	// Web
	"web_search": "🔍 Searching the web...",
	"web_fetch":  "🔍 Fetching web content...",
	// Memory
	"memory_search":          "🧠 Searching memory...",
	"memory_get":             "🧠 Retrieving memory...",
	"knowledge_graph_search": "🧠 Querying knowledge graph...",
	// Media
	"read_image":    "👁 Analyzing image...",
	"read_document": "📄 Reading document...",
	"read_audio":    "🎧 Processing audio...",
	"read_video":    "🎬 Processing video...",
	"create_image":  "🎨 Creating image...",
	"create_video":  "🎬 Creating video...",
	"create_audio":  "🎵 Creating audio...",
	"tts":           "🔊 Generating speech...",
	"deliver_file":  "📤 Sending file...",
	// Browser
	"browser": "🌐 Browsing...",
	// Delegation & teams
	"spawn":        "👥 Delegating task...",
	"team_tasks":   "📋 Managing team tasks...",
	// Sessions
	"sessions_list":    "📋 Listing sessions...",
	"session_status":   "📋 Checking session...",
	"sessions_history": "📋 Reading history...",
	"sessions_send":    "📤 Sending message...",
	// Other
	"message":         "📤 Sending message...",
	"cron":            "⏰ Managing schedule...",
	"skill_search":    "🔍 Searching skills...",
	"use_skill":       "🧩 Using skill...",
	"mcp_tool_search": "🔌 Searching MCP tools...",
}

// toolPrefixStatus maps tool name prefixes to status messages (fallback for dynamic tools).
var toolPrefixStatus = []struct {
	prefix string
	status string
}{
	{"mcp_", "🔌 Using external tool..."},
}

// formatToolStatus returns a user-friendly status message for a tool name.
func formatToolStatus(toolName string) string {
	if s, ok := toolStatusMap[toolName]; ok {
		return s
	}
	for _, p := range toolPrefixStatus {
		if strings.HasPrefix(toolName, p.prefix) {
			return p.status
		}
	}
	return "🔧 Running " + toolName + "..."
}

// formatReasoningPreview formats accumulated thinking text for display as a
// streaming reasoning message. Uses markdown italic prefix so channels that
// convert markdown (Telegram, Slack) show "Reasoning:" in italics.
// Truncated to 4096 runes (Telegram limit, rune-safe for CJK/emoji).
func formatReasoningPreview(thinking string) string {
	if thinking == "" {
		return ""
	}
	const maxRunes = 4096
	text := "_Reasoning:_\n" + thinking
	runes := []rune(text)
	if len(runes) > maxRunes {
		text = string(runes[:maxRunes-3]) + "..."
	}
	return text
}

// resolveToolReactionStatus maps a tool name to a reaction status string.
// Returns tool-specific statuses ("web", "coding") that activate existing
// but previously unused reaction variants in channel implementations.
func resolveToolReactionStatus(toolName string) string {
	switch {
	case strings.HasPrefix(toolName, "web") || toolName == "browser":
		return "web"
	case toolName == "exec":
		return "coding"
	default:
		return "tool"
	}
}
