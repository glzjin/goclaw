package dingtalk

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/channels/media"
)

// handleInboundData converts DingTalk's generic BOT callback data into bus.InboundMessage
// and routes it through the standard channel lifecycle via HandleMessage.
func (c *Channel) handleInboundData(data *chatbot.BotCallbackDataModel) {
	if data == nil {
		slog.Warn("dingtalk: received nil callback data")
		return
	}
	slog.Info("dingtalk: received inbound raw stream payload", "sender", data.SenderId, "type", data.Msgtype, "content", data.Text.Content)

	senderID := data.SenderStaffId
	if senderID == "" {
		senderID = data.SenderId
	}
	// rawSenderID is the platform user identity, consistent across DM and group contexts.
	// Used for contact recording so the same person is not duplicated per group.
	rawSenderID := senderID

	isGroup := data.ConversationType == "2"

	var chatID string
	if isGroup {
		chatID = "group:" + data.ConversationId // Use ConversationId for group context
		senderID = "group:dingtalk:" + data.ConversationId + ":" + senderID
	} else {
		chatID = senderID
	}

	content := strings.TrimSpace(data.Text.Content)
	senderLabel := data.SenderNick
	if senderLabel == "" {
		senderLabel = "User"
	}

	var wasMentioned bool
	if isGroup {
		require := c.cfg.RequireMention
		if require == nil || *require {
			for _, user := range data.AtUsers {
				if (user.StaffId != "" && user.StaffId == data.ChatbotUserId) ||
					(user.DingtalkId != "" && user.DingtalkId == data.ChatbotUserId) {
					wasMentioned = true
					break
				}
			}
		} else {
			wasMentioned = true
		}
	} else {
		wasMentioned = true
	}

	var mediaFiles []bus.MediaFile
	var extraContent string

	isMedia := data.Msgtype == "picture" || data.Msgtype == "file" || data.Msgtype == "audio" || data.Msgtype == "video" || data.Msgtype == "richText"

	// Media downloaded immediately here if it has downloadCode
	if isMedia {
		if contentMap, ok := data.Content.(map[string]interface{}); ok {

			// Debug: display all metadata for file
			if data.Msgtype == "file" || data.Msgtype == "audio" || data.Msgtype == "video" {
				slog.Info("dingtalk: media content inspected", "msgtype", data.Msgtype, "content", data.Content)
			}

			dlCode, _ := contentMap["downloadCode"].(string)

			// For richText, sometimes downloadCode is nested inside array elements.
			if dlCode == "" {
				if richTextArr, ok := contentMap["richText"].([]interface{}); ok {
					for _, item := range richTextArr {
						if rMap, ok := item.(map[string]interface{}); ok {
							if innerDlCode, ok := rMap["downloadCode"].(string); ok && innerDlCode != "" {
								dlCode = innerDlCode
								break
							}
						}
					}
				}
			}

			if dlCode != "" {
				// Only download full media if it was mentioned or we don't care about memory limits
				tmpPath, mime, err := c.downloadMediaFiles(context.Background(), dlCode)
				if err != nil {
					slog.Error("dingtalk: failed to download media", "err", err, "downloadCode", dlCode)
				} else {
					fName, _ := contentMap["fileName"].(string)
					// Rename temp file if fileName is provided in contentMap to help document parser
					if fName != "" && !strings.Contains(tmpPath, ".") {
						if ext := filepath.Ext(fName); ext != "" {
							newPath := tmpPath + ext
							if err := os.Rename(tmpPath, newPath); err == nil {
								tmpPath = newPath
							}
						}
					}

					mediaFiles = append(mediaFiles, bus.MediaFile{
						Path:     tmpPath,
						MimeType: mime,
					})

					// Native Media Pre-Extraction Engine
					if data.Msgtype == "audio" || data.Msgtype == "voice" {
						transcript, sttErr := media.TranscribeAudio(context.Background(), media.STTConfig{
							ProxyURL:       c.cfg.STTProxyURL,
							APIKey:         c.cfg.STTAPIKey,
							TenantID:       c.cfg.STTTenantID,
							TimeoutSeconds: c.cfg.STTTimeoutSeconds,
						}, tmpPath)
						if sttErr != nil {
							slog.Warn("dingtalk: STT transcription failed", "error", sttErr)
						} else if transcript != "" {
							extraContent += "\n\n[语音内容识别: " + transcript + "]"
						}
					} else if data.Msgtype == "file" && fName != "" {
						docText, docErr := media.ExtractDocumentContent(tmpPath, fName)
						if docErr != nil {
							slog.Warn("dingtalk: document extraction failed", "file", fName, "error", docErr)
							// Fallback to reference exposure
							extraContent += "\n\n[收到大文件或未能解析文档: " + fName + ", 内部路径: " + tmpPath + "]"
						} else if docText != "" {
							extraContent += "\n\n[从文件中提取的文本(" + fName + "):\n" + docText + "\n]"
						}
					} else if data.Msgtype == "picture" {
						extraContent += "\n\n[收到图片: " + tmpPath + "]"
					}
				}
			}
		}
	}

	if extraContent != "" {
		if content == "" {
			content = strings.TrimSpace(extraContent)
		} else {
			content = content + extraContent
		}
	}

	if content == "" && len(mediaFiles) == 0 {
		slog.Debug("dingtalk: rejecting empty message or unsupported type", "type", data.Msgtype)
		return
	}

	// Filter based on require_mention in groups
	if isGroup && !wasMentioned {
		// Group History caching for unmentioned messages
		// Note: we inject a lightweight tag to represent media
		lightTag := ""
		if data.Msgtype == "picture" {
			lightTag = "[sent an image]"
		} else if data.Msgtype == "file" {
			lightTag = "[sent a file]"
		} else if data.Msgtype == "audio" {
			lightTag = "[sent audio]"
		} else if data.Msgtype == "video" {
			lightTag = "[sent a video]"
		}

		histContent := content
		if lightTag != "" {
			histContent = lightTag + "\n" + histContent
		}
		if histContent == "" {
			histContent = "[empty message]"
		}

		c.GroupHistory().Record(chatID, channels.HistoryEntry{
			Sender:    senderLabel,
			SenderID:  senderID,
			Body:      histContent,
			MediaRefs: nil,
			Timestamp: time.UnixMilli(data.CreateAt),
			MessageID: data.MsgId,
		}, c.HistoryLimit())

		slog.Debug("dingtalk: recorded unmentioned group message", "chat", chatID, "sender", senderLabel)

		// Record contact quietly — use rawSenderID so the same person
		// is not duplicated across groups and DMs.
		if cc := c.ContactCollector(); cc != nil {
			cc.EnsureContact(context.Background(), c.Type(), c.Name(), rawSenderID, data.SenderId, senderLabel, "", "group", "user", "", "")
			cc.EnsureContact(context.Background(), c.Type(), c.Name(), chatID, "", data.ConversationTitle, "", "group", "group", "", "")
		}
		return
	}

	// Message was mentioned or is DM
	finalContent := content
	if finalContent == "" {
		finalContent = "[empty message]"
	}

	// Build context from history if group
	if isGroup {
		annotated := "[From: " + senderLabel + "]\n" + finalContent
		if c.HistoryLimit() > 0 {
			finalContent = c.GroupHistory().BuildContext(chatID, annotated, c.HistoryLimit())
		} else {
			finalContent = annotated
		}
	} else {
		finalContent = "[From: " + senderLabel + "]\n" + finalContent
	}

	metadata := map[string]string{
		"msg_id":      data.MsgId,
		"tenant_id":   data.ChatbotCorpId,
		"msg_type":    data.Msgtype,
		"sender_nick": data.SenderNick,
	}

	if isGroup {
		metadata["group_name"] = data.ConversationTitle
	}

	peerKind := "direct"
	if isGroup {
		peerKind = "group"
	}

	// Evaluate DM / Group policies (pairing / allowlist / open / disabled)
	if isGroup && !c.checkGroupPolicy(context.Background(), senderID, chatID) {
		slog.Debug("dingtalk: group message rejected by policy", "chat", chatID, "sender", senderID)
		return
	} else if !isGroup && !c.checkDMPolicy(context.Background(), senderID, chatID) {
		slog.Debug("dingtalk: direct message rejected by policy", "chat", chatID, "sender", senderID)
		return
	}

	// Collect contact for processed messages — use rawSenderID so the
	// same person is not duplicated across groups and DMs.
	if cc := c.ContactCollector(); cc != nil {
		cc.EnsureContact(context.Background(), c.Type(), c.Name(), rawSenderID, data.SenderId, senderLabel, "", peerKind, "user", "", "")
		if isGroup {
			cc.EnsureContact(context.Background(), c.Type(), c.Name(), chatID, "", data.ConversationTitle, "", "group", "group", "", "")
		}
	}

	msg := bus.InboundMessage{
		Channel:  c.Name(),
		ChatID:   chatID,
		SenderID: senderID,
		UserID:   data.SenderId,
		Content:  finalContent,
		Media:    mediaFiles,
		PeerKind: peerKind,
		Metadata: metadata,
		TenantID: c.TenantID(),
		AgentID:  c.AgentID(),
	}

	c.msgBus.PublishInbound(msg)

	// Clear history after publishing
	if isGroup {
		c.GroupHistory().Clear(chatID)
	}
}
