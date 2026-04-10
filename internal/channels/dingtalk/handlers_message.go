package dingtalk

import (
	"context"
	"log/slog"
	"strings"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
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

	isGroup := data.ConversationType == "2"

	var chatID string
	if isGroup {
		chatID = "group:" + data.ConversationId // Use ConversationId for group context
		senderID = "group:dingtalk:" + data.ConversationId + ":" + senderID
	} else {
		chatID = senderID
	}

	content := strings.TrimSpace(data.Text.Content)

	if content == "" {
		slog.Debug("dingtalk: rejecting empty message or unsupported type", "type", data.Msgtype)
		return
	}

	// Filter based on require_mention in groups
	if isGroup {
		// Basic mention check: either the bot is explicitly @-mentioned, or the message relies on being replied to the bot
		require := c.cfg.RequireMention
		if require == nil || *require {
			// Check if bot's staff ID or username is in AtUsers
			isMentioned := false
			for _, user := range data.AtUsers {
				if user.StaffId == data.ChatbotUserId {
					isMentioned = true
					break
				}
			}
			
			if !isMentioned {
				slog.Debug("dingtalk: ignoring unmentioned group message", "chat", chatID)
				return
			}
		}
	}

	metadata := map[string]string{
		"msg_id":    data.MsgId,
		"tenant_id": data.ChatbotCorpId,
		"msg_type":  data.Msgtype,
		"sender_nick": data.SenderNick,
	}

	if isGroup {
		metadata["group_name"] = data.ConversationTitle
	}

	peerKind := "direct"
	if isGroup {
		peerKind = "group"
	}

	msg := bus.InboundMessage{
		Channel:   c.Name(),
		ChatID:    chatID,
		SenderID:  senderID,
		UserID:    senderID,
		Content:   content,
		PeerKind:  peerKind,
		Metadata:  metadata,
		TenantID:  c.TenantID(),
		AgentID:   c.AgentID(),
	}

	// Evaluate DM / Group policies (pairing / allowlist / open / disabled)
	if isGroup && !c.checkGroupPolicy(context.Background(), senderID, chatID) {
		slog.Debug("dingtalk: group message rejected by policy", "chat", chatID, "sender", senderID)
		return
	} else if !isGroup && !c.checkDMPolicy(context.Background(), senderID, chatID) {
		slog.Debug("dingtalk: direct message rejected by policy", "chat", chatID, "sender", senderID)
		return
	}

	c.msgBus.PublishInbound(msg)
}
