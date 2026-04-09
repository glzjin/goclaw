package dingtalk

import (
	"log/slog"
	"strings"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
)

// handleInboundData converts DingTalk's generic BOT callback data into bus.InboundMessage
// and routes it through the standard channel lifecycle via HandleMessage.
func (c *Channel) handleInboundData(data *chatbot.BotCallbackDataModel) {
	if data == nil {
		return
	}

	senderID := data.SenderStaffId
	if senderID == "" {
		senderID = data.SenderId
	}

	isGroup := data.ConversationType == "2"

	var chatID string
	if isGroup {
		chatID = data.ConversationId // Use ConversationId for group context
		senderID = "group:dingtalk:" + chatID + ":" + senderID
	} else {
		chatID = data.ConversationId
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
		Content:   content,
		PeerKind:  peerKind,
		Metadata:  metadata,
		TenantID:  c.TenantID(),
	}

	// In the future: handle pairing rejection logic by sending a raw message back via c.SendRaw
	// Send raw string back if not paired (for now, simply PublishingInbound)
	c.msgBus.PublishInbound(msg)
}
