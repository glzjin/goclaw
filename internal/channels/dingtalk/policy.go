package dingtalk

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
)

const pairingDebounceTime = 60 * time.Second

// checkGroupPolicy evaluates security policy for group messages.
func (c *Channel) checkGroupPolicy(ctx context.Context, senderID, chatID string) bool {
	groupPolicy := c.cfg.GroupPolicy
	if groupPolicy == "" {
		groupPolicy = "open"
	}

	switch groupPolicy {
	case "disabled":
		return false
	case "allowlist":
		if c.IsAllowed(senderID) {
			return true
		}
		return false
	case "pairing":
		// Allowlist bypass (per-user)
		if c.HasAllowList() && c.IsAllowed(senderID) {
			return true
		}

		// Group-level pairing (one approval per group)
		if _, cached := c.approvedGroups.Load(chatID); cached {
			return true
		}
		groupSenderID := chatID // chatID already has "group:" prefix for groups in dingtalk
		if c.pairingSvc != nil {
			paired, err := c.pairingSvc.IsPaired(ctx, groupSenderID, c.Name())
			if err != nil {
				slog.Warn("security.pairing_check_failed, assuming paired (fail-open)",
					"group_sender", groupSenderID, "channel", c.Name(), "error", err)
				paired = true
			}
			if paired {
				c.approvedGroups.Store(chatID, true)
				return true
			}
		}
		c.sendPairingReply(ctx, groupSenderID, chatID)
		return false
	default: // "open"
		return true
	}
}

// checkDMPolicy evaluates security policy for direct messages.
func (c *Channel) checkDMPolicy(ctx context.Context, senderID, chatID string) bool {
	dmPolicy := c.cfg.DMPolicy
	if dmPolicy == "" {
		dmPolicy = "pairing"
	}

	switch dmPolicy {
	case "disabled":
		slog.Debug("dingtalk DM rejected: disabled", "sender_id", senderID)
		return false
	case "open":
		return true
	case "allowlist":
		if !c.IsAllowed(senderID) {
			slog.Debug("dingtalk DM rejected by allowlist", "sender_id", senderID)
			return false
		}
		return true
	default: // "pairing"
		paired := false
		if c.pairingSvc != nil {
			p, err := c.pairingSvc.IsPaired(ctx, senderID, c.Name())
			if err != nil {
				slog.Warn("security.pairing_check_failed, assuming paired (fail-open)",
					"sender_id", senderID, "channel", c.Name(), "error", err)
				paired = true
			} else {
				paired = p
			}
		}
		inAllowList := c.HasAllowList() && c.IsAllowed(senderID)

		if paired || inAllowList {
			return true
		}

		c.sendPairingReply(ctx, senderID, chatID)
		return false
	}
}

func (c *Channel) sendPairingReply(ctx context.Context, senderID, chatID string) {
	if c.pairingSvc == nil {
		return
	}

	// Debounce
	if lastSent, ok := c.pairingDebounce.Load(senderID); ok {
		if t, ok := lastSent.(time.Time); ok && time.Since(t) < pairingDebounceTime {
			return
		}
	}

	code, err := c.pairingSvc.RequestPairing(ctx, senderID, c.Name(), chatID, "default", nil)
	if err != nil {
		slog.Debug("dingtalk pairing request failed", "sender_id", senderID, "error", err)
		return
	}

	replyText := fmt.Sprintf(
		"GoClaw: 尚未配置访问权限。\n\n你的 DingTalk ID: %s\n\n配对验证码: %s\n\n请联系机器人管理员执行以下命令来审核通过:\n  goclaw pairing approve %s",
		senderID, code, code,
	)

	// Route internal pairing message out via the channel Send method.
	msg := bus.OutboundMessage{
		Channel:   c.Name(),
		ChatID:    chatID,
		Content:   replyText,
	}
	
	if err := c.Send(ctx, msg); err != nil {
		slog.Warn("failed to send dingtalk pairing reply", "error", err)
	} else {
		c.pairingDebounce.Store(senderID, time.Now())
		slog.Info("dingtalk pairing reply sent", "sender_id", senderID, "code", code)
	}
}
