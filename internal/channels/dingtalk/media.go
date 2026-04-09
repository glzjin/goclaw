package dingtalk

import (
	"context"
	"fmt"
	"log/slog"

	dingtalkrobot_1_0 "github.com/alibabacloud-go/dingtalk/robot_1_0"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
)

// Send delivers an outbound message to the channel via the DingTalk Robot OpenAPI.
func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	token, err := c.getAccessToken()
	if err != nil {
		return fmt.Errorf("failed to get token: %w", err)
	}

	headers := &dingtalkrobot_1_0.OrgGroupSendHeaders{}
	headers.XAcsDingtalkAccessToken = tea.String(token)

	// Determine if we need Markdown based on formatting
	request := &dingtalkrobot_1_0.OrgGroupSendRequest{
		MsgKey:    tea.String("sampleMarkdown"),
		MsgParam:  tea.String(fmt.Sprintf(`{"title": "GoClaw", "text": %q}`, msg.Content)), // Requires JSON representation safely
		OpenConversationId: tea.String(msg.ChatID),
		RobotCode:          tea.String(c.cfg.ClientID),
	}
	
	// Fallback to plain text if needed: msgKey: "sampleText", msgParam: {"content": "..."}
	request.MsgKey = tea.String("sampleText")
	request.MsgParam = tea.String(fmt.Sprintf(`{"content": %q}`, msg.Content))

	response, err := c.robotCli.OrgGroupSendWithOptions(request, headers, &util.RuntimeOptions{})
	if err != nil {
		slog.Error("dingtalk: failed to send message", "err", err)
		return err
	}

	slog.Debug("dingtalk: message sent", "status", *response.StatusCode)
	return nil
}

// SendRaw is a helper for directly sending raw strings (like authorized messages).
func (c *Channel) SendRaw(ctx context.Context, chatID string, txt string) error {
	return c.Send(ctx, bus.OutboundMessage{
		ChatID:  chatID,
		Content: txt,
	})
}
