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

	isGroup := strings.HasPrefix(msg.ChatID, "group:")
	targetChatID := strings.TrimPrefix(msg.ChatID, "group:")

	if isGroup {
		headers := &dingtalkrobot_1_0.OrgGroupSendHeaders{}
		headers.XAcsDingtalkAccessToken = tea.String(token)
		request := &dingtalkrobot_1_0.OrgGroupSendRequest{
			MsgKey:             tea.String("sampleMarkdown"),
			MsgParam:           tea.String(fmt.Sprintf(`{"title": "GoClaw", "text": %q}`, msg.Content)),
			OpenConversationId: tea.String(targetChatID),
			RobotCode:          tea.String(c.cfg.ClientID),
		}

		response, err := c.robotCli.OrgGroupSendWithOptions(request, headers, &util.RuntimeOptions{})
		if err != nil {
			slog.Error("dingtalk: failed to send group message", "err", err)
			return err
		}
		slog.Debug("dingtalk: group message sent", "status", *response.StatusCode)
	} else {
		headers := &dingtalkrobot_1_0.BatchSendOTOHeaders{}
		headers.XAcsDingtalkAccessToken = tea.String(token)
		request := &dingtalkrobot_1_0.BatchSendOTORequest{
			MsgKey:    tea.String("sampleMarkdown"),
			MsgParam:  tea.String(fmt.Sprintf(`{"title": "GoClaw", "text": %q}`, msg.Content)),
			UserIds:   []*string{tea.String(targetChatID)},
			RobotCode: tea.String(c.cfg.ClientID),
		}

		response, err := c.robotCli.BatchSendOTOWithOptions(request, headers, &util.RuntimeOptions{})
		if err != nil {
			slog.Error("dingtalk: failed to send direct message", "err", err)
			return err
		}
		slog.Debug("dingtalk: direct message sent", "status", *response.StatusCode)
	}

	return nil
}

// SendRaw is a helper for directly sending raw strings (like authorized messages).
func (c *Channel) SendRaw(ctx context.Context, chatID string, txt string) error {
	return c.Send(ctx, bus.OutboundMessage{
		ChatID:  chatID,
		Content: txt,
	})
}
