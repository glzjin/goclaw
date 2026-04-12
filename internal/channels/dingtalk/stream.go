package dingtalk

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	card_1_0 "github.com/alibabacloud-go/dingtalk/card_1_0"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/channels"
)

func (c *Channel) StreamEnabled(isGroup bool) bool {
	slog.Info("dingtalk: checking stream enabled", "reply_style", c.cfg.ReplyStyle, "tmpl", c.cfg.CardTemplateID)
	return c.cfg.ReplyStyle == "stream_card" && c.cfg.CardTemplateID != ""
}

// ReasoningStreamEnabled disables a separate reasoning message, just putting it directly.
func (c *Channel) ReasoningStreamEnabled() bool {
	return false
}

// CreateStream initializes an AI Streaming Card for the DingTalk conversation.
func (c *Channel) CreateStream(ctx context.Context, chatID string, firstStream bool) (channels.ChannelStream, error) {
	if !c.StreamEnabled(strings.HasPrefix(chatID, "group:")) {
		return nil, fmt.Errorf("stream mode not enabled for dingtalk channel")
	}

	token, err := c.getAccessToken()
	if err != nil {
		return nil, fmt.Errorf("failed to get access token for stream card: %v", err)
	}

	cardClient, err := card_1_0.NewClient(&openapi.Config{
		Protocol: tea.String("https"),
		RegionId: tea.String("central"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init card client: %v", err)
	}

	outTrackID := uuid.New().String()

	req := &card_1_0.CreateAndDeliverRequest{
		UserIdType:     tea.Int32(1), // 1 is staffId
		CardTemplateId: tea.String(c.cfg.CardTemplateID),
		OutTrackId:     tea.String(outTrackID),
		CardData: &card_1_0.CreateAndDeliverRequestCardData{
			CardParamMap: map[string]*string{
				"content": tea.String("..."),
			},
		},
	}

	isGroup := strings.HasPrefix(chatID, "group:")
	if isGroup {
		// ChatID is "group:<ConversationId>"
		parts := strings.SplitN(chatID, ":", 2)
		if len(parts) == 2 {
			req.OpenSpaceId = tea.String(parts[1])
			req.ImGroupOpenSpaceModel = &card_1_0.CreateAndDeliverRequestImGroupOpenSpaceModel{
				SupportForward: tea.Bool(true),
			}
		}
	} else {
		// ChatID for DM is SenderID
		req.UserId = tea.String(chatID)
		req.CallbackType = tea.String("STREAM")
		req.OpenSpaceId = tea.String("dtv1.card//im_robot." + chatID)
		req.ImRobotOpenSpaceModel = &card_1_0.CreateAndDeliverRequestImRobotOpenSpaceModel{
			SupportForward: tea.Bool(true),
			LastMessageI18n: map[string]*string{
				"ZH_CN": tea.String("..."),
			},
		}
		req.ImRobotOpenDeliverModel = &card_1_0.CreateAndDeliverRequestImRobotOpenDeliverModel{
			SpaceType: tea.String("IM_ROBOT"), // Mandatory per documentation
			RobotCode: tea.String(c.cfg.ClientID),
		}
	}

	headers := &card_1_0.CreateAndDeliverHeaders{}
	headers.SetXAcsDingtalkAccessToken(token)

	// Send the initial card
	res, deliverErr := cardClient.CreateAndDeliverWithOptions(req, headers, &util.RuntimeOptions{})
	if deliverErr != nil {
		slog.Error("dingtalk: failed to deliver stream card", "error", deliverErr)
		return nil, deliverErr
	}
	
	status := int32(0)
	if res != nil && res.StatusCode != nil {
		status = *res.StatusCode
	}
	slog.Info("dingtalk: stream card created", "status", status, "body", res.Body)

	return &dingCardStream{
		channel:    c,
		cardClient: cardClient,
		outTrackID: outTrackID,
		token:      token,
	}, nil
}

// FinalizeStream passes the final message payload.
// For DingTalk cards, the card stream updates in place and there's no platform message ID returned.
func (c *Channel) FinalizeStream(ctx context.Context, chatID string, stream channels.ChannelStream) {
	c.streamDedup.Store(chatID, true)
}

type dingCardStream struct {
	channel    *Channel
	cardClient *card_1_0.Client
	outTrackID string
	token      string
	lastText   string
}

func (s *dingCardStream) Update(ctx context.Context, text string) {
	if text == "" {
		return
	}
	s.lastText = text

	req := &card_1_0.StreamingUpdateRequest{
		OutTrackId: tea.String(s.outTrackID),
		Guid:       tea.String(uuid.New().String()), // Each update needs a unique GUID
		Key:        tea.String("content"),
		Content:    tea.String(text),
		IsFull:     tea.Bool(true),
		IsFinalize: tea.Bool(false),
	}

	headers := &card_1_0.StreamingUpdateHeaders{}
	headers.SetXAcsDingtalkAccessToken(s.token)

	_, err := s.cardClient.StreamingUpdateWithOptions(req, headers, &util.RuntimeOptions{})
	if err != nil {
		slog.Warn("dingtalk: streaming update failed", "error", err)
	}
}

func (s *dingCardStream) Stop(ctx context.Context) error {
	finalContent := s.lastText
	isFull := true
	
	if finalContent == "" {
		finalContent = "..."
	}

	// Send finalize signal
	req := &card_1_0.StreamingUpdateRequest{
		OutTrackId: tea.String(s.outTrackID),
		Guid:       tea.String(uuid.New().String()),
		Key:        tea.String("content"), // Required by OpenAPI schema
		Content:    tea.String(finalContent), // Required by OpenAPI schema
		IsFull:     tea.Bool(isFull), // Safest to use full redraw, unless empty to prevent clearing
		IsFinalize: tea.Bool(true),
	}

	headers := &card_1_0.StreamingUpdateHeaders{}
	headers.SetXAcsDingtalkAccessToken(s.token)

	_, err := s.cardClient.StreamingUpdateWithOptions(req, headers, &util.RuntimeOptions{})
	if err != nil {
		slog.Warn("dingtalk: streaming finalize failed", "error", err)
	}
	return nil
}

func (s *dingCardStream) MessageID() int {
	return 0 // DingTalk streaming cards don't map to a numeric message ID inside GoCLAW
}
