package dingtalk

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	card_1_0 "github.com/alibabacloud-go/dingtalk/card_1_0"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/channels"
)

// StreamEnabled returns true if the channel is configured to use AI Streaming Cards.
func (c *Channel) StreamEnabled(isGroup bool) bool {
	return c.cfg.ReplyStyle == "stream_card" && c.cfg.CardTemplateID != ""
}

// ReasoningStreamEnabled disables a separate reasoning message, just putting it directly.
func (c *Channel) ReasoningStreamEnabled() bool {
	return false
}

// CreateStream initializes an AI Streaming Card for the DingTalk conversation.
func (c *Channel) CreateStream(ctx context.Context, chatID string, firstStream bool) (channels.ChannelStream, error) {
	if !c.StreamEnabled(strings.HasPrefix(chatID, "group:")) {
		return nil, channels.ErrNotImplemented
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
		UserIdType:     tea.String("staffId"),
		CardTemplateId: tea.String(c.cfg.CardTemplateID),
		OutTrackId:     tea.String(outTrackID),
		CardData: &card_1_0.CreateAndDeliverRequestCardData{
			CardParamMap: map[string]*string{
				"content": tea.String("思考中..."), // Initial loading text
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
		req.ReceiverUserIdList = []*string{tea.String(chatID)}
		req.ImRobotOpenSpaceModel = &card_1_0.CreateAndDeliverRequestImRobotOpenSpaceModel{
			SupportForward: tea.Bool(true),
		}
	}

	headers := &card_1_0.CreateAndDeliverHeaders{}
	headers.SetXAcsDingtalkAccessToken(token)

	// Send the initial card
	_, deliverErr := cardClient.CreateAndDeliverWithOptions(req, headers, &openapi.RuntimeOptions{})
	if deliverErr != nil {
		slog.Error("dingtalk: failed to deliver stream card", "error", deliverErr)
		return nil, deliverErr
	}

	return &dingCardStream{
		channel:    c,
		cardClient: cardClient,
		outTrackID: outTrackID,
		token:      token,
	}, nil
}

// FinalizeStream passes the final message payload.
// For DingTalk cards, the card stream updates in place and there's no platform message ID returned.
func (c *Channel) FinalizeStream(ctx context.Context, chatID string, stream channels.ChannelStream) {}

type dingCardStream struct {
	channel    *Channel
	cardClient *card_1_0.Client
	outTrackID string
	token      string
}

func (s *dingCardStream) Update(ctx context.Context, text string) {
	if text == "" {
		return
	}

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

	_, err := s.cardClient.StreamingUpdateWithOptions(req, headers, &openapi.RuntimeOptions{})
	if err != nil {
		slog.Warn("dingtalk: streaming update failed", "error", err)
	}
}

func (s *dingCardStream) Stop(ctx context.Context) error {
	// Send finalize signal
	req := &card_1_0.StreamingUpdateRequest{
		OutTrackId: tea.String(s.outTrackID),
		Guid:       tea.String(uuid.New().String()),
		Key:        tea.String("content"),
		Content:    tea.String(""), // Content isn't applied when IsFinalize is true usually, or pass last content.
		IsFull:     tea.Bool(true),
		IsFinalize: tea.Bool(true),
	}

	headers := &card_1_0.StreamingUpdateHeaders{}
	headers.SetXAcsDingtalkAccessToken(s.token)

	_, err := s.cardClient.StreamingUpdateWithOptions(req, headers, &openapi.RuntimeOptions{})
	if err != nil {
		slog.Warn("dingtalk: streaming finalize failed", "error", err)
	}
	return nil
}

func (s *dingCardStream) MessageID() int {
	return 0 // DingTalk streaming cards don't map to a numeric message ID inside GoCLAW
}
