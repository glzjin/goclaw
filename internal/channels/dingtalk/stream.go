package dingtalk

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

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
			req.OpenSpaceId = tea.String("dtv1.card//IM_GROUP." + parts[1])
			req.ImGroupOpenSpaceModel = &card_1_0.CreateAndDeliverRequestImGroupOpenSpaceModel{
				SupportForward: tea.Bool(true),
			}
			req.ImGroupOpenDeliverModel = &card_1_0.CreateAndDeliverRequestImGroupOpenDeliverModel{
				RobotCode: tea.String(c.cfg.ClientID),
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
		throttle:   1000 * time.Millisecond,
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
	
	mu       sync.Mutex
	stopped  bool
	lastText string
	pending  string
	lastEdit time.Time
	throttle time.Duration
}

func (s *dingCardStream) Update(ctx context.Context, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped || text == "" {
		return
	}

	// Dedup
	if text == s.lastText {
		return
	}
	s.pending = text

	// Throttle check
	if time.Since(s.lastEdit) < s.throttle {
		return
	}

	s.flush(ctx)
}

// flush does the actual HTTP call. Caller must hold mu.
// Uses incremental append mode (IsFull=false) to send only new content
// since the last successful update. This avoids the rendering flicker
// that occurs when DingTalk replaces the entire card on each update.
func (s *dingCardStream) flush(ctx context.Context) {
	if s.pending == "" || s.pending == s.lastText {
		return
	}
	text := s.pending

	// Compute delta: only send content added since last successful update.
	delta := text
	if len(s.lastText) < len(text) {
		delta = text[len(s.lastText):]
	}
	if delta == "" {
		return
	}

	req := &card_1_0.StreamingUpdateRequest{
		OutTrackId: tea.String(s.outTrackID),
		Guid:       tea.String(uuid.New().String()),
		Key:        tea.String("content"),
		Content:    tea.String(delta),
		IsFull:     tea.Bool(false),
		IsFinalize: tea.Bool(false),
	}

	headers := &card_1_0.StreamingUpdateHeaders{}
	headers.SetXAcsDingtalkAccessToken(s.token)

	// Release lock to avoid blocking other Update/Stop calls while HTTP is pending
	s.mu.Unlock()
	_, err := s.cardClient.StreamingUpdateWithOptions(req, headers, &util.RuntimeOptions{})
	s.mu.Lock()

	if err != nil {
		slog.Warn("dingtalk: streaming update failed", "error", err)
		return
	}
	s.lastText = text
	s.lastEdit = time.Now()
}

func (s *dingCardStream) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopped = true
	finalContent := s.pending
	if finalContent == "" {
		finalContent = s.lastText
	}
	if finalContent == "" {
		finalContent = "..."
	}

	req := &card_1_0.StreamingUpdateRequest{
		OutTrackId: tea.String(s.outTrackID),
		Guid:       tea.String(uuid.New().String()),
		Key:        tea.String("content"),
		Content:    tea.String(finalContent),
		IsFull:     tea.Bool(true),
		IsFinalize: tea.Bool(true),
	}

	headers := &card_1_0.StreamingUpdateHeaders{}
	headers.SetXAcsDingtalkAccessToken(s.token)

	s.mu.Unlock()
	_, err := s.cardClient.StreamingUpdateWithOptions(req, headers, &util.RuntimeOptions{})
	s.mu.Lock()

	if err != nil {
		slog.Warn("dingtalk: streaming finalize failed", "error", err)
	}
	return err
}

func (s *dingCardStream) MessageID() int {
	return 0 // DingTalk streaming cards don't map to a numeric message ID inside GoCLAW
}
