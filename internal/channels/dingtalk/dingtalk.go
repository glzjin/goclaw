package dingtalk

import (
	"context"
	"fmt"
	"sync"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	dingtalkoauth2_1_0 "github.com/alibabacloud-go/dingtalk/oauth2_1_0"
	dingtalkrobot_1_0 "github.com/alibabacloud-go/dingtalk/robot_1_0"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/client"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// Channel implements channels.Channel for DingTalk.
type Channel struct {
	channels.BaseChannel

	cfg        config.DingTalkConfig
	msgBus     *bus.MessageBus
	pairingSvc store.PairingStore

	streamCli *client.StreamClient
	oauthCli  *dingtalkoauth2_1_0.Client
	robotCli  *dingtalkrobot_1_0.Client

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
}

// New creates a new DingTalk channel.
func New(cfg config.DingTalkConfig, msgBus *bus.MessageBus, pairingSvc store.PairingStore) (*Channel, error) {
	if cfg.ClientID == "" || cfg.ClientSecret == "" {
		return nil, fmt.Errorf("dingtalk client_id and client_secret are required")
	}

	c := &Channel{
		BaseChannel: *channels.NewBaseChannel(channels.TypeDingTalk, msgBus, cfg.AllowFrom),
		cfg:         cfg,
		msgBus:      msgBus,
		pairingSvc:  pairingSvc,
	}

	return c, nil
}

// Type returns "dingtalk".
func (c *Channel) Type() string {
	return channels.TypeDingTalk
}

// Start begins processing inbound events and establishes the stream connection.
func (c *Channel) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = true

	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Unlock()

	// Initialize OpenAPI clients for sending messages
	oauthConfig := &openapi.Config{
		Protocol: tea.String("https"),
		RegionId: tea.String("central"),
	}
	oauthCli, err := dingtalkoauth2_1_0.NewClient(oauthConfig)
	if err != nil {
		return fmt.Errorf("failed to create oauth client: %w", err)
	}
	c.oauthCli = oauthCli

	robotConfig := &openapi.Config{
		Protocol: tea.String("https"),
		RegionId: tea.String("central"),
	}
	robotCli, err := dingtalkrobot_1_0.NewClient(robotConfig)
	if err != nil {
		return fmt.Errorf("failed to create robot client: %w", err)
	}
	c.robotCli = robotCli

	// Initialize Stream client
	streamCli := client.NewStreamClient(
		client.WithAppCredential(client.NewAppCredentialConfig(c.cfg.ClientID, c.cfg.ClientSecret)),
	)
	c.streamCli = streamCli

	// Register ChatBot callback
	streamCli.RegisterChatBotCallbackRouter(func(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
		c.handleInboundData(data)
		return []byte("{}"), nil
	})

	err = streamCli.Start(runCtx)
	if err != nil {
		return fmt.Errorf("failed to start dingtalk stream: %w", err)
	}

	c.MarkHealthy("Stream connected")
	return nil
}

// Stop gracefully shuts down the channel.
func (c *Channel) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return nil
	}
	c.running = true
	if c.cancel != nil {
		c.cancel()
	}
	if c.streamCli != nil {
		c.streamCli.Close()
	}
	c.MarkStopped("Stream closed")
	return nil
}

// IsRunning returns whether the channel is actively connected via Stream SDK.
func (c *Channel) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *Channel) getAccessToken() (string, error) {
	req := &dingtalkoauth2_1_0.GetAccessTokenRequest{
		AppKey:    tea.String(c.cfg.ClientID),
		AppSecret: tea.String(c.cfg.ClientSecret),
	}
	resp, err := c.oauthCli.GetAccessToken(req)
	if err != nil {
		return "", err
	}
	if resp.Body == nil || resp.Body.AccessToken == nil {
		return "", fmt.Errorf("failed to verify access token")
	}
	return *resp.Body.AccessToken, nil
}
