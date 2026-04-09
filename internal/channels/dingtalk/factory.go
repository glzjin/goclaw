package dingtalk

import (
	"encoding/json"
	"fmt"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/channels"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	"github.com/nextlevelbuilder/goclaw/internal/store"
)

// dingTalkCreds maps the credentials JSON from the channel_instances table.
type dingTalkCreds struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

// dingTalkInstanceConfig maps the non-secret config JSONB from the channel_instances table.
type dingTalkInstanceConfig struct {
	AllowFrom      []string `json:"allow_from,omitempty"`
	DMPolicy       string   `json:"dm_policy,omitempty"`
	GroupPolicy    string   `json:"group_policy,omitempty"`
	RequireMention *bool    `json:"require_mention,omitempty"`
	BlockReply     *bool    `json:"block_reply,omitempty"`
}

// Factory creates a DingTalk channel from DB instance data.
func Factory(name string, creds json.RawMessage, cfg json.RawMessage,
	msgBus *bus.MessageBus, pairingSvc store.PairingStore) (channels.Channel, error) {

	var c dingTalkCreds
	if len(creds) > 0 {
		if err := json.Unmarshal(creds, &c); err != nil {
			return nil, fmt.Errorf("decode dingtalk credentials: %w", err)
		}
	}
	if c.ClientID == "" || c.ClientSecret == "" {
		return nil, fmt.Errorf("dingtalk client_id and client_secret are required")
	}

	var ic dingTalkInstanceConfig
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &ic); err != nil {
			return nil, fmt.Errorf("decode dingtalk config: %w", err)
		}
	}

	dtCfg := config.DingTalkConfig{
		Enabled:        true,
		ClientID:       c.ClientID,
		ClientSecret:   c.ClientSecret,
		AllowFrom:      ic.AllowFrom,
		DMPolicy:       ic.DMPolicy,
		GroupPolicy:    ic.GroupPolicy,
		RequireMention: ic.RequireMention,
		BlockReply:     ic.BlockReply,
	}

	// DB instances default to "pairing" for groups (secure by default).
	if dtCfg.GroupPolicy == "" {
		dtCfg.GroupPolicy = "pairing"
	}

	ch, err := New(dtCfg, msgBus, pairingSvc)
	if err != nil {
		return nil, err
	}

	ch.SetName(name)
	return ch, nil
}
