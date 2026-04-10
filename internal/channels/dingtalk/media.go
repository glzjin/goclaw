package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	dingtalkrobot_1_0 "github.com/alibabacloud-go/dingtalk/robot_1_0"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/nextlevelbuilder/goclaw/internal/bus"
)

// downloadMediaFiles fetches the media from DingTalk and saves it to a local temporary file.
func (c *Channel) downloadMediaFiles(ctx context.Context, downloadCode string) (string, string, error) {
	token, err := c.getAccessToken()
	if err != nil {
		return "", "", fmt.Errorf("failed to get token: %w", err)
	}

	headers := &dingtalkrobot_1_0.RobotMessageFileDownloadHeaders{}
	headers.XAcsDingtalkAccessToken = tea.String(token)
	request := &dingtalkrobot_1_0.RobotMessageFileDownloadRequest{
		DownloadCode: tea.String(downloadCode),
		RobotCode:    tea.String(c.cfg.ClientID),
	}

	response, err := c.robotCli.RobotMessageFileDownloadWithOptions(request, headers, &util.RuntimeOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to call RobotMessageFileDownload: %w", err)
	}
	if response.Body == nil || response.Body.DownloadUrl == nil {
		return "", "", fmt.Errorf("empty download url")
	}

	downloadUrl := *response.Body.DownloadUrl

	req, err := http.NewRequestWithContext(ctx, "GET", downloadUrl, nil)
	if err != nil {
		return "", "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("download failed with status %d", resp.StatusCode)
	}

	mimeType := resp.Header.Get("Content-Type")

	tmpFile, err := os.CreateTemp("", "dingtalk-media-*")
	if err != nil {
		return "", "", err
	}
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		os.Remove(tmpFile.Name())
		return "", "", err
	}

	return tmpFile.Name(), mimeType, nil
}

// uploadMedia payload to oapi.dingtalk.com/media/upload
func (c *Channel) uploadMedia(ctx context.Context, token, filePath, mediaType string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("media", filepath.Base(filePath))
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(part, file); err != nil {
		return "", err
	}

	if err := writer.WriteField("type", mediaType); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}

	url := "https://oapi.dingtalk.com/media/upload?access_token=" + token
	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var result struct {
		Errcode int    `json:"errcode"`
		Errmsg  string `json:"errmsg"`
		MediaID string `json:"media_id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("failed to parse upload response: %w", err)
	}
	if result.Errcode != 0 {
		return "", fmt.Errorf("dingtalk upload failed: [%d] %s", result.Errcode, result.Errmsg)
	}

	return result.MediaID, nil
}

// Send delivers an outbound message to the channel via the DingTalk Robot OpenAPI.
func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	token, err := c.getAccessToken()
	if err != nil {
		return fmt.Errorf("failed to get token: %w", err)
	}

	isGroup := strings.HasPrefix(msg.ChatID, "group:")
	targetChatID := strings.TrimPrefix(msg.ChatID, "group:")

	headers := &dingtalkrobot_1_0.OrgGroupSendHeaders{}
	headers.XAcsDingtalkAccessToken = tea.String(token)
	headersDM := &dingtalkrobot_1_0.BatchSendOTOHeaders{}
	headersDM.XAcsDingtalkAccessToken = tea.String(token)

	// Send Media Attachments First
	for _, m := range msg.Media {
		mediaType := "file"
		if strings.HasPrefix(m.ContentType, "image/") {
			mediaType = "image"
		} else if strings.HasPrefix(m.ContentType, "video/") {
			mediaType = "video"
		} else if strings.HasPrefix(m.ContentType, "audio/") {
			mediaType = "voice"
		}

		mediaId, err := c.uploadMedia(ctx, token, m.URL, mediaType)
		if err != nil {
			slog.Error("dingtalk: failed to upload media", "err", err, "path", m.URL)
			continue
		}

		var msgKey, msgParam string
		if mediaType == "image" {
			msgKey = "sampleImageMsg"
			msgParam = fmt.Sprintf(`{"photoURL": "%s"}`, mediaId)
		} else {
			msgKey = "sampleFile"
			fileName := filepath.Base(m.URL)
			if m.Caption != "" {
				fileName = m.Caption
			}
			fileType := strings.TrimPrefix(filepath.Ext(fileName), ".")
			if fileType == "" {
				fileType = "file"
			}
			// Dingtalk expects media_id, file_name, file_type
			msgParam = fmt.Sprintf(`{"media_id": "%s", "file_name": "%s", "file_type": "%s"}`, mediaId, fileName, fileType)
		}

		if isGroup {
			request := &dingtalkrobot_1_0.OrgGroupSendRequest{
				MsgKey:             tea.String(msgKey),
				MsgParam:           tea.String(msgParam),
				OpenConversationId: tea.String(targetChatID),
				RobotCode:          tea.String(c.cfg.ClientID),
			}
			_, err = c.robotCli.OrgGroupSendWithOptions(request, headers, &util.RuntimeOptions{})
		} else {
			request := &dingtalkrobot_1_0.BatchSendOTORequest{
				MsgKey:    tea.String(msgKey),
				MsgParam:  tea.String(msgParam),
				UserIds:   []*string{tea.String(targetChatID)},
				RobotCode: tea.String(c.cfg.ClientID),
			}
			_, err = c.robotCli.BatchSendOTOWithOptions(request, headersDM, &util.RuntimeOptions{})
		}
		if err != nil {
			slog.Error("dingtalk: failed to send media message", "err", err, "type", mediaType)
		} else {
			slog.Debug("dingtalk: media message sent", "type", mediaType)
		}
	}

	// Send Text Message If Present
	if msg.Content != "" {
		if isGroup {
			request := &dingtalkrobot_1_0.OrgGroupSendRequest{
				MsgKey:             tea.String("sampleMarkdown"),
				MsgParam:           tea.String(fmt.Sprintf(`{"title": "GoClaw", "text": %q}`, msg.Content)),
				OpenConversationId: tea.String(targetChatID),
				RobotCode:          tea.String(c.cfg.ClientID),
			}
			response, err := c.robotCli.OrgGroupSendWithOptions(request, headers, &util.RuntimeOptions{})
			if err != nil {
				slog.Error("dingtalk: failed to send group markdown message", "err", err)
				return err
			}
			slog.Debug("dingtalk: group message sent", "status", *response.StatusCode)
		} else {
			request := &dingtalkrobot_1_0.BatchSendOTORequest{
				MsgKey:    tea.String("sampleMarkdown"),
				MsgParam:  tea.String(fmt.Sprintf(`{"title": "GoClaw", "text": %q}`, msg.Content)),
				UserIds:   []*string{tea.String(targetChatID)},
				RobotCode: tea.String(c.cfg.ClientID),
			}
			response, err := c.robotCli.BatchSendOTOWithOptions(request, headersDM, &util.RuntimeOptions{})
			if err != nil {
				slog.Error("dingtalk: failed to send direct markdown message", "err", err)
				return err
			}
			slog.Debug("dingtalk: direct message sent", "status", *response.StatusCode)
		}
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

