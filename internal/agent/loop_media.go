package agent

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// responseMediaPattern matches "MEDIA:" followed by a non-whitespace path in response text.
var responseMediaPattern = regexp.MustCompile(`MEDIA:\S+`)

// parseMediaResult extracts a MediaResult from a tool result string containing "MEDIA:" prefix.
// Handles formats: "MEDIA:/path/to/file" and "[[audio_as_voice]]\nMEDIA:/path/to/file".
// Returns nil if no MEDIA: prefix is found.
//
// IMPORTANT: Only matches "MEDIA:" at the start of the (trimmed) string to avoid false
// positives when tool output contains "MEDIA:" in arbitrary text (e.g. a web page
// mentioning a commit message like "return MEDIA: path from screenshot").
func parseMediaResult(toolOutput string) *MediaResult {
	s := toolOutput
	asVoice := false

	// Check for [[audio_as_voice]] tag (TTS voice messages)
	if strings.Contains(s, "[[audio_as_voice]]") {
		asVoice = true
		s = strings.ReplaceAll(s, "[[audio_as_voice]]", "")
	}

	s = strings.TrimSpace(s)

	// Only match MEDIA: at the beginning of the string.
	if !strings.HasPrefix(s, "MEDIA:") {
		return nil
	}
	path := strings.TrimSpace(s[6:])
	if path == "" {
		return nil
	}
	// Take only the first line (in case there's trailing text)
	if nl := strings.IndexByte(path, '\n'); nl >= 0 {
		path = strings.TrimSpace(path[:nl])
	}

	return &MediaResult{
		Path:        path,
		ContentType: mimeFromExt(filepath.Ext(path)),
		AsVoice:     asVoice,
	}
}

// deduplicateMedia removes duplicate media results by path, keeping the first occurrence.
func deduplicateMedia(media []MediaResult) []MediaResult {
	if len(media) <= 1 {
		return media
	}
	seen := make(map[string]bool, len(media))
	result := make([]MediaResult, 0, len(media))
	for _, m := range media {
		if seen[m.Path] {
			continue
		}
		seen[m.Path] = true
		result = append(result, m)
	}
	return result
}

// mimeFromExt returns a MIME type for common media file extensions.
func mimeFromExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".mp4":
		return "video/mp4"
	case ".ogg", ".opus":
		return "audio/ogg"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".txt":
		return "text/plain"
	case ".pdf":
		return "application/pdf"
	case ".csv":
		return "text/csv"
	case ".json":
		return "application/json"
	case ".html", ".htm":
		return "text/html"
	case ".xml":
		return "application/xml"
	case ".zip":
		return "application/zip"
	case ".doc", ".docx":
		return "application/msword"
	case ".xls", ".xlsx":
		return "application/vnd.ms-excel"
	case ".md":
		return "text/markdown"
	default:
		return "application/octet-stream"
	}
}

// extractResponseMedia scans agent response text for embedded MEDIA: references,
// resolves container paths to host paths via the workspace mount mapping, and
// returns the cleaned text plus any valid media results.
// This prevents raw "MEDIA:/workspace/..." from leaking to channels when the
// agent includes media references in its response text instead of delivering
// files through the message tool or write_file(deliver=true).
func extractResponseMedia(content, workspace string) (string, []MediaResult) {
	if !strings.Contains(content, "MEDIA:") {
		return content, nil
	}

	const containerWorkdir = "/workspace"

	lines := strings.Split(content, "\n")
	var cleaned []string
	var media []MediaResult

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		matches := responseMediaPattern.FindAllString(trimmed, -1)
		if len(matches) == 0 {
			cleaned = append(cleaned, line)
			continue
		}

		for _, raw := range matches {
			path := strings.TrimSpace(raw[len("MEDIA:"):])
			if path == "" {
				continue
			}

			// Map container path /workspace/X → workspace/X on host.
			if workspace != "" && (strings.HasPrefix(path, containerWorkdir+"/") || path == containerWorkdir) {
				rel := strings.TrimPrefix(path, containerWorkdir)
				rel = strings.TrimPrefix(rel, "/")
				if rel != "" {
					path = filepath.Join(workspace, rel)
				}
			}

			// Verify file exists before adding as media result.
			if _, err := os.Stat(path); err == nil {
				media = append(media, MediaResult{
					Path:        path,
					ContentType: mimeFromExt(filepath.Ext(path)),
				})
			}
		}

		// Strip MEDIA: tokens from line, keep surrounding text.
		remainder := strings.TrimSpace(responseMediaPattern.ReplaceAllString(line, ""))
		if remainder != "" {
			cleaned = append(cleaned, remainder)
		}
	}

	return strings.TrimSpace(strings.Join(cleaned, "\n")), media
}
