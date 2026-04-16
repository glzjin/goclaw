package tools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/sandbox"
)

// DeliverFileTool delivers an existing file from the workspace to the user
// as an attachment. Unlike write_file (which creates new files), this tool
// sends files that already exist — e.g. output from exec, previously created
// archives, or downloaded artifacts.
type DeliverFileTool struct {
	workspace  string
	restrict   bool
	sandboxMgr sandbox.Manager
}

func NewDeliverFileTool(workspace string, restrict bool) *DeliverFileTool {
	return &DeliverFileTool{workspace: workspace, restrict: restrict}
}

func NewSandboxedDeliverFileTool(workspace string, restrict bool, mgr sandbox.Manager) *DeliverFileTool {
	return &DeliverFileTool{workspace: workspace, restrict: restrict, sandboxMgr: mgr}
}

func (t *DeliverFileTool) Name() string { return "deliver_file" }
func (t *DeliverFileTool) Description() string {
	return "Deliver an existing file from the workspace to the user as an attachment. " +
		"Use this for files already created by exec or other tools (e.g. zip archives, generated PDFs, images). " +
		"Unlike write_file, this does NOT create or modify files — it sends an existing file."
}
func (t *DeliverFileTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{
				"type":        "string",
				"description": "Path to the existing file to deliver (relative to workspace, or absolute). Use the path as it appears in exec output or ls results.",
			},
		},
		"required": []string{"path"},
	}
}

func (t *DeliverFileTool) Execute(ctx context.Context, args map[string]any) *Result {
	path, _ := args["path"].(string)
	if path == "" {
		return ErrorResult("path is required")
	}

	workspace := ToolWorkspaceFromCtx(ctx)
	if workspace == "" {
		workspace = t.workspace
	}
	restrict := effectiveRestrict(ctx, t.restrict)

	// Sandbox container path remapping: /workspace/X → host workspace/X.
	containerWorkdir := sandbox.DefaultContainerWorkdir
	if strings.HasPrefix(path, containerWorkdir+"/") || path == containerWorkdir {
		rel := strings.TrimPrefix(path, containerWorkdir)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" {
			return ErrorResult("path points to the workspace root, not a file")
		}
		// Map container path → host path. The sandbox mounts t.workspace at /workspace,
		// but the effective per-user workspace is a subdirectory. Try both.
		hostPath := filepath.Join(workspace, rel)
		if _, err := os.Stat(hostPath); err != nil {
			// Fall back to global workspace (t.workspace) in case the file is outside user dir.
			hostPath = filepath.Join(t.workspace, rel)
		}
		path = hostPath
	}

	// Resolve and validate the path within workspace boundaries.
	resolved, err := resolvePath(path, workspace, restrict)
	if err != nil {
		// Also try against base workspace if per-user workspace didn't resolve.
		if workspace != t.workspace {
			resolved, err = resolvePath(path, t.workspace, restrict)
		}
		if err != nil {
			return ErrorResult(fmt.Sprintf("cannot access file: %v", err))
		}
	}

	// Verify file exists and is not a directory.
	info, err := os.Stat(resolved)
	if err != nil {
		return ErrorResult(fmt.Sprintf("file not found: %s", path))
	}
	if info.IsDir() {
		return ErrorResult("path is a directory, not a file")
	}

	result := SilentResult(fmt.Sprintf("File delivered: %s (%d bytes)", filepath.Base(resolved), info.Size()))
	result.Media = []bus.MediaFile{{Path: resolved}}
	if dm := DeliveredMediaFromCtx(ctx); dm != nil {
		dm.Mark(resolved)
	}
	return result
}
