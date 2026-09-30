package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	protocol "github.com/viant/mcp-protocol/server"
)

func (s *Service) AuthorizeCatalogTool(ctx context.Context, name, action string) error {
	if s.lazy != nil {
		return s.lazy.service().AuthorizeCatalogTool(ctx, name, action)
	}
	if s.authorizeCatalogTool == nil {
		return nil
	}
	if name == SkillListTool || name == SkillGetTool {
		return nil
	}
	plan, ok := s.catalog.Tool(name)
	if !ok {
		return fmt.Errorf("unknown MCP tool")
	}
	return s.authorizeCatalogTool(ctx, plan.Target(), action)
}

func (s *Service) AuthorizeCatalogResource(ctx context.Context, uri, action string) error {
	if s.lazy != nil {
		return s.lazy.service().AuthorizeCatalogResource(ctx, uri, action)
	}
	if s.authorizeCatalogResource == nil {
		return nil
	}
	return s.authorizeCatalogResource(ctx, uri, action)
}

func (s *Service) AuthorizeResourceRead(ctx context.Context, uri string) error {
	if s.lazy != nil {
		return s.lazy.service().AuthorizeResourceRead(ctx, uri)
	}
	if s.authorizeResource == nil {
		return nil
	}
	return s.authorizeResource(ctx, uri)
}

func (s *Service) ListVisibleSkills(ctx context.Context, cursor *string) (*schema.ListSkillsResult, *jsonrpc.Error) {
	if s.lazy != nil {
		return s.lazy.service().ListVisibleSkills(ctx, cursor)
	}
	return listVisibleSkills(ctx, s.registry, s.authorizeCatalogResource, cursor)
}

func listVisibleSkills(ctx context.Context, registry *protocol.Registry, authorize func(context.Context, string, string) error, cursor *string) (*schema.ListSkillsResult, *jsonrpc.Error) {
	if err := ctx.Err(); err != nil {
		return nil, jsonrpc.NewInternalError("skill catalog unavailable", nil)
	}
	entries := []schema.Skill{}
	for _, entry := range registry.ListRegisteredSkills() {
		if authorize != nil && (authorize(ctx, entry.Uri, "discover") != nil || authorize(ctx, entry.Uri, "describe") != nil) {
			continue
		}
		// A manifest discloses its entire supporting-file inventory.
		permitted := true
		for _, file := range entry.Resources.Files {
			if authorize != nil && authorize(ctx, file.Uri, "describe") != nil {
				permitted = false
				break
			}
		}
		if permitted {
			entries = append(entries, entry)
		}
	}
	raw, _ := json.Marshal(entries)
	revision := fmt.Sprintf("%x", sha256.Sum256(raw))
	start := 0
	if cursor != nil {
		decoded, err := base64.RawURLEncoding.DecodeString(*cursor)
		parts := strings.Split(string(decoded), ":")
		if err != nil || len(parts) != 2 || parts[0] != revision {
			return nil, jsonrpc.NewInvalidParamsError("invalid or stale skills cursor", nil)
		}
		start, err = strconv.Atoi(parts[1])
		if err != nil || start <= 0 || start >= len(entries) || start%32 != 0 {
			return nil, jsonrpc.NewInvalidParamsError("invalid skills cursor", nil)
		}
	}
	end := min(start+32, len(entries))
	result := &schema.ListSkillsResult{ResultType: schema.ResultTypeComplete, Skills: entries[start:end]}
	if end < len(entries) {
		next := base64.RawURLEncoding.EncodeToString([]byte(revision + ":" + strconv.Itoa(end)))
		result.NextCursor = &next
	}
	return result, nil
}
