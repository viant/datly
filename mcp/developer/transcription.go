package developer

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/viant/datly/transcribe"
)

func (s *Service) transcribe(ctx context.Context, args arguments) (*Transcription, error) {
	request, ok := s.authoring[args.Target]
	if !ok {
		return nil, fmt.Errorf("transcription is not configured for this target")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A developer client can edit source text, never output roots or options. The
	// canonical generator additionally rejects symlinks and protects authored files.
	if err := filepath.WalkDir(request.Destination, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("authoring workspace contains a symlink")
		}
		return ctx.Err()
	}); err != nil {
		return nil, err
	}
	source := *request.Source
	source.Text = args.Source
	request.Source = &source
	if source.Types != nil {
		types, err := source.Types.Clone()
		if err != nil {
			return nil, err
		}
		source.Types = types
	}
	result, err := transcribe.NewCompiler().Transcribe(ctx, request)
	if err != nil {
		return nil, err
	}
	response := &Transcription{Target: args.Target}
	for _, file := range result.Result.Files {
		relative, err := filepath.Rel(s.targets[args.Target].BaseDir, file.Path)
		if err != nil {
			return nil, err
		}
		response.Files = append(response.Files, filepath.ToSlash(relative))
	}
	sort.Strings(response.Files)
	return response, nil
}
