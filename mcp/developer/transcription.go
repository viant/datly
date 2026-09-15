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
	if request.Generation.Enabled() {
		target := s.targets[args.Target]
		compiled, err := (&transcribe.Discovery{
			BaseDir:       target.BaseDir,
			ModuleDirs:    target.ModuleDirs,
			Connector:     target.Connector,
			ColumnRefiner: target.ColumnRefiner,
		}).CompileSource(ctx, request.Source)
		if err != nil {
			return nil, err
		}
		result, err := (transcribe.Generator{Operation: request.Generation.Operation, Language: request.Generation.Language}).Generate(ctx, transcribe.GenerationRequest{Compiled: compiled, Destination: request.Destination})
		if err != nil {
			return nil, err
		}
		return s.transcription(args.Target, result, request.Generation)
	}
	result, err := transcribe.NewCompiler().Transcribe(ctx, request)
	if err != nil {
		return nil, err
	}
	return s.transcription(args.Target, result, transcribe.GenerationOptions{})
}

func (s *Service) transcription(target string, result *transcribe.GeneratedPackage, generation transcribe.GenerationOptions) (*Transcription, error) {
	response := &Transcription{Target: target, Mode: "transcribe"}
	if generation.Enabled() {
		response.Mode = "generation"
		response.Operation = generation.Operation
		response.Language = string(generation.Language)
	}
	for _, file := range result.Result.Files {
		relative, err := filepath.Rel(s.targets[target].BaseDir, file.Path)
		if err != nil {
			return nil, err
		}
		response.Files = append(response.Files, filepath.ToSlash(relative))
	}
	sort.Strings(response.Files)
	return response, nil
}
