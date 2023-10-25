package signature

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/view/extension"
	"github.com/viant/xreflect"
	"path"
)

type (
	Service struct {
		APIPrefix string
		URL       string
		Headers   []*Header
		matcher   *matcher.Matcher
	}

	entry struct {
		uri     string
		methods []string
		header  *Header
		index   int
	}
)

func (m *entry) URI() string {
	return m.uri
}
func (m *entry) Namespaces() []string {
	return m.methods
}

func (s *Service) init(ctx context.Context) error {
	if err := s.loadSignatures(ctx, s.URL, true); err != nil {
		return err
	}

	var matchables = make([]matcher.Matchable, 0, 3*len(s.Headers))
	for i := range s.Headers {
		header := s.Headers[i]
		for j, route := range header.Contracts {
			aMatchable := &entry{header: header, index: j, uri: route.URI, methods: []string{route.Method}}
			matchables = append(matchables, aMatchable)
		}
	}
	s.matcher = matcher.NewMatcher(matchables)
	return nil
}

// Signature returns match component signature
func (s *Service) Signature(method, URI string) (*Signature, error) {
	matchable, err := s.matcher.MatchOne(method, URI)
	if err != nil && s.APIPrefix != "" { //fallback to full URI
		matchable, err = s.matcher.MatchOne(method, path.Join(s.APIPrefix, URI))
	}
	if err != nil {
		return nil, err
	}
	aMatch, _ := matchable.(*entry)
	if aMatch == nil {
		return nil, fmt.Errorf("invalid contract match")
	}
	contract := aMatch.header.Contracts[aMatch.index]

	typeRegistry := xreflect.NewTypes(xreflect.WithRegistry(extension.Config.Types))
	for _, typeDef := range aMatch.header.Resource.Types {
		typeRegistry.Register(typeDef.Name, xreflect.WithTypeDefinition(typeDef.DataType))
	}

	contract.Input = aMatch.header.Resource.InputParameters
	for _, parameter := range contract.Input {
		if len(parameter.Predicates) == 0 {
			continue
		}
		parameter.Schema.InitType(typeRegistry.Lookup, false)
	}

	return aMatch.header.Signature(contract, typeRegistry)
}

func (s *Service) loadSignatures(ctx context.Context, URL string, isRoot bool) error {
	objects, err := fs.List(ctx, URL)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if url.Equals(object.URL(), URL) {
			continue
		}
		if isRoot && object.Name() == "paths.yaml" {
			continue
		}
		if object.IsDir() {
			if object.Name() == ".meta" {
				continue
			}
			if err := s.loadSignatures(ctx, object.URL(), isRoot); err != nil {
				return err
			}
		}
		ext := path.Ext(object.Name())
		switch ext {
		case ".yaml", ".yml":
			header, err := NewHeader(ctx, object.URL())
			if err != nil {
				return err
			}
			s.Headers = append(s.Headers, header)
		}
	}
	return nil
}

func New(ctx context.Context, APIPrefix string, URL string) (*Service, error) {
	ret := &Service{URL: URL, APIPrefix: APIPrefix}
	return ret, ret.init(ctx)
}
