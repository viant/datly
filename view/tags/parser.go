package tags

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/structology/format"
	"github.com/viant/structology/tags"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

// ParseViewTags parse view related tags
func ParseViewTags(tag reflect.StructTag, fs *embed.FS) (*Tag, error) {
	return Parse(tag, fs, ViewTag, SQLTag, SQLSummaryTag, LinkOnTag)
}

// ParseStateTags parse state related tags
func ParseStateTags(tag reflect.StructTag, fs *embed.FS) (*Tag, error) {
	ret, err := Parse(tag, fs, ParameterTag, SQLTag, PredicateTag, CodecTag, format.TagName)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func Parse(tag reflect.StructTag, fs *embed.FS, tagNames ...string) (*Tag, error) {
	ret := &Tag{fs: afs.New(), TypeName: tag.Get(xreflect.TagTypeName), Description: tag.Get(DescriptionTag), embed: fs}
	var err error
	for _, tagName := range tagNames {
		tagValue, ok := tag.Lookup(tagName)
		if !ok {
			continue
		}
		var name string
		values := tags.Values(tagValue)
		name, values = values.Name()
		switch tagName {
		case ViewTag:
			ret.View = ret.ensureView()
			ret.View.Name = name
			if err := values.MatchPairs(ret.updateView); err != nil {
				return nil, err
			}
		case SQLTag:
			ret.View = ret.ensureView()
			if !strings.HasPrefix(tagValue, "uri") {
				ret.SQL = ViewSQL(tagValue)
				continue
			}
			URI := tagValue[4:]
			data, err := ret.fs.DownloadWithURL(context.Background(), strings.TrimSpace(URI), ret.getOptions()...)
			if err != nil {
				return nil, err
			}
			ret.SQL = ViewSQL(data)

		case SQLSummaryTag:
			ret.View = ret.ensureView()
			if !strings.HasPrefix(tagValue, "uri") {
				ret.SummarySQL = ViewSQLSummary(tagValue)
				continue
			}
			URI := tagValue[4:]
			data, err := ret.fs.DownloadWithURL(context.Background(), strings.TrimSpace(URI), ret.getOptions()...)
			if err != nil {
				return nil, err
			}
			ret.SummarySQL = ViewSQLSummary(data)
		case PredicateTag:
			ret.Predicate = &Predicate{Name: name}
			if err := values.MatchPairs(ret.updatedPredicate); err != nil {
				return nil, err
			}
		case CodecTag:
			ret.Codec = &Codec{Name: name}
			if err := values.MatchPairs(ret.updatedCodec); err != nil {
				return nil, err
			}
		case LinkOnTag:
			if err := values.Match(func(value string) error {
				ret.LinkOn = append(ret.LinkOn, value)
				return nil
			}); err != nil {
				return nil, err
			}
		case ParameterTag:
			ret.Parameter = &Parameter{Name: name}
			return ret, values.MatchPairs(ret.updatedParameter)
		case format.TagName:
			if ret.Format, err = format.Parse(tag); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported tag: %s", tagName)
		}

	}

	return ret, nil
}
