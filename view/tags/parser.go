package tags

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/tags"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

// ValueTag represents default value tag
const ValueTag = "value"

// ParseViewTags parse view related tags
func ParseViewTags(tag reflect.StructTag, fs *embed.FS) (*Tag, error) {
	return Parse(tag, fs, ViewTag, SQLTag, SQLSummaryTag, LinkOnTag, format.TagName)
}

// ParseStateTags parse state related tags
func ParseStateTags(tag reflect.StructTag, fs *embed.FS) (*Tag, error) {
	ret, err := Parse(tag, fs, ParameterTag, SQLTag, PredicateTag, CodecTag, HandlerTag, format.TagName)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func Parse(tag reflect.StructTag, fs *embed.FS, tagNames ...string) (*Tag, error) {
	var value *string
	if val, ok := tag.Lookup(ValueTag); ok {
		value = &val
	}
	ret := &Tag{fs: afs.New(), TypeName: tag.Get(xreflect.TagTypeName), Description: tag.Get(DescriptionTag), Example: tag.Get(ExampleTag), Value: value, embed: fs}
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
				ret.SQL = NewViewSQL(tagValue, "")
				continue
			}
			URI := tagValue[4:]
			data, err := loadContent(URI, ret.fs, ret.getOptions(), fs)
			if err != nil {
				return nil, err
			}
			ret.SQL = NewViewSQL(string(data), URI)

		case SQLSummaryTag:
			ret.View = ret.ensureView()
			if !strings.HasPrefix(tagValue, "uri") {
				ret.SummarySQL = NewViewSQLSummary(tagValue, "")
				continue
			}
			URI := tagValue[4:]
			data, err := loadContent(URI, ret.fs, ret.getOptions(), fs)
			if err != nil {
				return nil, err
			}
			ret.SummarySQL = NewViewSQLSummary(string(data), URI)
		case PredicateTag:
			err := parsePredicate(tag, ret)
			if err != nil {
				return nil, err
			}
		case HandlerTag:
			ret.Handler = &Handler{Name: name}
			if err := values.MatchPairs(ret.updatedHandler); err != nil {
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
			err := values.MatchPairs(ret.updatedParameter)
			if err != nil {
				return nil, err
			}
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

func loadContent(URI string, fs afs.Service, storageOptions []storage.Option, embedFs *embed.FS) ([]byte, error) {
	if embedFs != nil {
		storageOptions = append(storageOptions, embedFs)
	}
	embedURI := strings.TrimSpace(URI)
	if url.Scheme(embedURI, "") == "" {
		embedURI = "embed:///" + URI
		if data, err := fs.DownloadWithURL(context.Background(), embedURI, storageOptions...); err == nil {
			return data, nil
		}
	}
	data, err := fs.DownloadWithURL(context.Background(), URI, storageOptions...)
	return data, err
}

func parsePredicate(tag reflect.StructTag, ret *Tag) error {
	tagLiteral := string(tag)
	for i := 0; i < 10; i++ { //upto 10 predicates max
		index := strings.Index(tagLiteral, PredicateTag+":")
		if index == -1 {
			break
		}
		offset := len(ParameterTag) + 3
		predicateTag := tagLiteral[index:]
		if index = strings.Index(predicateTag[offset:], `"`); index != -1 {
			predicateTag = predicateTag[:offset+index+1]
			tagLiteral = strings.Replace(tagLiteral, predicateTag, "", 1)
			values := tags.Values(reflect.StructTag(predicateTag).Get(PredicateTag))
			var name string
			name, values = values.Name()
			ret.Predicates = append(ret.Predicates, &Predicate{Name: name})
			if err := values.MatchPairs(ret.updatedPredicate); err != nil {
				return err
			}
		}
	}
	return nil
}

// ExcludeStateTags exclude state tags
func ExcludeStateTags(tag string) string {
	fieldTags := tags.NewTags(tag)
	updatedTags := tags.Tags{}
	for _, item := range fieldTags {
		switch item.Name {
		case ParameterTag, PredicateTag, ValueTag, CodecTag:
		default:
			updatedTags = append(updatedTags, item)
		}
	}
	return updatedTags.Stringify()
}
