package json

type Option interface{}
type Options []Option

func (o Options) Tag() *Tag {
	for _, candidate := range o {
		if value, ok := candidate.(*Tag); ok {
			return value
		}
	}
	return nil
}

func (o Options) DefaultTag() *DefaultTag {
	for _, candidate := range o {
		if value, ok := candidate.(*DefaultTag); ok {
			return value
		}
	}
	return nil
}

type cacheConfig struct {
	ignoreCustomUnmarshaller bool
	ignoreCustomMarshaller   bool
}
