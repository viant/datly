package json

import (
	"github.com/francoispqt/gojay"
)

type (
	BytesSlice struct {
		b *[]byte
	}

	BytesPtrSlice struct {
		b **[]byte
	}
)

func (b *BytesPtrSlice) UnmarshalJSONArray(d *gojay.Decoder) error {
	if *b.b == nil {
		*b.b = new([]byte)
	}

	return (&BytesSlice{b: *b.b}).UnmarshalJSONArray(d)
}

func (b *BytesSlice) UnmarshalJSONArray(d *gojay.Decoder) error {
	anInt8 := new(int8)
	if err := d.AddInt8(anInt8); err != nil {
		return err
	}

	*b.b = append(*b.b, byte(*anInt8))
	return nil
}
