package tabjson

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io"
	"github.com/viant/toolbox/format"
	goIo "io"
	"log"
	"testing"
	"time"
)

func TestReader(t *testing.T) {
	testCases := []struct {
		description  string
		config       *Config
		data         func() interface{}
		expectedRead string
	}{{
		description: "ensure values are properly escaped",
		config: &Config{
			FieldSeparator:  `,`,
			ObjectSeparator: `#`,
			EncloseBy:       `'`,
			EscapeBy:        `\`,
			NullValue:       "null",
			Stringify: StringifyConfig{
				IgnoreObjectSeparator: false,
				IgnoreEncloseBy:       false,
			},
		},
		data: func() interface{} {
			type Foo struct {
				ID      string
				Comment string
			}

			return []*Foo{
				{
					ID:      `\`,
					Comment: `\,`,
				},
				{
					ID:      `\'`,
					Comment: `\'#`,
				},
			}
		},
		expectedRead: `'\\','\\\,'#'\\\'','\\\'\#'`,
	},
		{
			description: "ensure values are properly escaped while ignoring ObjectSeparator and EncloseBy characters",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: true,
					IgnoreEncloseBy:       true,
				},
			},
			data: func() interface{} {
				type Foo struct {
					ID      string
					Comment string
				}

				return []*Foo{
					{
						ID:      `\`,
						Comment: `\,`,
					},
					{
						ID:      `\'`,
						Comment: `\'#`,
					},
				}
			},
			expectedRead: `'\\','\\\,'#'\\'','\\'#'`,
		},
		{
			description: "ensure values are properly escaped while ignoring ObjectSeparator and EncloseBy and FieldSeparator",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreFieldSeparator:  true,
					IgnoreObjectSeparator: true,
					IgnoreEncloseBy:       true,
				},
			},
			data: func() interface{} {
				type Foo struct {
					ID      string
					Comment string
				}

				return []*Foo{
					{
						ID:      `\`,
						Comment: `\,`,
					},
					{
						ID:      `\'`,
						Comment: `\'#`,
					},
				}
			},
			expectedRead: `'\\','\\,'#'\\'','\\'#'`,
		},
		{
			description: "ensure values are properly nullified - string type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp                    int
					FieldString           string  // zero value == `` => ``
					FieldStringNullify    string  `sqlx:"nullifyEmpty=true"` // zero value == `` => null
					FieldStringPtr        *string // zero value == nil => null
					FieldStringPtrNullify *string `sqlx:"nullifyEmpty=true"` // zero value == nil  => null`, dereferenced value == `` => null
				}

				return []*Foo{
					{
						Lp:                    1,
						FieldString:           "",
						FieldStringNullify:    "",
						FieldStringPtr:        nil,
						FieldStringPtrNullify: nil,
					},
					{
						Lp:                    2,
						FieldString:           "",
						FieldStringNullify:    "",
						FieldStringPtr:        new(string),
						FieldStringPtrNullify: new(string),
					},
					{
						Lp:                    3,
						FieldString:           "1",
						FieldStringNullify:    "1",
						FieldStringPtr:        stringPtr("1"),
						FieldStringPtrNullify: stringPtr("1"),
					},
				}
			},
			expectedRead: `1,'',null,null,null#2,'',null,'',null#3,'1','1','1','1'`,
		},
		{
			description: "ensure values are properly nullified - int type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int
					Field           int  // zero value == 0 => 0
					FieldNullify    int  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *int // zero value == nil => null
					FieldPtrNullify *int `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(int),
						FieldPtrNullify: new(int),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        intPtr(1),
						FieldPtrNullify: intPtr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - int8 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int8
					Field           int8  // zero value == 0 => 0
					FieldNullify    int8  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *int8 // zero value == nil => null
					FieldPtrNullify *int8 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(int8),
						FieldPtrNullify: new(int8),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        int8Ptr(1),
						FieldPtrNullify: int8Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - int16 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int16
					Field           int16  // zero value == 0 => 0
					FieldNullify    int16  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *int16 // zero value == nil => null
					FieldPtrNullify *int16 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(int16),
						FieldPtrNullify: new(int16),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        int16Ptr(1),
						FieldPtrNullify: int16Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - int32 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int32
					Field           int32  // zero value == 0 => 0
					FieldNullify    int32  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *int32 // zero value == nil => null
					FieldPtrNullify *int32 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(int32),
						FieldPtrNullify: new(int32),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        int32Ptr(1),
						FieldPtrNullify: int32Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - int64 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int64
					Field           int64  // zero value == 0 => 0
					FieldNullify    int64  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *int64 // zero value == nil => null
					FieldPtrNullify *int64 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(int64),
						FieldPtrNullify: new(int64),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        int64Ptr(1),
						FieldPtrNullify: int64Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - uint type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              uint
					Field           uint  // zero value == 0 => 0
					FieldNullify    uint  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *uint // zero value == nil => null
					FieldPtrNullify *uint `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(uint),
						FieldPtrNullify: new(uint),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        uintPtr(1),
						FieldPtrNullify: uintPtr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - uint8 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              uint8
					Field           uint8  // zero value == 0 => 0
					FieldNullify    uint8  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *uint8 // zero value == nil => null
					FieldPtrNullify *uint8 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(uint8),
						FieldPtrNullify: new(uint8),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        uint8Ptr(1),
						FieldPtrNullify: uint8Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - uint16 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              uint16
					Field           uint16  // zero value == 0 => 0
					FieldNullify    uint16  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *uint16 // zero value == nil => null
					FieldPtrNullify *uint16 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(uint16),
						FieldPtrNullify: new(uint16),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        uint16Ptr(1),
						FieldPtrNullify: uint16Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - uint32 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              uint32
					Field           uint32  // zero value == 0 => 0
					FieldNullify    uint32  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *uint32 // zero value == nil => null
					FieldPtrNullify *uint32 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(uint32),
						FieldPtrNullify: new(uint32),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        uint32Ptr(1),
						FieldPtrNullify: uint32Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - uint64 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              uint64
					Field           uint64  // zero value == 0 => 0
					FieldNullify    uint64  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *uint64 // zero value == nil => null
					FieldPtrNullify *uint64 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(uint64),
						FieldPtrNullify: new(uint64),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        uint64Ptr(1),
						FieldPtrNullify: uint64Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - bool type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int
					Field           bool  // zero value == false => false
					FieldNullify    bool  `sqlx:"nullifyEmpty=true"` // zero value == false => null
					FieldPtr        *bool // zero value == nil => null
					FieldPtrNullify *bool `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == false => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           false,
						FieldNullify:    false,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           false,
						FieldNullify:    false,
						FieldPtr:        new(bool),
						FieldPtrNullify: new(bool),
					},
					{
						Lp:              3,
						Field:           true,
						FieldNullify:    true,
						FieldPtr:        boolPtr(true),
						FieldPtrNullify: boolPtr(true),
					},
				}
			},
			expectedRead: `1,false,null,null,null#2,false,null,false,null#3,true,true,true,true`,
		},
		{
			description: "ensure values are properly nullified - float64 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int
					Field           float64  // zero value == 0 => 0
					FieldNullify    float64  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *float64 // zero value == nil => null
					FieldPtrNullify *float64 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(float64),
						FieldPtrNullify: new(float64),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        float64Ptr(1),
						FieldPtrNullify: float64Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - float32 type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int
					Field           float32  // zero value == 0 => 0
					FieldNullify    float32  `sqlx:"nullifyEmpty=true"` // zero value == 0 => null
					FieldPtr        *float32 // zero value == nil => null
					FieldPtrNullify *float32 `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == 0 => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           0,
						FieldNullify:    0,
						FieldPtr:        new(float32),
						FieldPtrNullify: new(float32),
					},
					{
						Lp:              3,
						Field:           1,
						FieldNullify:    1,
						FieldPtr:        float32Ptr(1),
						FieldPtrNullify: float32Ptr(1),
					},
				}
			},
			expectedRead: `1,0,null,null,null#2,0,null,0,null#3,1,1,1,1`,
		},
		{
			description: "ensure values are properly nullified - time.Time type",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					Lp              int
					Field           time.Time  // zero value == time.Time{} => '0001-01-01T00:00:00Z'
					FieldNullify    time.Time  `sqlx:"nullifyEmpty=true"` // zero value == time.Time{} => null
					FieldPtr        *time.Time // zero value == nil => null
					FieldPtrNullify *time.Time `sqlx:"nullifyEmpty=true"` // zero value == nil => null, dereferenced value == time.Time{} => null
				}

				return []*Foo{
					{
						Lp:              1,
						Field:           time.Time{},
						FieldNullify:    time.Time{},
						FieldPtr:        nil,
						FieldPtrNullify: nil,
					},
					{
						Lp:              2,
						Field:           time.Time{},
						FieldNullify:    time.Time{},
						FieldPtr:        new(time.Time),
						FieldPtrNullify: new(time.Time),
					},
					{
						Lp:              3,
						Field:           time.Date(1410, time.July, 15, 9, 0, 0, 0, time.FixedZone("UTC", 2*60*60)),
						FieldNullify:    time.Date(1410, time.July, 15, 9, 0, 0, 0, time.FixedZone("UTC", 2*60*60)),
						FieldPtr:        timePtr(time.Date(1410, time.July, 15, 9, 0, 0, 0, time.FixedZone("UTC", 2*60*60))),
						FieldPtrNullify: timePtr(time.Date(1410, time.July, 15, 9, 0, 0, 0, time.FixedZone("UTC", 2*60*60))),
					},
				}
			},
			expectedRead: `1,'0001-01-01T00:00:00Z',null,null,null#2,'0001-01-01T00:00:00Z',null,'0001-01-01T00:00:00Z',null#3,'1410-07-15T09:00:00+02:00','1410-07-15T09:00:00+02:00','1410-07-15T09:00:00+02:00','1410-07-15T09:00:00+02:00'`,
		},
	}

	for _, testCase := range testCases {
		testData := testCase.data()
		reader, _, err := NewReader(testData, testCase.config)
		assert.Nil(t, err, testCase.description)

		all, err := goIo.ReadAll(reader)
		assert.Equal(t, testCase.expectedRead, string(all), testCase.description)
		assert.Nil(t, err, testCase.description)
	}
}

func TestReader_Read(t *testing.T) {
	testCases := []struct {
		description  string
		config       *Config
		data         func() interface{}
		expectedRead string
		bufferSizes  []int
		options      []interface{}
	}{
		{
			description: "ensure Read function properly serves any positive buffer size",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					ID      string
					Comment string
				}

				return []*Foo{
					{
						ID:      `\`,
						Comment: `\,`,
					},
					{
						ID:      `\'`,
						Comment: `\'#`,
					},
				}
			},
			expectedRead: `'\\','\\\,'#'\\\'','\\\'\#'`,            // len(expectedRead) == 27
			bufferSizes:  []int{1, 2, 3, 13, 14, 27, 28, 54, 1000}, // from 1 to more than len(expectedRead)
		},
		{
			description: "specified fields with header",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: "\n",
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					ID      string
					Comment string
				}

				return []*Foo{
					{
						ID:      `id - 1`,
						Comment: `comment - 1`,
					},
					{
						ID:      `id - 2`,
						Comment: `comment - 2`,
					},
				}
			},
			options: []interface{}{
				io.StringifierConfig{
					CaseFormat: format.CaseLowerUnderscore,
					Fields:     []string{"Comment"},
				},
			},
			expectedRead: `'comment'
'comment - 1'
'comment - 2'`,
			bufferSizes: []int{1, 2, 3, 13, 14, 27, 28, 54, 1000}, // from 1 to more than len(expectedRead)
		},
		{
			description: "specified case format without fields",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: "\n",
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
				Stringify: StringifyConfig{
					IgnoreObjectSeparator: false,
					IgnoreEncloseBy:       false,
				},
			},
			data: func() interface{} {
				type Foo struct {
					ID      string
					Comment string
				}

				return []*Foo{
					{
						ID:      `id - 1`,
						Comment: `comment - 1`,
					},
					{
						ID:      `id - 2`,
						Comment: `comment - 2`,
					},
				}
			},
			options: []interface{}{
				io.StringifierConfig{
					CaseFormat: format.CaseLowerUnderscore,
				},
			},
			expectedRead: `'i_d','comment'
'id - 1','comment - 1'
'id - 2','comment - 2'`,
			bufferSizes: []int{1, 2, 3, 13, 14, 27, 28, 54, 1000}, // from 1 to more than len(expectedRead)
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		testData := testCase.data()

		for _, buffSize := range testCase.bufferSizes {
			reader, _, err := NewReader(testData, testCase.config, testCase.options...)
			assert.Nil(t, err, testCase.description)

			if buffSize < 1 {
				log.Fatalf("Buffer size must be greater than 0 (current: %d)", buffSize)
			}

			buf := make([]byte, buffSize)
			all := make([]byte, 0)
			maxIterationCountAllowed := len(testCase.expectedRead)/len(buf) + 2
			iterationCounter := 0

			for {
				iterationCounter++
				n, err := reader.Read(buf)
				all = append(all, buf[:n]...)

				if err == goIo.EOF {
					break
				} else {
					assert.Nil(t, err, testCase.description, "Current buffer size = ", buffSize)
				}

				if iterationCounter > maxIterationCountAllowed {
					log.Fatalf("Infinite loop danger using buffSize = %d - the maximum count of loop iterations has been exceeded (max: %d, current: %d)", buffSize, maxIterationCountAllowed, iterationCounter)
				}
			}

			assert.Equal(t, testCase.expectedRead, string(all), testCase.description, "Buffer size = ", buffSize)
			assert.Nil(t, err, testCase.description)

		}
	}
}

func stringPtr(i string) *string {
	return &i
}

func intPtr(i int) *int {
	return &i
}

func int8Ptr(i int8) *int8 {
	return &i
}

func int16Ptr(i int16) *int16 {
	return &i
}

func int32Ptr(i int32) *int32 {
	return &i
}

func int64Ptr(i int64) *int64 {
	return &i
}

func uintPtr(i uint) *uint {
	return &i
}

func uint8Ptr(i uint8) *uint8 {
	return &i
}

func uint16Ptr(i uint16) *uint16 {
	return &i
}

func uint32Ptr(i uint32) *uint32 {
	return &i
}

func uint64Ptr(i uint64) *uint64 {
	return &i
}

func boolPtr(i bool) *bool {
	return &i
}

func float32Ptr(i float32) *float32 {
	return &i
}

func float64Ptr(i float64) *float64 {
	return &i
}

func timePtr(i time.Time) *time.Time {
	return &i
}
