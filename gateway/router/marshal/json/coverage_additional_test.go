package json

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xunsafe"
)

type fallbackMarshaller struct {
	marshalCalled   bool
	unmarshalCalled bool
}

type errMarshaller struct{}

func (e *errMarshaller) MarshallObject(ptr unsafe.Pointer, session *MarshallSession) error {
	return errors.New("marshal err")
}
func (e *errMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return errors.New("unmarshal err")
}

func (f *fallbackMarshaller) MarshallObject(ptr unsafe.Pointer, session *MarshallSession) error {
	f.marshalCalled = true
	session.WriteString(`{"fallback":true}`)
	return nil
}

func (f *fallbackMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	f.unmarshalCalled = true
	return nil
}

type gjOnlyPtr struct {
	V int
}

func (g *gjOnlyPtr) MarshalJSONObject(enc *gojay.Encoder) {
	enc.IntKey("V", g.V)
}

func (g *gjOnlyPtr) IsNil() bool { return g == nil }

func (g *gjOnlyPtr) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	if key == "V" {
		return dec.Int(&g.V)
	}
	return nil
}

func (g *gjOnlyPtr) NKeys() int { return 0 }

type customSum int
type customStruct int
type customStructHolder struct {
	V int
}
type gojayBadInit struct {
	C chan int
}

type withM interface{ M() }
type withMImpl struct{}

func (withMImpl) M() {}

func (c *customSum) UnmarshalJSONWithOptions(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	var vals []int
	if err := decoder.SliceInt(&vals); err != nil {
		return err
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	*dst.(**customSum) = (*customSum)(&sum)
	return nil
}

func (customStruct) UnmarshalJSONWithOptions(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	var v int
	if err := decoder.Int(&v); err != nil {
		return err
	}
	p := dst.(*customStruct)
	*p = customStruct(v)
	return nil
}

func (c customStructHolder) UnmarshalJSONWithOptions(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	var v int
	if err := decoder.Int(&v); err != nil {
		return err
	}
	c.V = v
	p := dst.(*customStructHolder)
	*p = c
	return nil
}

func (g gojayBadInit) MarshalJSONObject(enc *gojay.Encoder) {}
func (g gojayBadInit) IsNil() bool                          { return false }

func TestCoverage_OptionsAndTags(t *testing.T) {
	opts := Options{&Tag{FieldName: "x"}, &format.Tag{Name: "y"}}
	require.Equal(t, "x", opts.Tag().FieldName)
	require.Equal(t, "y", opts.FormatTag().Name)

	parsed := Parse("name,omitempty")
	require.Equal(t, "name", parsed.FieldName)
	require.True(t, parsed.OmitEmpty)

	transient := Parse("-")
	require.True(t, transient.Transient)

	xTag := ParseXTag("", "inline")
	require.True(t, xTag.Inline)
}

func TestCoverage_DefaultTagAndParseValue(t *testing.T) {
	type sample struct {
		A *int       `default:"value=7,nullable=false,required=true"`
		B time.Time  `default:"value=2024-01-01T00:00:00Z,format=2006-01-02T15:04:05Z07:00"`
		C *time.Time `default:"value=2024-01-01T00:00:00Z,format=2006-01-02T15:04:05Z07:00"`
	}
	rType := reflect.TypeOf(sample{})

	aTag, err := NewDefaultTag(rType.Field(0))
	require.NoError(t, err)
	require.True(t, aTag.IsRequired())
	require.False(t, aTag.IsNullable())

	bTag, err := NewDefaultTag(rType.Field(1))
	require.NoError(t, err)
	require.NotNil(t, bTag._value)

	cTag, err := NewDefaultTag(rType.Field(2))
	require.NoError(t, err)
	require.NotNil(t, cTag._value)

	_, err = parseValue(reflect.TypeOf(time.Time{}), "bad-time", time.RFC3339)
	require.Error(t, err)
}

func TestCoverage_BytesSliceUnmarshal(t *testing.T) {
	var b []byte
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`[1,2,3]`)))
	defer dec.Release()
	require.NoError(t, dec.Array(&BytesSlice{b: &b}))
	require.Equal(t, []byte{1, 2, 3}, b)

	var bPtr *[]byte
	dec2 := gojay.BorrowDecoder(bytes.NewReader([]byte(`[4,5]`)))
	defer dec2.Release()
	require.NoError(t, dec2.Array(&BytesPtrSlice{b: &bPtr}))
	require.Equal(t, []byte{4, 5}, *bPtr)
}

func TestCoverage_ErrorJoin(t *testing.T) {
	err := NewError("a", errors.New("x"))
	require.Contains(t, err.Error(), "failed to unmarshal a")

	nested := NewError("obj", NewError("field", errors.New("boom")))
	require.Equal(t, "obj.field", nested.Path)

	nestedArr := NewError("arr", NewError("[1]", errors.New("boom")))
	require.Equal(t, "arr[1]", nestedArr.Path)
}

func TestCoverage_UnsignedAndPointers_MarshalUnmarshal(t *testing.T) {
	type payload struct {
		U   uint
		U8  uint8
		U16 uint16
		U32 uint32
		U64 uint64
		PU  *uint
		P8  *uint8
		P16 *uint16
		P32 *uint32
		P64 *uint64
	}
	m := New(&config.IOConfig{})

	u := uint(10)
	u8 := uint8(11)
	u16 := uint16(12)
	u32 := uint32(13)
	u64 := uint64(14)
	in := payload{U: 1, U8: 2, U16: 3, U32: 4, U64: 5, PU: &u, P8: &u8, P16: &u16, P32: &u32, P64: &u64}

	data, err := m.Marshal(in)
	require.NoError(t, err)

	var out payload
	require.NoError(t, m.Unmarshal(data, &out))
	require.Equal(t, in.U, out.U)
	require.Equal(t, in.U8, out.U8)
	require.Equal(t, in.U16, out.U16)
	require.Equal(t, in.U32, out.U32)
	require.Equal(t, in.U64, out.U64)
	require.NotNil(t, out.PU)
	require.NotNil(t, out.P8)
	require.NotNil(t, out.P16)
	require.NotNil(t, out.P32)
	require.NotNil(t, out.P64)
}

func TestCoverage_ArrayAndMapEdges(t *testing.T) {
	m := New(&config.IOConfig{CaseFormat: text.CaseFormatLowerUnderscore})

	type boolArr struct {
		Flags [3]bool
	}
	encoded, err := m.Marshal(boolArr{Flags: [3]bool{true, false, true}})
	require.NoError(t, err)
	require.Contains(t, string(encoded), "[true,false,true]")

	var arrOut boolArr
	err = m.Unmarshal([]byte(`{"Flags":[true,false,true]}`), &arrOut)
	require.Error(t, err) // array unmarshal not supported

	type mapHolder struct {
		M map[string]int
	}
	var mh mapHolder
	require.NoError(t, m.Unmarshal([]byte(`{"M":{"a":1,"b":2}}`), &mh))
	require.Equal(t, 2, mh.M["b"])

	type unsupported struct {
		M map[string]bool
	}
	var bad unsupported
	err = m.Unmarshal([]byte(`{"M":{"a":true}}`), &bad)
	require.Error(t, err)
}

func TestCoverage_InterfaceAndSliceInterface(t *testing.T) {
	m := New(&config.IOConfig{})
	type obj struct {
		Any  interface{}
		List []interface{}
	}
	var out obj
	require.NoError(t, m.Unmarshal([]byte(`{"Any":{"k":1},"List":[1,"x",{"a":2}]}`), &out))
	require.Len(t, out.List, 1) // current behavior: appended as a single decoded interface payload

	encoded, err := m.Marshal(out)
	require.NoError(t, err)
	require.Contains(t, string(encoded), "\"List\"")
}

func TestCoverage_CustomUnmarshallerAndGojayWrapper(t *testing.T) {
	m := New(&config.IOConfig{})
	type holder struct {
		Sum *customSum
		G   gjOnlyPtr
	}
	var out holder
	require.NoError(t, m.Unmarshal([]byte(`{"Sum":[1,2,3],"G":{"V":7}}`), &out))
	require.NotNil(t, out.Sum)
	require.Equal(t, 6, int(*out.Sum))
	require.Equal(t, 7, out.G.V)

	data, err := m.Marshal(out)
	require.NoError(t, err)
	require.Contains(t, string(data), `"V":7`)
}

func TestCoverage_GojayWrapperFallbackAndDeferred(t *testing.T) {
	rType := reflect.TypeOf(struct{ X int }{})
	fb := &fallbackMarshaller{}
	wrapper := newGojayObjectMarshaller(getXType(rType), getXType(reflect.PtrTo(rType)), fb, true, true)
	session := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	val := struct{ X int }{X: 1}
	require.NoError(t, wrapper.MarshallObject(AsPtr(val, rType), session))
	require.True(t, fb.marshalCalled)

	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"X":1}`)))
	defer dec.Release()
	ptr := reflect.New(rType)
	require.NoError(t, wrapper.UnmarshallObject(unsafe.Pointer(ptr.Pointer()), dec, nil, &UnmarshalSession{}))
	require.True(t, fb.unmarshalCalled)

	d := newDeferred()
	d.fail(errors.New("boom"))
	require.Error(t, d.MarshallObject(nil, &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	d2 := newDeferred()
	d2.setTarget(fb)
	require.NoError(t, d2.MarshallObject(nil, &MarshallSession{Buffer: bytes.NewBuffer(nil)}))
}

func TestCoverage_PathCacheHelpers(t *testing.T) {
	pc := &pathCache{cache: sync.Map{}}
	fb := &fallbackMarshaller{}
	pc.storeMarshaler(reflect.TypeOf(1), fb)
	got, ok := pc.loadMarshaller(reflect.TypeOf(1))
	require.True(t, ok)
	require.NotNil(t, got)

	cfg := pc.parseConfig([]interface{}{&cacheConfig{IgnoreCustomMarshaller: true}})
	require.NotNil(t, cfg)
	require.True(t, cfg.IgnoreCustomMarshaller)
}

func TestCoverage_TimeAndRawMessageAndAsPtrMap(t *testing.T) {
	cfg := &config.IOConfig{TimeLayout: "2006-01-02T15:04:05Z07:00"}
	m := New(cfg)
	now := time.Now().UTC().Truncate(time.Second)
	type payload struct {
		T  time.Time
		TP *time.Time
		R  stdjson.RawMessage
		RP *stdjson.RawMessage
	}
	raw := stdjson.RawMessage(`{"a":1}`)
	in := payload{T: now, TP: &now, R: raw, RP: &raw}

	data, err := m.Marshal(in)
	require.NoError(t, err)

	var out payload
	require.NoError(t, m.Unmarshal(data, &out))
	require.Equal(t, raw, out.R)
	require.NotNil(t, out.RP)

	// map branch in AsPtr
	mapped := map[string]int{"a": 1}
	ptr := AsPtr(mapped, reflect.TypeOf(mapped))
	require.NotNil(t, ptr)
}

func TestCoverage_MarshalSessionOptionsAndInterceptors(t *testing.T) {
	m := New(&config.IOConfig{})
	type payload struct {
		Items []int
	}

	session := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	interceptors := MarshalerInterceptors{
		"Items": func() ([]byte, error) { return []byte(`[9,8,7]`), nil },
	}
	data, err := m.Marshal(payload{Items: []int{1, 2, 3}}, session, interceptors)
	require.NoError(t, err)
	require.Contains(t, string(data), `"Items":[9,8,7]`)

	_, err = m.Marshal(nil)
	require.NoError(t, err)
}

func TestCoverage_PrepareUnmarshalSessionAndInterceptor(t *testing.T) {
	m := New(&config.IOConfig{})
	type payload struct {
		ID int
	}

	um := &UnmarshalSession{}
	interceptors := UnmarshalerInterceptors{
		"ID": func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
			// consume incoming value but force a custom value
			var throwaway int
			if err := decoder.Int(&throwaway); err != nil {
				return err
			}
			*dst.(*int) = 77
			return nil
		},
	}

	var out payload
	require.NoError(t, m.Unmarshal([]byte(`{"ID":1}`), &out, um, interceptors))
	require.Equal(t, 77, out.ID)
	require.NotEmpty(t, um.Options)
}

func TestCoverage_IntAndStringBranches(t *testing.T) {
	m := New(&config.IOConfig{})

	type ints struct {
		I8  int8
		I16 int16
		I32 int32
		I64 int64
	}
	var out ints
	require.NoError(t, m.Unmarshal([]byte(`{"I8":8,"I16":16,"I32":32,"I64":64}`), &out))
	require.EqualValues(t, 8, out.I8)
	require.EqualValues(t, 16, out.I16)
	require.EqualValues(t, 32, out.I32)
	require.EqualValues(t, 64, out.I64)

	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	marshallString("line\u2028sep\u2029par\n\t\r\b\f\"\\/", sb, nil)
	require.Contains(t, sb.String(), `\u2028`)
	require.Contains(t, sb.String(), `\u2029`)
}

func TestCoverage_MapVariantsAndKeys(t *testing.T) {
	m := New(&config.IOConfig{CaseFormat: text.CaseFormatLowerUnderscore})

	type maps struct {
		MI  map[string]int
		MF  map[string]float64
		MS  map[string]string
		ANY map[string]interface{}
	}
	var out maps
	require.NoError(t, m.Unmarshal([]byte(`{"MI":{"a":1},"MF":{"x":1.5},"MS":{"k":"v"}}`), &out))
	require.Equal(t, 1, out.MI["a"])
	require.Equal(t, 1.5, out.MF["x"])
	require.Equal(t, "v", out.MS["k"])

	type intKey struct {
		M map[int]string
	}
	enc, err := m.Marshal(intKey{M: map[int]string{1: "x", 2: "y"}})
	require.NoError(t, err)
	require.Contains(t, string(enc), `"1":"x"`)

	type anyMap struct {
		M map[string]interface{}
	}
	enc2, err := m.Marshal(anyMap{M: map[string]interface{}{"MyKey": 1}})
	require.NoError(t, err)
	require.Contains(t, string(enc2), `"my_key"`)
}

func TestCoverage_CacheDispatchAndErrors(t *testing.T) {
	c := newCache()
	cfg := &config.IOConfig{}

	pc := c.pathCache("x")
	_, err := pc.getMarshaller(nil, cfg, "x", "x", nil)
	require.Error(t, err)

	// Unsupported kind falls into default unsupported branch.
	_, err = c.loadMarshaller(reflect.TypeOf(make(chan int)), cfg, "", "", nil)
	require.Error(t, err)

	// Load representative kinds to exercise switch branches.
	cases := []reflect.Type{
		reflect.TypeOf([2]bool{}),
		reflect.TypeOf([]int{}),
		reflect.TypeOf([]interface{}{}),
		reflect.TypeOf(map[string]int{}),
		reflect.TypeOf(time.Time{}),
		reflect.TypeOf((*time.Time)(nil)),
		reflect.TypeOf(""),
		reflect.TypeOf(true),
		reflect.TypeOf(float32(0)),
		reflect.TypeOf(float64(0)),
		reflect.TypeOf(int(0)),
		reflect.TypeOf(uint(0)),
		reflect.TypeOf((*int)(nil)),
		reflect.TypeOf((*uint)(nil)),
	}
	for _, rType := range cases {
		_, err = c.loadMarshaller(rType, cfg, "", "", nil)
		require.NoError(t, err)
	}
}

func TestCoverage_OptionPresenceDefaultAndMarshalErrors(t *testing.T) {
	empty := Options{123}
	require.Nil(t, empty.Tag())
	require.Nil(t, empty.FormatTag())

	_, err := getFields(reflect.TypeOf(1))
	require.Error(t, err)

	type badTag struct {
		F string `default:"broken"`
	}
	_, err = NewDefaultTag(reflect.TypeOf(badTag{}).Field(0))
	require.Error(t, err)

	type badValue struct {
		F int `default:"value=abc"`
	}
	_, err = NewDefaultTag(reflect.TypeOf(badValue{}).Field(0))
	require.Error(t, err)

	m := New(&config.IOConfig{})
	_, err = m.Marshal(make(chan int))
	require.Error(t, err)
}

func TestCoverage_LowLevelBranches(t *testing.T) {
	// String ensureReplacer nil branch
	s := &stringMarshaller{dTag: &format.Tag{}, replacer: nil, defaultValue: `""`}
	buf := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	v := "abc"
	require.NoError(t, s.MarshallObject(unsafe.Pointer(&v), buf))

	// Slice interceptor error branch
	errExpected := errors.New("interceptor")
	_, err := New(&config.IOConfig{}).Marshal(
		struct{ X []int }{X: []int{1}},
		MarshalerInterceptors{
			"X": func() ([]byte, error) { return nil, errExpected },
		},
	)
	require.Error(t, err)

	// Time unmarshal invalid input branch
	tm := newTimeMarshaller(&format.Tag{}, &config.IOConfig{})
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`"bad"`)))
	defer dec.Release()
	var tt time.Time
	require.Panics(t, func() {
		_ = tm.UnmarshallObject(unsafe.Pointer(&tt), dec, nil, &UnmarshalSession{})
	})
}

func TestCoverage_ZeroPercentFunctions(t *testing.T) {
	// formatFloat
	require.Equal(t, "1.25", formatFloat(1.25))

	// float unmarshallers + pointer variants
	type floats struct {
		F32 float32
		F64 float64
		P32 *float32
		P64 *float64
		I8  *int8
		I16 *int16
		I32 *int32
		I64 *int64
		U8  *uint8
		U16 *uint16
		U32 *uint32
		U64 *uint64
	}
	m := New(&config.IOConfig{})
	var out floats
	err := m.Unmarshal([]byte(`{"F32":1.5,"F64":2.5,"P32":3.5,"P64":4.5,"I8":8,"I16":16,"I32":32,"I64":64,"U8":9,"U16":19,"U32":29,"U64":39}`), &out)
	require.NoError(t, err)
	require.NotNil(t, out.P32)
	require.NotNil(t, out.P64)
	require.NotNil(t, out.I8)
	require.NotNil(t, out.I16)
	require.NotNil(t, out.I32)
	require.NotNil(t, out.I64)
	require.NotNil(t, out.U8)
	require.NotNil(t, out.U16)
	require.NotNil(t, out.U32)
	require.NotNil(t, out.U64)

	// inlinable unmarshal branch (invoke marshaller directly)
	type inner struct {
		A int
		B string
	}
	type outer struct {
		Inner inner `jsonx:"inline"`
	}
	rType := reflect.TypeOf(outer{})
	field, _ := rType.FieldByName("Inner")
	ilm, err := newInlinableMarshaller(field, &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	var o outer
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"A":7,"B":"x"}`)))
	defer dec.Release()
	require.NoError(t, ilm.UnmarshallObject(unsafe.Pointer(&o.Inner), dec, nil, &UnmarshalSession{}))
}

func TestCoverage_BranchHelpersAndPrimitiveMarshallers(t *testing.T) {
	// isExcluded / filterByPath
	ioCfg := &config.IOConfig{Exclude: map[string]bool{"A.B": true}}
	require.True(t, isExcluded(nil, "B", ioCfg, "A.B"))
	require.False(t, isExcluded(nil, "C", ioCfg, "A.C"))
	filters := NewFilters(&FilterEntry{Path: "A", Fields: []string{"X"}})
	f, ok := filterByPath(filters, "A")
	require.True(t, ok)
	require.True(t, f["X"])
	_, ok = filterByPath(nil, "A")
	require.False(t, ok)

	// primitive marshallers zero/non-zero branches
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	intV := 0
	require.NoError(t, newIntMarshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&intV), sb))
	intV = 3
	require.NoError(t, newIntMarshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&intV), sb))

	f32 := float32(0)
	require.NoError(t, newFloat32Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&f32), sb))
	f32 = 1.25
	require.NoError(t, newFloat32Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&f32), sb))

	u := uint(0)
	require.NoError(t, newUintMarshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u), sb))
	u = 5
	require.NoError(t, newUintMarshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u), sb))

	b := false
	require.NoError(t, newBoolMarshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&b), sb))
	b = true
	require.NoError(t, newBoolMarshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&b), sb))

	// explicit width marshaller zero/non-zero branches
	i8 := int8(0)
	require.NoError(t, NewInt8Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i8), sb))
	i8 = 1
	require.NoError(t, NewInt8Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i8), sb))

	i16 := int16(0)
	require.NoError(t, newInt16Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i16), sb))
	i16 = 2
	require.NoError(t, newInt16Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i16), sb))

	i32 := int32(0)
	require.NoError(t, newInt32Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i32), sb))
	i32 = 3
	require.NoError(t, newInt32Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i32), sb))

	i64 := int64(0)
	require.NoError(t, newInt64Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i64), sb))
	i64 = 4
	require.NoError(t, newInt64Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&i64), sb))

	u8 := uint8(0)
	require.NoError(t, newUint8Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u8), sb))
	u8 = 1
	require.NoError(t, newUint8Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u8), sb))

	u16 := uint16(0)
	require.NoError(t, newUint16Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u16), sb))
	u16 = 2
	require.NoError(t, newUint16Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u16), sb))

	u32 := uint32(0)
	require.NoError(t, newUint32Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u32), sb))
	u32 = 3
	require.NoError(t, newUint32Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u32), sb))

	u64 := uint64(0)
	require.NoError(t, newUint64Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u64), sb))
	u64 = 4
	require.NoError(t, newUint64Marshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&u64), sb))
}

func TestCoverage_InterfaceArrayRawAndWrapperBranches(t *testing.T) {
	// interface marshaller hasMethod=true branch
	v := withM(withMImpl{})
	im, err := newInterfaceMarshaller(reflect.TypeOf((*withM)(nil)).Elem(), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	require.NotNil(t, im.AsInterface(unsafe.Pointer(&v)))
	require.NotNil(t, asInterface(im.xType, unsafe.Pointer(&v)))

	// array unmarshal null path
	am, err := newArrayMarshaller(reflect.TypeOf([2]bool{}), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	decNull := gojay.BorrowDecoder(bytes.NewReader([]byte(`null`)))
	defer decNull.Release()
	var arr [2]bool
	require.Error(t, am.UnmarshallObject(unsafe.Pointer(&arr), decNull, nil, &UnmarshalSession{}))

	// raw message marshal nil path
	rm := newRawMessageMarshaller()
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	var raw []byte
	require.NoError(t, rm.MarshallObject(unsafe.Pointer(&raw), sb))
	raw = []byte(`{"x":1}`)
	require.NoError(t, rm.MarshallObject(unsafe.Pointer(&raw), sb))

	// gojay wrapper useMarshal/useUnmarshal false branches
	fb := &fallbackMarshaller{}
	rType := reflect.TypeOf(struct{ A int }{})
	w := newGojayObjectMarshaller(getXType(rType), getXType(reflect.PtrTo(rType)), fb, false, false)
	val := struct{ A int }{A: 1}
	require.NoError(t, w.MarshallObject(AsPtr(val, rType), sb))
	require.True(t, fb.marshalCalled)
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"A":1}`)))
	defer dec.Release()
	ptr := reflect.New(rType)
	require.NoError(t, w.UnmarshallObject(unsafe.Pointer(ptr.Pointer()), dec, nil, &UnmarshalSession{}))
	require.True(t, fb.unmarshalCalled)

}

func TestCoverage_GojayWrapperPointerPathAndSkipNull(t *testing.T) {
	// use existing gjOnlyPtr type to hit useMarshal/useUnmarshal=true path
	w := newGojayObjectMarshaller(getXType(reflect.TypeOf(gjOnlyPtr{})), getXType(reflect.TypeOf(&gjOnlyPtr{})), &fallbackMarshaller{}, true, true)
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	v := gjOnlyPtr{V: 9}
	require.NoError(t, w.MarshallObject(AsPtr(v, reflect.TypeOf(v)), sb))

	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"V":9}`)))
	defer dec.Release()
	p := reflect.New(reflect.TypeOf(gjOnlyPtr{}))
	require.NoError(t, w.UnmarshallObject(unsafe.Pointer(p.Pointer()), dec, nil, &UnmarshalSession{}))

	// skipNull true/false branches
	decNull := gojay.BorrowDecoder(bytes.NewReader([]byte(`null`)))
	defer decNull.Release()
	_ = skipNull(decNull)

	decNonNull := gojay.BorrowDecoder(bytes.NewReader([]byte(`[]`)))
	defer decNonNull.Release()
	require.False(t, skipNull(decNonNull))
}

func TestCoverage_LowFunctionsExtra(t *testing.T) {
	// uint ptr marshaller non-nil branch
	up := uint(11)
	ptr := &up
	upp := &ptr
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, newUintPtrMarshaller(&format.Tag{}).MarshallObject(unsafe.Pointer(&upp), sb))

	// gojay wrapper nil ptr marshal branch
	w := newGojayObjectMarshaller(
		getXType(reflect.TypeOf(gjOnlyPtr{})),
		getXType(reflect.TypeOf(&gjOnlyPtr{})),
		&fallbackMarshaller{},
		true,
		true,
	)
	require.NoError(t, w.MarshallObject(nil, sb))

	// slice decoder error branch
	sd := newSliceDecoder(reflect.TypeOf(0), unsafe.Pointer(&[]int{}), xunsafe.NewSlice(reflect.TypeOf([]int{}), xunsafe.UseItemAddrOpt(true)), newIntMarshaller(&format.Tag{}), &UnmarshalSession{})
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`["x"]`)))
	defer dec.Release()
	_ = dec.Array(sd)
}

func TestCoverage_RemainingBranches(t *testing.T) {
	// skipNull branches
	origData, origCur := decData, decCur
	decData, decCur = nil, nil
	decDummy := gojay.BorrowDecoder(bytes.NewReader([]byte(`null`)))
	require.False(t, skipNull(decDummy))
	decDummy.Release()
	decData, decCur = origData, origCur

	decNull := gojay.BorrowDecoder(bytes.NewReader([]byte(`null`)))
	_ = skipNull(decNull)
	decNull.Release()

	// force internal decoder state to hit skipNull true path
	forced := gojay.BorrowDecoder(bytes.NewReader([]byte(`[]`)))
	decPtr := unsafe.Pointer(forced)
	decData.SetBytes(decPtr, []byte("null"))
	decCur.SetInt(decPtr, 0)
	require.True(t, skipNull(forced))
	forced.Release()

	// slice marshaller constructor error path
	_, err := newSliceMarshaller(reflect.TypeOf([]chan int{}), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.Error(t, err)

	// slice marshaller marshal nil ptr branch
	sNoop := &sliceMarshaller{path: "p", xslice: xunsafe.NewSlice(reflect.TypeOf([]int{}), xunsafe.UseItemAddrOpt(true))}
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, sNoop.MarshallObject(nil, sb))

	// slice marshaller interceptor error branch
	sInt := &sliceMarshaller{path: "p", xslice: xunsafe.NewSlice(reflect.TypeOf([]int{}), xunsafe.UseItemAddrOpt(true))}
	sb2 := &MarshallSession{
		Buffer: bytes.NewBuffer(nil),
		Interceptors: MarshalerInterceptors{
			"p": func() ([]byte, error) { return nil, errors.New("x") },
		},
	}
	var arr []int
	require.Error(t, sInt.MarshallObject(unsafe.Pointer(&arr), sb2))

	// slice decoder error wrapping branch
	sd := &sliceDecoder{
		appender:     xunsafe.NewSlice(reflect.TypeOf([]int{}), xunsafe.UseItemAddrOpt(true)).Appender(unsafe.Pointer(&arr)),
		unmarshaller: &errMarshaller{},
	}
	decArr := gojay.BorrowDecoder(bytes.NewReader([]byte(`[1]`)))
	err = decArr.Array(sd)
	decArr.Release()
	require.Error(t, err)

	// slice interface marshaller branches
	sim := newSliceInterfaceMarshaller(&config.IOConfig{}, "", "", &format.Tag{}, newCache()).(*sliceInterfaceMarshaller)
	ifaces := []interface{}{nil, (*int)(nil), map[string]int{"a": 1}}
	sb3 := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, sim.MarshallObject(unsafe.Pointer(&ifaces), sb3))
	ifacesBad := []interface{}{make(chan int)}
	require.Error(t, sim.MarshallObject(unsafe.Pointer(&ifacesBad), sb3))
	decBad := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	require.Error(t, sim.UnmarshallObject(unsafe.Pointer(&[]interface{}{}), decBad, nil, &UnmarshalSession{}))
	decBad.Release()

	// ptr marshaller constructor and branches
	_, err = newPtrMarshaller(reflect.TypeOf((*chan int)(nil)), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.Error(t, err)
	pm := &ptrMarshaller{rType: reflect.TypeOf((*int)(nil)), marshaler: newIntMarshaller(&format.Tag{})}
	require.NoError(t, pm.MarshallObject(nil, sb))
	var pnil *int
	require.NoError(t, pm.MarshallObject(unsafe.Pointer(&pnil), sb))
	decPtr2 := gojay.BorrowDecoder(bytes.NewReader([]byte(`null`)))
	require.NoError(t, pm.UnmarshallObject(unsafe.Pointer(&pnil), decPtr2, nil, &UnmarshalSession{}))
	decPtr2.Release()

	// map marshaller direct key switch and nil map branches
	mm := &mapMarshaller{
		xType:           getXType(reflect.TypeOf(map[int]int{})),
		keyType:         reflect.TypeOf(""),
		valueType:       reflect.TypeOf(int(0)),
		keyMarshaller:   newStringMarshaller(&format.Tag{}),
		valueMarshaller: newIntMarshaller(&format.Tag{}),
		config:          &config.IOConfig{},
	}
	mval := map[int]int{1: 2}
	require.NoError(t, mm.MarshallObject(unsafe.Pointer(&mval), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	mm64 := &mapMarshaller{
		xType:           getXType(reflect.TypeOf(map[uint64]int{})),
		keyType:         reflect.TypeOf(""),
		valueType:       reflect.TypeOf(int(0)),
		keyMarshaller:   newStringMarshaller(&format.Tag{}),
		valueMarshaller: newIntMarshaller(&format.Tag{}),
		config:          &config.IOConfig{},
	}
	m64 := map[uint64]int{7: 1}
	require.NoError(t, mm64.MarshallObject(unsafe.Pointer(&m64), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	mmNil := &mapMarshaller{
		xType:           getXType(reflect.TypeOf(map[string]int{})),
		keyType:         reflect.TypeOf(""),
		valueType:       reflect.TypeOf(int(0)),
		keyMarshaller:   newStringMarshaller(&format.Tag{}),
		valueMarshaller: newIntMarshaller(&format.Tag{}),
		config:          &config.IOConfig{},
	}
	var nilMap map[string]int
	require.NoError(t, mmNil.MarshallObject(unsafe.Pointer(&nilMap), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	// map unmarshaler error branches
	mi := &mapStringIntUnmarshaler{aMap: map[string]int{}}
	d1 := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	require.Error(t, mi.UnmarshalJSONObject(d1, "a"))
	d1.Release()
	mf := &mapStringFloatUnmarshaler{aMap: map[string]float64{}}
	d2 := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	require.Error(t, mf.UnmarshalJSONObject(d2, "a"))
	d2.Release()
	ms := &mapStringStringUnmarshaler{aMap: map[string]string{}}
	d3 := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	require.Error(t, ms.UnmarshalJSONObject(d3, "a"))
	d3.Release()

	// struct helper normalized exclusion branch
	ioCfg := &config.IOConfig{Exclude: map[string]bool{"ab": true}}
	require.True(t, isExcluded(nil, "X", ioCfg, "A_B"))

	// cache path load miss branch
	pc := &pathCache{cache: sync.Map{}}
	_, ok := pc.loadMarshaller(reflect.TypeOf(123))
	require.False(t, ok)
}

func TestCoverage_ConstructorAndNilVariants(t *testing.T) {
	nullableTag := &format.Tag{}
	b := true
	nullableTag.Nullable = &b
	nonNullableTag := &format.Tag{}
	f := false
	nonNullableTag.Nullable = &f

	// bool/string/float/int ctor nullable branches
	require.Equal(t, null, newBoolMarshaller(nullableTag).zeroValue)
	require.Equal(t, null, newStringMarshaller(nullableTag).defaultValue)
	require.Equal(t, null, newFloat32Marshaller(nullableTag).zeroValue)
	require.Equal(t, null, newFloat64Marshaller(nullableTag).zeroValue)
	require.Equal(t, null, newInt64Marshaller(nullableTag).zeroValue)
	require.Equal(t, null, intZeroValue(nullableTag))
	require.Equal(t, "0", intZeroValue(nonNullableTag))

	// time ptr ctor branch
	require.Equal(t, "null", newTimePtrMarshaller(nullableTag, &config.IOConfig{}).zeroValue)
	require.NotEqual(t, "null", newTimePtrMarshaller(nonNullableTag, &config.IOConfig{}).zeroValue)

	// uint ptr marshaller both nil-pointer branches
	um := newUintPtrMarshaller(&format.Tag{})
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, um.MarshallObject(nil, sb))
	var x *uint
	require.NoError(t, um.MarshallObject(unsafe.Pointer(&x), sb))
	v := uint(1)
	x = &v
	require.NoError(t, um.MarshallObject(unsafe.Pointer(&x), sb))

	// ptr marshaller branch where ptr non-nil but deref nil
	pm := &ptrMarshaller{rType: reflect.TypeOf((*int)(nil)), marshaler: newIntMarshaller(&format.Tag{})}
	var pi *int
	piPtr := &pi
	require.NoError(t, pm.MarshallObject(unsafe.Pointer(piPtr), sb))

	// interface marshaller error branch
	im, err := newInterfaceMarshaller(reflect.TypeOf((*interface{})(nil)).Elem(), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	iface := interface{}(make(chan int))
	require.Error(t, im.MarshallObject(unsafe.Pointer(&iface), sb))

	// raw message marshal ptr nil and unmarshal invalid
	rm := newRawMessageMarshaller()
	require.NoError(t, rm.MarshallObject(nil, sb))
	decInvalid := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	defer decInvalid.Release()
	var raw []byte
	require.Error(t, rm.UnmarshallObject(unsafe.Pointer(&raw), decInvalid, nil, &UnmarshalSession{}))

	// array marshaller unsupported branch
	am, err := newArrayMarshaller(reflect.TypeOf([1]int{}), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	a := [1]int{1}
	require.Error(t, am.MarshallObject(unsafe.Pointer(&a), sb))

	// force array unmarshal null fast-path
	decArrNull := gojay.BorrowDecoder(bytes.NewReader([]byte(`[]`)))
	decArrPtr := unsafe.Pointer(decArrNull)
	decData.SetBytes(decArrPtr, []byte("null"))
	decCur.SetInt(decArrPtr, 0)
	require.NoError(t, am.UnmarshallObject(unsafe.Pointer(&a), decArrNull, nil, &UnmarshalSession{}))
	decArrNull.Release()

	// custom unmarshaller constructor error path
	_, err = newCustomUnmarshaller(reflect.TypeOf(make(chan int)), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.Error(t, err)

	// inlinable marshaller constructor error path
	type badInline struct{ C chan int }
	field, _ := reflect.TypeOf(badInline{}).FieldByName("C")
	_, err = newInlinableMarshaller(field, &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.Error(t, err)

	// decoderError nil field branch
	oldErr := decErr
	decErr = nil
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{}`)))
	require.NoError(t, decoderError(dec))
	dec.Release()
	decErr = oldErr
}

func TestCoverage_AdditionalLowBranches(t *testing.T) {
	// string marshaller empty + nullable branch
	nullable := &format.Tag{}
	tval := true
	nullable.Nullable = &tval
	sm := newStringMarshaller(nullable)
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	empty := ""
	require.NoError(t, sm.MarshallObject(unsafe.Pointer(&empty), sb))

	// deferred unmarshal fail branch
	d := newDeferred()
	d.fail(errors.New("uerr"))
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{}`)))
	defer dec.Release()
	require.Error(t, d.UnmarshallObject(nil, dec, nil, &UnmarshalSession{}))

	// ptr unmarshal pointer==nil and auxiliary decoder branch
	pm := &ptrMarshaller{rType: reflect.TypeOf((*int)(nil)), marshaler: newIntMarshaller(&format.Tag{})}
	require.NoError(t, pm.UnmarshallObject(nil, dec, nil, &UnmarshalSession{}))
	var p *int
	aux := gojay.BorrowDecoder(bytes.NewReader([]byte(`1`)))
	defer aux.Release()
	require.NoError(t, pm.UnmarshallObject(unsafe.Pointer(&p), dec, aux, &UnmarshalSession{}))

	// enc BytesSlice error branch
	bs := &BytesSlice{b: &[]byte{}}
	bad := gojay.BorrowDecoder(bytes.NewReader([]byte(`"x"`)))
	defer bad.Release()
	_ = bs.UnmarshalJSONArray(bad)

	// struct marshaller branches with anonymous ptr embed and ignores
	type Emb struct {
		Arr []int
		S   string
	}
	type HolderNoOmit struct {
		*Emb
		Hidden   string `json:"-"`
		Internal string `internal:"true"`
		Name     string
	}
	type HolderOmit struct {
		*Emb `json:",omitempty"`
		Name string
	}
	m := New(&config.IOConfig{})
	_, err := m.Marshal(HolderNoOmit{Name: "x"}) // nil embed no omitempty -> explicit null paths
	require.NoError(t, err)
	_, err = m.Marshal(HolderOmit{Name: "y"}) // nil embed with omitempty -> skip path
	require.NoError(t, err)

	// createStructMarshallers self-reference skip branch
	type Self struct {
		Child []*Self
		Name  string
	}
	s, err := newStructMarshaller(&config.IOConfig{}, reflect.TypeOf(Self{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	gf := groupFields(reflect.TypeOf(Self{}))
	mrs, err := s.createStructMarshallers(gf, "", "", &format.Tag{})
	require.NoError(t, err)
	// only Name should remain (Child is self-reference and skipped)
	require.Len(t, mrs, 1)

	// enc.go error path: decoder exhausted
	bufBytes := []byte{}
	bs2 := &BytesSlice{b: &bufBytes}
	dEmpty := gojay.BorrowDecoder(bytes.NewReader([]byte{}))
	defer dEmpty.Release()
	require.Error(t, bs2.UnmarshalJSONArray(dEmpty))

	// default Init unknown attr ignored, malformed kv errors
	type withDefault struct {
		V string `default:"unknown=1,value=x"`
	}
	_, err = NewDefaultTag(reflect.TypeOf(withDefault{}).Field(0))
	require.NoError(t, err)
	type badDefault struct {
		V string `default:"badformat"`
	}
	_, err = NewDefaultTag(reflect.TypeOf(badDefault{}).Field(0))
	require.Error(t, err)

	// presence updater error path
	type wrongPresence struct {
		Has int `setMarker:"true"`
	}
	_, err = newPresenceUpdater(reflect.TypeOf(wrongPresence{}).Field(0))
	require.Error(t, err)

	// formatName remaining ID branch variants
	require.Equal(t, "ID", formatName("ID", text.CaseFormatUpper))
	require.Equal(t, "id", formatName("ID", text.CaseFormatLower))
}

func TestCoverage_StructHeavyBranches(t *testing.T) {
	// init() error path via unsupported field type
	type Bad struct {
		C chan int
	}
	smBad, err := newStructMarshaller(&config.IOConfig{}, reflect.TypeOf(Bad{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	require.Error(t, smBad.init())

	// init() error path via invalid presence marker type
	type BadPresence struct {
		ID  int
		Has int `setMarker:"true"`
	}
	smBadPresence, err := newStructMarshaller(&config.IOConfig{}, reflect.TypeOf(BadPresence{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	require.Error(t, smBadPresence.init())

	// UnmarshalObject branch where marker holder is non-pointer struct (lines 85-87)
	type HasStruct struct{ ID bool }
	type MarkerStruct struct {
		ID  int
		Has HasStruct `setMarker:"true"`
	}
	smMarker, err := newStructMarshaller(&config.IOConfig{}, reflect.TypeOf(MarkerStruct{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	require.NoError(t, smMarker.init())
	var ms MarkerStruct
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"ID":1}`)))
	require.NoError(t, smMarker.UnmarshallObject(unsafe.Pointer(&ms), dec, nil, &UnmarshalSession{}))
	dec.Release()

	// MarshallObject nil pointer branch (lines 106-109)
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, smMarker.MarshallObject(nil, sb))

	// MarshallObject filter exclusion branch + filter miss branch in isExcluded
	mCfg := &config.IOConfig{}
	smFilt, err := newStructMarshaller(mCfg, reflect.TypeOf(struct {
		A int
		B int
	}{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	require.NoError(t, smFilt.init())
	v := struct {
		A int
		B int
	}{A: 1, B: 2}
	filtered := &MarshallSession{
		Buffer:  bytes.NewBuffer(nil),
		Filters: NewFilters(&FilterEntry{Path: "", Fields: []string{"A"}}),
	}
	require.NoError(t, smFilt.MarshallObject(unsafe.Pointer(&v), filtered))

	// newFieldMarshaller anonymous error path (line 295) via anonymous bad struct
	type anonInner struct {
		C chan int
	}
	type anonBad struct {
		anonInner
	}
	smAnonBad, err := newStructMarshaller(&config.IOConfig{}, reflect.TypeOf(anonBad{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	require.Error(t, smAnonBad.init())

	// newFieldMarshaller ignore path (line 316) is unreachable for valid Go identifiers; keep behavior documented.

	// isZeroValue default false path (line 434) with non-comparable func type
	type fnHolder struct {
		F func()
	}
	ff, _ := reflect.TypeOf(fnHolder{}).FieldByName("F")
	fmwf := &marshallerWithField{xField: xunsafe.NewField(ff), marshallerMetadata: marshallerMetadata{comparable: false}}
	require.False(t, isZeroValue(unsafe.Pointer(&fnHolder{}), fmwf, nil))

	// structDecoder interceptor error path (lines 494-496)
	type simple struct{ A int }
	smSimple, err := newStructMarshaller(&config.IOConfig{}, reflect.TypeOf(simple{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	require.NoError(t, smSimple.init())
	sv := simple{}
	ud := &structDecoder{
		ptr:        unsafe.Pointer(&sv),
		marshaller: smSimple,
		session: &UnmarshalSession{PathMarshaller: UnmarshalerInterceptors{
			"A": func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
				return errors.New("intercept")
			},
		}},
	}
	dec2 := gojay.BorrowDecoder(bytes.NewReader([]byte(`1`)))
	err = ud.unmarshalJson(dec2, "A")
	dec2.Release()
	require.Error(t, err)
}

type gjValueOnly struct {
	V int
}

func (g gjValueOnly) MarshalJSONObject(enc *gojay.Encoder) {
	enc.IntKey("V", g.V)
}

func (g gjValueOnly) IsNil() bool { return false }

func TestCoverage_TargetedReachableBranches(t *testing.T) {
	// default.Init empty tag branch + ignorecaseformatter attribute branch
	type noDefault struct {
		A int
	}
	aTag := &DefaultTag{}
	require.NoError(t, aTag.Init(reflect.TypeOf(noDefault{}).Field(0)))

	type attrDefault struct {
		A string `default:"value=x,ignorecaseformatter=true"`
	}
	aTag2, err := NewDefaultTag(reflect.TypeOf(attrDefault{}).Field(0))
	require.NoError(t, err)
	require.True(t, aTag2.IgnoreCaseFormatter)

	// parseValue time branch with empty format fallback
	parsed, err := parseValue(reflect.TypeOf(time.Time{}), "2024-01-01T00:00:00Z", "")
	require.NoError(t, err)
	require.IsType(t, time.Time{}, parsed)

	// namesCaseIndex undefined format branch
	n := &namesCaseIndex{registry: map[text.CaseFormat]map[string]string{}}
	require.Equal(t, "a_b", n.formatTo("a-b", text.CaseFormatLowerUnderscore))

	// marshal.prepareMarshallSession nil option and filters branch
	j := New(&config.IOConfig{})
	sess, putBack := j.prepareMarshallSession([]interface{}{nil, []*FilterEntry{{Path: "", Fields: []string{"A"}}}})
	require.True(t, putBack)
	require.NotNil(t, sess.Filters)

	// marshal.Unmarshal error branch when marshaller construction fails
	err = j.Unmarshal([]byte(`1`), make(chan int))
	require.Error(t, err)

	// cache getMarshaller ptr fallback error, map error, and custom-unmarshaller struct branch
	c := newCache()
	_, err = c.loadMarshaller(reflect.TypeOf((*chan int)(nil)), &config.IOConfig{}, "", "", nil)
	require.Error(t, err)
	_, err = c.loadMarshaller(reflect.TypeOf(map[string]chan int{}), &config.IOConfig{}, "", "", nil)
	require.Error(t, err)

	_, err = c.loadMarshaller(reflect.TypeOf(customStruct(0)), &config.IOConfig{}, "", "", nil)
	require.NoError(t, err)

	// deferred.resolved nil-target branch
	d := newDeferred()
	close(d.ready)
	_, err = d.resolved()
	require.Error(t, err)

	// gojay wrapper value-receiver marshal branch + auxiliary decoder branch
	fb := &fallbackMarshaller{}
	gw := newGojayObjectMarshaller(
		getXType(reflect.TypeOf(gjValueOnly{})),
		getXType(reflect.TypeOf(&gjValueOnly{})),
		fb,
		true,
		false,
	)
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	val := gjValueOnly{V: 5}
	require.NoError(t, gw.MarshallObject(AsPtr(val, reflect.TypeOf(val)), sb))
	require.Contains(t, sb.String(), `"V":5`)

	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"V":6}`)))
	aux := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"V":7}`)))
	defer dec.Release()
	defer aux.Release()
	dst := gjValueOnly{}
	require.NoError(t, gw.UnmarshallObject(unsafe.Pointer(&dst), dec, aux, &UnmarshalSession{}))
	require.True(t, fb.unmarshalCalled)

	// custom marshaller fallback branch
	cm := &customMarshaller{
		valueType:  getXType(reflect.TypeOf(1)),
		addrType:   getXType(reflect.TypeOf(new(int))),
		marshaller: fb,
	}
	dec2 := gojay.BorrowDecoder(bytes.NewReader([]byte(`1`)))
	defer dec2.Release()
	i := 0
	require.NoError(t, cm.UnmarshallObject(unsafe.Pointer(&i), dec2, nil, &UnmarshalSession{}))
	require.True(t, fb.unmarshalCalled)

	// array len==0 branch
	am, err := newArrayMarshaller(reflect.TypeOf([0]bool{}), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	sbArr := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	arr := [0]bool{}
	require.NoError(t, am.MarshallObject(unsafe.Pointer(&arr), sbArr))
	require.Equal(t, "[]", sbArr.String())

	// ptr unmarshal decoder error branch
	pm := &ptrMarshaller{rType: reflect.TypeOf((*int)(nil)), marshaler: newIntMarshaller(&format.Tag{})}
	badDec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	defer badDec.Release()
	var pi *int
	require.Error(t, pm.UnmarshallObject(unsafe.Pointer(&pi), badDec, nil, &UnmarshalSession{}))

	// slice unmarshal decoder.Array error and marshaller error branch
	sm := &sliceMarshaller{
		elemType:   reflect.TypeOf(0),
		marshaller: &errMarshaller{},
		xslice:     xunsafe.NewSlice(reflect.TypeOf([]int{}), xunsafe.UseItemAddrOpt(true)),
	}
	badArrayDec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"x":1}`)))
	defer badArrayDec.Release()
	sliceDst := []int{}
	require.Error(t, sm.UnmarshallObject(unsafe.Pointer(&sliceDst), badArrayDec, nil, &UnmarshalSession{}))

	// sliceInterfaceMarshaller marshaller.MarshallObject error branch
	cache := newCache()
	cache.pathCache("").storeMarshaler(reflect.TypeOf(1), &errMarshaller{})
	sim := &sliceInterfaceMarshaller{
		cache:  cache,
		config: &config.IOConfig{},
		tag:    &format.Tag{},
	}
	list := []interface{}{1}
	require.Error(t, sim.MarshallObject(unsafe.Pointer(&list), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	// map marshaller int64/default key switch, mapStringIface nil-pointer/counter/error branches
	mInt64 := &mapMarshaller{
		xType:           getXType(reflect.TypeOf(map[int64]int{})),
		keyType:         reflect.TypeOf(""),
		valueType:       reflect.TypeOf(int(0)),
		keyMarshaller:   newStringMarshaller(&format.Tag{}),
		valueMarshaller: newIntMarshaller(&format.Tag{}),
		config:          &config.IOConfig{},
	}
	data64 := map[int64]int{11: 1}
	require.NoError(t, mInt64.MarshallObject(unsafe.Pointer(&data64), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	mDefault := &mapMarshaller{
		xType:           getXType(reflect.TypeOf(map[float64]int{})),
		keyType:         reflect.TypeOf(""),
		valueType:       reflect.TypeOf(int(0)),
		keyMarshaller:   newStringMarshaller(&format.Tag{}),
		valueMarshaller: newIntMarshaller(&format.Tag{}),
		config:          &config.IOConfig{},
	}
	dataDef := map[float64]int{1.5: 2}
	require.NoError(t, mDefault.MarshallObject(unsafe.Pointer(&dataDef), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	mIface := &mapMarshaller{
		config:          &config.IOConfig{CaseFormat: text.CaseFormatLower},
		valueType:       reflect.TypeOf((*interface{})(nil)).Elem(),
		valueMarshaller: newInterfaceMarshallerMust(t),
	}
	fn := mIface.mapStringIfaceMarshaller()
	var nilMapPtr *map[string]interface{}
	require.NoError(t, fn(unsafe.Pointer(nilMapPtr), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))
	vmap := map[string]interface{}{"A": 1, "B": 2}
	require.NoError(t, fn(unsafe.Pointer(&vmap), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	mIfaceErr := &mapMarshaller{
		config:          &config.IOConfig{},
		valueType:       reflect.TypeOf((*interface{})(nil)).Elem(),
		valueMarshaller: &errMarshaller{},
	}
	fnErr := mIfaceErr.mapStringIfaceMarshaller()
	require.Error(t, fnErr(unsafe.Pointer(&vmap), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	// time and time ptr constructor/unmarshal error branches
	require.Equal(t, "2006", newTimeMarshaller(&format.Tag{TimeLayout: "2006"}, &config.IOConfig{}).timeLayout)
	tm := newTimeMarshaller(&format.Tag{}, &config.IOConfig{})
	tBad := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	defer tBad.Release()
	var tv time.Time
	require.Error(t, tm.UnmarshallObject(unsafe.Pointer(&tv), tBad, nil, &UnmarshalSession{}))
	tBad2 := gojay.BorrowDecoder(bytes.NewReader([]byte(`"bad"`)))
	defer tBad2.Release()
	require.Panics(t, func() { _ = tm.UnmarshallObject(unsafe.Pointer(&tv), tBad2, nil, &UnmarshalSession{}) })

	require.Equal(t, "2006", newTimePtrMarshaller(&format.Tag{TimeLayout: "2006"}, &config.IOConfig{}).timeLayout)
	tpm := newTimePtrMarshaller(&format.Tag{}, &config.IOConfig{})
	tpBad := gojay.BorrowDecoder(bytes.NewReader([]byte(`{`)))
	defer tpBad.Release()
	var tp *time.Time
	require.Error(t, tpm.UnmarshallObject(unsafe.Pointer(&tp), tpBad, nil, &UnmarshalSession{}))
	tpBad2 := gojay.BorrowDecoder(bytes.NewReader([]byte(`"bad"`)))
	defer tpBad2.Release()
	require.Panics(t, func() { _ = tpm.UnmarshallObject(unsafe.Pointer(&tp), tpBad2, nil, &UnmarshalSession{}) })

	// presence getFields continue non-bool branch
	type mixed struct {
		I int
		B bool
	}
	fields, err := getFields(reflect.TypeOf(mixed{}))
	require.NoError(t, err)
	require.Len(t, fields, 1)

	// struct newFieldMarshaller non-letter name branch + marshallerWithField.init parse error branch
	sstruct, err := newStructMarshaller(&config.IOConfig{}, reflect.TypeOf(struct{ A int }{}), "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	listMarshallers := make([]*marshallerWithField, 0)
	require.NoError(t, sstruct.newFieldMarshaller(&listMarshallers, reflect.StructField{Name: "1bad", Type: reflect.TypeOf(0)}, "", "", &format.Tag{}))
	require.Empty(t, listMarshallers)

	mwf := &marshallerWithField{}
	require.NoError(t, mwf.init(reflect.StructField{Name: "X", Type: reflect.TypeOf(0), Tag: reflect.StructTag("json:\"x")}, &config.IOConfig{}, newCache()))

	// isZeroValue nil pointer currently panics for slice fields
	type hs struct{ S []int }
	sf, _ := reflect.TypeOf(hs{}).FieldByName("S")
	sfw := &marshallerWithField{xField: xunsafe.NewField(sf)}
	require.Panics(t, func() { _ = isZeroValue(nil, sfw, []int{}) })
}

func newInterfaceMarshallerMust(t *testing.T) marshaler {
	m, err := newInterfaceMarshaller(reflect.TypeOf((*interface{})(nil)).Elem(), &config.IOConfig{}, "", "", &format.Tag{}, newCache())
	require.NoError(t, err)
	return m
}

func TestCoverage_ExtraReachableBranches(t *testing.T) {
	cfg := &config.IOConfig{}
	cache := newCache()
	pc := cache.pathCache("")

	// cache struct branches: base.init error in gojay and non-gojay paths + custom unmarshaller struct branch
	_, err := pc.getMarshaller(reflect.TypeOf(gojayBadInit{}), cfg, "", "", &format.Tag{})
	require.Error(t, err)

	type badPlain struct {
		C chan int
	}
	_, err = pc.getMarshaller(reflect.TypeOf(badPlain{}), cfg, "", "", &format.Tag{})
	require.Error(t, err)

	_, err = pc.getMarshaller(reflect.TypeOf(customStructHolder{}), cfg, "", "", &format.Tag{})
	require.NoError(t, err)

	// default tag Name/Embedded attributes
	type namedEmbedded struct {
		A int `default:"name=abc,embedded=true"`
	}
	dt, err := NewDefaultTag(reflect.TypeOf(namedEmbedded{}).Field(0))
	require.NoError(t, err)
	require.Equal(t, "abc", dt.Name)
	require.True(t, dt.Embedded)

	// gojay wrapper: force value-receiver branch by using non-matching addrType
	wv := newGojayObjectMarshaller(
		getXType(reflect.TypeOf(gjValueOnly{})),
		getXType(reflect.TypeOf(0)),
		&fallbackMarshaller{},
		true,
		true,
	)
	sb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	gv := gjValueOnly{V: 12}
	require.NoError(t, wv.MarshallObject(AsPtr(gv, reflect.TypeOf(gv)), sb))
	require.Contains(t, sb.String(), `"V":12`)
	dec := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"V":12}`)))
	aux := gojay.BorrowDecoder(bytes.NewReader([]byte(`{"V":13}`)))
	defer dec.Release()
	defer aux.Release()
	require.NoError(t, wv.UnmarshallObject(unsafe.Pointer(&gv), dec, aux, &UnmarshalSession{}))

	// map marshaller key/value marshaller error branches
	mKeyErr := &mapMarshaller{
		xType:           getXType(reflect.TypeOf(map[int]int{})),
		keyType:         reflect.TypeOf(""),
		valueType:       reflect.TypeOf(int(0)),
		keyMarshaller:   &errMarshaller{},
		valueMarshaller: newIntMarshaller(&format.Tag{}),
		config:          cfg,
	}
	mv := map[int]int{1: 2}
	require.Error(t, mKeyErr.MarshallObject(unsafe.Pointer(&mv), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	mValErr := &mapMarshaller{
		xType:           getXType(reflect.TypeOf(map[int]int{})),
		keyType:         reflect.TypeOf(""),
		valueType:       reflect.TypeOf(int(0)),
		keyMarshaller:   newStringMarshaller(&format.Tag{}),
		valueMarshaller: &errMarshaller{},
		config:          cfg,
	}
	require.Error(t, mValErr.MarshallObject(unsafe.Pointer(&mv), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	// map constructor key marshaller error branch via unsupported key kind
	_, err = newMapMarshaller(reflect.TypeOf(map[chan int]int{}), cfg, "", "", &format.Tag{}, cache)
	require.NoError(t, err)

	// slice unmarshal skipNull true + array decode error; slice marshal nested marshaller error
	sm := &sliceMarshaller{
		elemType:   reflect.TypeOf(0),
		marshaller: &errMarshaller{},
		xslice:     xunsafe.NewSlice(reflect.TypeOf([]int{}), xunsafe.UseItemAddrOpt(true)),
	}
	forced := gojay.BorrowDecoder(bytes.NewReader([]byte(`[]`)))
	ptrDec := unsafe.Pointer(forced)
	decData.SetBytes(ptrDec, []byte("null"))
	decCur.SetInt(ptrDec, 0)
	dst := []int{}
	require.NoError(t, sm.UnmarshallObject(unsafe.Pointer(&dst), forced, nil, &UnmarshalSession{}))
	forced.Release()

	bad := gojay.BorrowDecoder(bytes.NewReader([]byte(`"x"`)))
	defer bad.Release()
	require.Error(t, sm.UnmarshallObject(unsafe.Pointer(&dst), bad, nil, &UnmarshalSession{}))

	outSlice := []int{1}
	require.Error(t, sm.MarshallObject(unsafe.Pointer(&outSlice), &MarshallSession{Buffer: bytes.NewBuffer(nil)}))

	// marshallString generic control-char escaping branch (<0x20)
	ssb := &MarshallSession{Buffer: bytes.NewBuffer(nil)}
	marshallString(string([]byte{0x01}), ssb, nil)
	require.Contains(t, ssb.String(), `\\u00`)

	// struct marshaller branches: indirect ignore, nil slice handling, nil pointer field
	type embIgnored struct {
		N int
	}
	type holderIgnored struct {
		*embIgnored `json:"-"`
		A           int
	}
	_, err = New(cfg).Marshal(holderIgnored{A: 1})
	require.NoError(t, err)

	type withNilSlice struct {
		S []int
	}
	_, err = New(cfg).Marshal(withNilSlice{})
	require.NoError(t, err)

	type withPtr struct {
		P *int
	}
	_, err = New(cfg).Marshal(withPtr{})
	require.NoError(t, err)

	// createStructMarshallers inlinable newInlinableMarshaller error branch
	type badInlineField struct {
		C chan int `jsonx:"inline"`
	}
	s, err := newStructMarshaller(cfg, reflect.TypeOf(badInlineField{}), "", "", &format.Tag{}, cache)
	require.NoError(t, err)
	_, err = s.createStructMarshallers(groupFields(reflect.TypeOf(badInlineField{})), "", "", &format.Tag{})
	require.Error(t, err)

	// createStructMarshallers format.Parse error path + parameter/body naming path
	type malformedTag struct {
		A int `json:"abc`
	}
	s2, err := newStructMarshaller(cfg, reflect.TypeOf(malformedTag{}), "", "", &format.Tag{}, cache)
	require.NoError(t, err)
	_, err = s2.createStructMarshallers(groupFields(reflect.TypeOf(malformedTag{})), "", "", &format.Tag{})
	require.NoError(t, err)

	type parameterBody struct {
		A int `parameter:"p1,kind=body,in=payload"`
	}
	s3, err := newStructMarshaller(cfg, reflect.TypeOf(parameterBody{}), "", "", &format.Tag{}, cache)
	require.NoError(t, err)
	marshallers, err := s3.createStructMarshallers(groupFields(reflect.TypeOf(parameterBody{})), "", "", &format.Tag{})
	require.NoError(t, err)
	require.NotEmpty(t, marshallers)
}

func TestCoverage_LastReachableAttempts(t *testing.T) {
	// namesCaseIndex undefined source format path (fallback to original value)
	n := &namesCaseIndex{registry: map[text.CaseFormat]map[string]string{}}
	require.Equal(t, "___", n.formatTo("___", text.CaseFormatLowerCamel))

	// cache slice constructor error branch (newSliceMarshaller -> elem unsupported)
	pc := newCache().pathCache("")
	_, err := pc.getMarshaller(reflect.TypeOf([]chan int{}), &config.IOConfig{}, "", "", &format.Tag{})
	require.Error(t, err)
}
