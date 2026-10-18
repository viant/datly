package tool

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"reflect"
	"strings"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/xlsbody"
	"github.com/viant/mcp-protocol/schema"
)

const xlsMIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

var fileType = reflect.TypeOf(multipart.FileHeader{})
var xlsType = reflect.TypeOf((*interface {
	UnmarshalXLS(context.Context, []byte) error
})(nil)).Elem()

func fileArgument(a Argument) bool {
	t := a.sourceType
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return a.sourceKind == "form" && (t == fileType || t != nil && t.Kind() == reflect.Slice && t.Elem() == reflect.PointerTo(fileType))
}
func rawArgument(a Argument) bool {
	t := a.sourceType
	if a.parentBodyType != nil {
		t = a.parentBodyType
	}
	return a.sourceKind == "body" && t != nil && (t.Implements(xlsType) || t.Kind() != reflect.Pointer && reflect.PointerTo(t).Implements(xlsType))
}
func binaryCapabilities(args []Argument) (files, raw bool) {
	for _, a := range args {
		files = files || fileArgument(a)
		raw = raw || rawArgument(a)
	}
	return
}
func descriptorSchema() map[string]interface{} {
	return map[string]interface{}{"type": "object", "properties": map[string]interface{}{"data": map[string]interface{}{"type": "string"}, "filename": map[string]interface{}{"type": "string"}, "mimeType": map[string]interface{}{"type": "string"}}}
}
func projectBinarySchema(args []Argument, s *schema.ToolInputSchema) {
	files, raw := binaryCapabilities(args)
	for _, a := range args {
		if fileArgument(a) {
			prop := descriptorSchema()
			t := a.sourceType
			for t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			if t.Kind() == reflect.Slice {
				prop = map[string]interface{}{"type": "array", "items": prop}
			}
			s.Properties[a.publicName] = prop
		}
	}
	if files {
		if _, exists := s.Properties["Payload"]; !exists {
			s.Properties["Payload"] = map[string]interface{}{"type": "object"}
		}
	}
	if raw {
		if _, exists := s.Properties["Blob"]; !exists {
			s.Properties["Blob"] = descriptorSchema()
		}
	}
	if files || raw {
		required := s.Required[:0]
		for _, name := range s.Required {
			body := false
			for _, a := range args {
				body = body || a.publicName == name && a.sourceKind == "body"
			}
			if !body {
				required = append(required, name)
			}
		}
		s.Required = required
	}
}
func (p *Plan) binaryArguments(args map[string]interface{}) (map[string]interface{}, map[string]interface{}, error) {
	files, raw := binaryCapabilities(p.args)
	synthetic := map[string]interface{}{}
	declared := map[string]bool{}
	for _, a := range p.args {
		declared[a.publicName] = true
		for _, alias := range a.aliases {
			declared[alias] = true
		}
	}
	ordinary := map[string]interface{}{}
	for k, v := range args {
		if !declared[k] && (files && k == "Payload" || raw && (k == "Blob" || k == "blob")) {
			synthetic[k] = v
		} else {
			ordinary[k] = v
		}
	}
	normalized, err := p.binding.NormalizeArguments(ordinary)
	return normalized, synthetic, err
}
func (p *Plan) binaryRequest(ctx context.Context, args, synthetic map[string]interface{}) (*http.Request, []requestprovider.Option, error) {
	files, raw := binaryCapabilities(p.args)
	var rawBody *mcpEncodedBody
	if raw {
		value, ok := synthetic["Blob"]
		if !ok {
			value = synthetic["blob"]
		}
		if value != nil {
			blobs, err := decodeMCPBlobs(value)
			if err != nil || len(blobs) != 1 {
				return nil, nil, fmt.Errorf("Blob must be one base64-encoded blob descriptor")
			}
			data, err := decodeMCPBlobData(blobs[0].Data)
			if err != nil {
				return nil, nil, err
			}
			media := strings.TrimSpace(blobs[0].MIMEType)
			if media == "" {
				media = xlsMIME
			}
			rawBody = &mcpEncodedBody{bytes.NewReader(data), media}
		}
	}
	var multi *mcpEncodedBody
	if files {
		hasFiles := false
		for _, a := range p.args {
			hasFiles = hasFiles || fileArgument(a) && args[a.publicName] != nil
		}
		if !hasFiles && synthetic["Payload"] != nil {
			return nil, nil, fmt.Errorf("multipart Payload requires Files")
		}
		if hasFiles {
			buf := &bytes.Buffer{}
			writer := multipart.NewWriter(buf)
			for _, a := range p.args {
				v := args[a.publicName]
				if v == nil {
					continue
				}
				var err error
				switch a.sourceKind {
				case "form":
					if fileArgument(a) {
						err = writeMCPBlobParts(writer, a.sourceName, v)
					} else {
						err = writeMCPFormValues(writer, a.sourceName, v)
					}
				case "body":
					name := strings.TrimSpace(a.sourceName)
					if name == "" {
						name = "json"
					}
					var data []byte
					data, err = json.Marshal(v)
					if err == nil {
						err = writer.WriteField(name, string(data))
					}
				}
				if err != nil {
					return nil, nil, err
				}
			}
			if v := synthetic["Payload"]; v != nil {
				data, err := json.Marshal(v)
				if err != nil {
					return nil, nil, err
				}
				var payload map[string]interface{}
				if err = json.Unmarshal(data, &payload); err != nil {
					return nil, nil, fmt.Errorf("multipart Payload must be an object")
				}
				reserved := map[string]bool{}
				for _, a := range p.args {
					if a.sourceKind == "form" {
						reserved[strings.ToLower(strings.TrimSpace(a.sourceName))] = true
					}
				}
				for name, v := range payload {
					name = strings.TrimSpace(name)
					if name == "" {
						return nil, nil, fmt.Errorf("multipart Payload contains an empty field name")
					}
					if reserved[strings.ToLower(name)] {
						return nil, nil, fmt.Errorf("multipart Payload field %q conflicts with a declared form parameter", name)
					}
					if err = writeMCPDynamicFormValue(writer, name, v); err != nil {
						return nil, nil, err
					}
				}
			}
			if err := writer.Close(); err != nil {
				return nil, nil, err
			}
			multi = &mcpEncodedBody{bytes.NewReader(buf.Bytes()), writer.FormDataContentType()}
		}
	}
	encoded := rawBody
	if encoded == nil {
		encoded = multi
	}
	if encoded == nil {
		return nil, nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "http://localhost/", encoded)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", encoded.contentType)
	var options []requestprovider.Option
	mediaType, _, _ := mime.ParseMediaType(encoded.contentType)
	if mediaType == xlsMIME {
		options = append(options, requestprovider.WithDecoder(xlsMIME, binaryXLSDecoder{ctx: ctx, args: p.args}))
	}
	return req, options, nil
}

type mcpEncodedBody struct {
	io.Reader
	contentType string
}

type mcpBlob struct {
	Data     string `json:"data"`
	Filename string `json:"filename,omitempty"`
	MIMEType string `json:"mimeType,omitempty"`
}

func writeMCPDynamicFormValue(writer *multipart.Writer, name string, value interface{}) error {
	if value == nil {
		return nil
	}
	rValue := reflect.ValueOf(value)
	for rValue.IsValid() && (rValue.Kind() == reflect.Interface || rValue.Kind() == reflect.Ptr) {
		if rValue.IsNil() {
			return nil
		}
		rValue = rValue.Elem()
	}
	if rValue.IsValid() && (rValue.Kind() == reflect.Map || rValue.Kind() == reflect.Struct) {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to encode multipart Payload field %q: %w", name, err)
		}
		return writer.WriteField(name, string(encoded))
	}
	return writeMCPFormValues(writer, name, value)
}

func writeMCPFormValues(writer *multipart.Writer, name string, value interface{}) error {
	rValue := reflect.ValueOf(value)
	for rValue.IsValid() && (rValue.Kind() == reflect.Interface || rValue.Kind() == reflect.Ptr) {
		if rValue.IsNil() {
			return nil
		}
		rValue = rValue.Elem()
	}
	if rValue.IsValid() && (rValue.Kind() == reflect.Slice || rValue.Kind() == reflect.Array) {
		for i := 0; i < rValue.Len(); i++ {
			if err := writer.WriteField(name, fmt.Sprint(rValue.Index(i).Interface())); err != nil {
				return err
			}
		}
		return nil
	}
	return writer.WriteField(name, fmt.Sprint(value))
}

func writeMCPBlobParts(writer *multipart.Writer, fieldName string, value interface{}) error {
	blobs, err := decodeMCPBlobs(value)
	if err != nil {
		return err
	}
	for index, blob := range blobs {
		data, err := decodeMCPBlobData(blob.Data)
		if err != nil {
			return fmt.Errorf("invalid %s blob %d: %w", fieldName, index, err)
		}
		filename := strings.TrimSpace(blob.Filename)
		if filename == "" {
			filename = "blob"
		}
		mediaType := strings.TrimSpace(blob.MIMEType)
		if mediaType == "" {
			mediaType = "application/octet-stream"
		}
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": fieldName, "filename": filename}))
		header.Set("Content-Type", mediaType)
		part, err := writer.CreatePart(header)
		if err != nil {
			return err
		}
		if _, err = part.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func decodeMCPBlobs(value interface{}) ([]mcpBlob, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > 0 && encoded[0] == '[' {
		var result []mcpBlob
		if err = json.Unmarshal(encoded, &result); err != nil {
			return nil, err
		}
		return result, nil
	}
	if text, ok := value.(string); ok {
		return []mcpBlob{{Data: text}}, nil
	}
	var result mcpBlob
	if err = json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return []mcpBlob{result}, nil
}

func decodeMCPBlobData(value string) ([]byte, error) {
	data := strings.TrimSpace(value)
	if comma := strings.IndexByte(data, ','); strings.HasPrefix(data, "data:") && comma >= 0 {
		data = data[comma+1:]
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.RawURLEncoding} {
		if decoded, err := encoding.DecodeString(data); err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("data must be base64 encoded")
}

type binaryXLSDecoder struct {
	ctx  context.Context
	args []Argument
}

func (d binaryXLSDecoder) Decode(raw []byte, target any) error {
	return xlsbody.Unmarshal(d.ctx, raw, target)
}
func (d binaryXLSDecoder) DecodeField(raw []byte, name string, target any) (bool, error) {
	for _, a := range d.args {
		sourceName := a.sourceName
		if a.parentBodyType != nil {
			sourceName = a.parentBodySource
		}
		if rawArgument(a) && sourceName == name {
			err := d.Decode(raw, target)
			return err == nil, err
		}
	}
	return false, nil
}
