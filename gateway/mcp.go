package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	furl "github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/content"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	serverproto "github.com/viant/mcp-protocol/server"
	"github.com/viant/toolbox"
)

func (r *Router) buildToolsIntegration(item *dpath.Item, aPath *dpath.Path, aRoute *Route, provider *repository.Provider) error {
	if aPath.Internal {
		return nil
	}
	component, err := provider.Component(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get component from provider: %w", err)
	}
	meta := aPath.Meta.Build(component.View.Name, component.View.Table, &aPath.Path)
	baseToolName := strings.ReplaceAll(meta.Name, " ", "")
	toolName, alternateRoute := mcpToolName(item, aPath, baseToolName)
	if !mcpRouteEnabled(component, aPath, alternateRoute) {
		return nil
	}
	toolInputType := r.buildToolInputTypeForPath(component, aPath)
	mcpTool := schema.Tool{
		Name:        toolName,
		Description: &meta.Description,
		InputSchema: schema.ToolInputSchema{},
	}
	if _, ok := r.mcpRegistry.ToolRegistry.Get(mcpTool.Name); ok {
		return nil //already registered
	}
	err = mcpTool.InputSchema.Load(reflect.New(toolInputType).Interface())
	if err != nil {
		return err
	}
	handler := r.mcpToolCallHandler(component, aRoute)
	tool := &serverproto.ToolEntry{
		Metadata: mcpTool,
		Handler:  handler,
	}
	r.mcpRegistry.RegisterTool(tool)
	return nil
}

// mcpRouteEnabled applies MCP-only controls declared on the parameter that
// owns WithURI. Both the base and derived tools default to enabled.
func mcpRouteEnabled(component *repository.Component, toolPath *dpath.Path, alternateRoute bool) bool {
	if component == nil || toolPath == nil {
		return true
	}
	toolURI := strings.TrimRight(toolPath.URI, "/")
	for _, parameter := range component.Input.Type.Parameters {
		if parameter == nil {
			continue
		}
		if !alternateRoute && parameter.MCP != nil && !*parameter.MCP {
			return false
		}
		if alternateRoute && parameter.URI != "" && strings.TrimRight(parameter.URI, "/") == toolURI && parameter.PathMCP != nil && !*parameter.PathMCP {
			return false
		}
	}
	return true
}

// mcpToolName keeps the first/base HTTP route name and gives
// each additional route emitted by WithURI a stable MCP name. For example,
// /advertiser/{id} becomes AdvertisersById while /advertiser remains
// Advertisers. The HTTP route model itself is unchanged.
func mcpToolName(item *dpath.Item, current *dpath.Path, baseName string) (string, bool) {
	base := mcpBasePath(item, current, baseName)
	if base == nil || base == current {
		return baseName, false
	}
	return baseName + mcpAlternateRouteSuffix(base.URI, current.URI), true
}

func mcpBasePath(item *dpath.Item, current *dpath.Path, baseName string) *dpath.Path {
	if item == nil || len(item.Paths) == 0 {
		return nil
	}
	var result *dpath.Path
	for _, candidate := range item.Paths {
		if candidate == nil || !candidate.MCPTool || candidate.Method != current.Method {
			continue
		}
		candidateName := strings.ReplaceAll(strings.TrimSpace(candidate.Name), " ", "")
		if candidateName == "" {
			candidateName = baseName
		}
		if candidateName != baseName {
			continue
		}
		if result == nil || mcpPathRank(candidate.URI) < mcpPathRank(result.URI) {
			result = candidate
		}
	}
	return result
}

func mcpPathRank(uri string) int {
	return len(mcpPathPlaceholders(uri))*10000 + len(strings.TrimRight(uri, "/"))
}

func mcpAlternateRouteSuffix(baseURI, alternateURI string) string {
	baseParams := map[string]bool{}
	for _, name := range mcpPathPlaceholders(baseURI) {
		baseParams[name] = true
	}
	var tokens []string
	for _, name := range mcpPathPlaceholders(alternateURI) {
		if !baseParams[name] {
			tokens = append(tokens, mcpUpperCamelToken(name))
		}
	}
	if len(tokens) == 0 {
		baseParts := splitMCPURIPath(baseURI)
		alternateParts := splitMCPURIPath(alternateURI)
		common := 0
		for common < len(baseParts) && common < len(alternateParts) && baseParts[common] == alternateParts[common] {
			common++
		}
		for _, part := range alternateParts[common:] {
			if !strings.HasPrefix(part, "{") {
				tokens = append(tokens, mcpUpperCamelToken(part))
			}
		}
	}
	if len(tokens) == 0 {
		tokens = []string{"Route"}
	}
	return "By" + strings.Join(tokens, "And")
}

func mcpPathPlaceholders(uri string) []string {
	var result []string
	for offset := 0; offset < len(uri); {
		start := strings.IndexByte(uri[offset:], '{')
		if start == -1 {
			break
		}
		start += offset
		end := strings.IndexByte(uri[start+1:], '}')
		if end == -1 {
			break
		}
		end += start + 1
		if name := strings.TrimSpace(uri[start+1 : end]); name != "" {
			result = append(result, name)
		}
		offset = end + 1
	}
	return result
}

func splitMCPURIPath(uri string) []string {
	var result []string
	for _, part := range strings.Split(strings.Trim(uri, "/"), "/") {
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func mcpUpperCamelToken(value string) string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == '-' || r == '_' || unicode.IsSpace(r)
	})
	for i, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		runes[0] = unicode.ToUpper(runes[0])
		parts[i] = string(runes)
	}
	return strings.Join(parts, "")
}

func (r *Router) mcpToolCallHandler(component *repository.Component, aRoute *Route) serverproto.ToolHandlerFunc {
	return func(ctx context.Context, req *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
		params := req.Params
		arguments, err := initializeToolArguments(ctx, component, params.Arguments)
		if err != nil {
			return nil, jsonrpc.NewInvalidParamsError(err.Error(), nil)
		}
		params.Arguments = arguments
		uri := r.matchToolCallComponentURI(aRoute, component, params)
		baseURL := fmt.Sprintf("http://localhost/%v", strings.TrimLeft(uri, "/")) // replace with actual service URL when available

		values := url.Values{}
		var body io.Reader
		uniquePath := map[string]bool{}
		uniqueQuery := map[string]bool{}

		// 1) Collect parameters (component + selector pagination)
		allParams := r.collectToolParameters(component)
		rawBlobBody, rpcErr := buildMCPRawBlobBody(component, params.Arguments)
		if rpcErr != nil {
			return nil, rpcErr
		}
		multipartBody, rpcErr := buildMCPMultipartBody(allParams, params.Arguments)
		if rpcErr != nil {
			return nil, rpcErr
		}
		if rawBlobBody != nil {
			body = rawBlobBody
		} else if multipartBody != nil {
			body = multipartBody
		}
		// 2) Apply parameters to request URL/query/body
		for _, p := range allParams {
			if rawBlobBody != nil && p.In.Kind == state.KindRequestBody {
				continue
			}
			if multipartBody != nil && (p.In.Kind == state.KindForm || p.In.Kind == state.KindRequestBody) {
				continue
			}
			value := toolArgumentValue(p, params.Arguments)
			pType := p.Schema.Type()
			if pType.Kind() == reflect.Ptr {
				pType = pType.Elem()
			}
			value = r.coerceNumericValue(value, pType)
			var rpcErr *jsonrpc.Error
			baseURL, body, rpcErr = r.applyParamToRequest(baseURL, values, p, value, uniquePath, uniqueQuery, body)
			if rpcErr != nil {
				return nil, rpcErr
			}
		}

		// 3) Finalize URL with query string
		finalURL := baseURL
		if enc := values.Encode(); enc != "" {
			if strings.Contains(finalURL, "?") {
				finalURL += "&" + enc
			} else {
				finalURL += "?" + enc
			}
		}

		// 4) Build HTTP request and route
		httpReq, rpcErr := r.newToolHTTPRequest(ctx, aRoute.Path.Method, finalURL, body)
		if rpcErr != nil {
			return nil, rpcErr
		}
		r.addAuthTokenIfPresent(ctx, httpReq)

		// NEW: map MCP view sync flag argument to Sync-Read header
		r.addSyncReadHeaderIfPresent(ctx, component, &params, httpReq)

		httpReq.RequestURI = httpReq.URL.RequestURI()
		if uri != aRoute.URI() {
			if matched, _ := r.match(component.Method, uri, httpReq); matched != nil {
				aRoute = matched
			}
		}
		rw := proxy.NewWriter()
		aRoute.Handle(rw, httpReq)

		if rw.Code == http.StatusUnauthorized {
			return nil, r.mcpUnauthorizedError()
		}

		// 5) Build tool result (text + structured on error)
		return r.buildToolCallResult(rw, finalURL, aRoute.Path.Method), nil
	}
}

func initializeToolArguments(ctx context.Context, component *repository.Component, arguments map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(arguments))
	for key, value := range arguments {
		result[key] = value
	}
	if component == nil || component.Input.Type.Type() == nil {
		return result, nil
	}
	inputType := component.Input.Type.Type().Type()
	if inputType == nil {
		return result, nil
	}
	if inputType.Kind() == reflect.Ptr {
		inputType = inputType.Elem()
	}
	if inputType.Kind() != reflect.Struct {
		return result, nil
	}
	holder := reflect.New(inputType)
	initializableArguments := make(map[string]interface{}, len(arguments))
	for key, value := range arguments {
		initializableArguments[key] = value
	}
	for _, parameter := range component.Input.Type.Parameters {
		if !isMCPMultipartFileParameter(parameter) {
			continue
		}
		for _, candidate := range toolArgumentCandidates(parameter) {
			delete(initializableArguments, candidate)
		}
	}
	encoded, err := json.Marshal(initializableArguments)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(encoded, holder.Interface()); err != nil {
		return nil, err
	}
	initializer, ok := holder.Interface().(state.Initializer)
	if !ok {
		return result, nil
	}
	if err = initializer.Init(ctx); err != nil {
		return nil, err
	}
	value := holder.Elem()
	var marker reflect.Value
	if has := value.FieldByName("Has"); has.IsValid() && has.Kind() == reflect.Ptr && !has.IsNil() {
		marker = has.Elem()
	}
	for _, parameter := range component.Input.Type.Parameters {
		if parameter == nil {
			continue
		}
		if isMCPMultipartFileParameter(parameter) {
			// The MCP wire value is an encoded blob descriptor, not a
			// multipart.FileHeader. Preserve the original argument for the
			// request encoder instead of decoding it into the component type.
			continue
		}
		field := value.FieldByName(parameter.Name)
		if !field.IsValid() || !field.CanInterface() {
			continue
		}
		present := toolArgumentValue(parameter, arguments) != nil
		if marker.IsValid() {
			if hasField := marker.FieldByName(parameter.Name); hasField.IsValid() && hasField.Kind() == reflect.Bool && hasField.Bool() {
				present = true
			}
		}
		if present {
			result[strings.Title(parameter.Name)] = field.Interface()
		}
	}
	return result, nil
}

func (r *Router) addSyncReadHeaderIfPresent(
	ctx context.Context,
	component *repository.Component,
	params *schema.CallToolRequestParams,
	httpRequest *http.Request,
) {
	if params == nil || params.Arguments == nil {
		return
	}
	// MCP tool arguments are generated using exported Go field names, so
	// the Datly view sync flag (view.SyncFlag == "viewSyncFlag") will appear
	// as "viewSyncFlag" in the schema/tool call.
	const mcpSyncFlagArg = "viewSyncFlag"
	const headerName = "Sync-Read"

	value, ok := params.Arguments[mcpSyncFlagArg]
	if !ok {
		return
	}

	if !isTruthy(value) {
		return
	}

	// Optionally, ensure that the underlying component actually declares
	// a sync flag parameter; if it does not, we simply skip setting the header.
	if !hasSyncFlagParameter(component) {
		return
	}

	httpRequest.Header.Set(headerName, "true")
}

// hasSyncFlagParameter checks whether the component declares a selector
// sync flag parameter, which should be exposed as view.SyncFlag.
func hasSyncFlagParameter(component *repository.Component) bool {
	if component == nil || component.View == nil || component.View.Selector == nil {
		return false
	}
	param := component.View.Selector.GetSyncFlagParameter()
	if param == nil {
		return false
	}
	// The selector sync flag parameter is defined in view.Config using
	// view.SyncFlag as the state key, but here we simply check that it exists.
	return true
}

// isTruthy interprets common JSON-serialised truthy values.
func isTruthy(v interface{}) bool {
	switch value := v.(type) {
	case bool:
		return value
	case string:
		s := strings.TrimSpace(strings.ToLower(value))
		return s == "true" || s == "1" || s == "yes" || s == "y"
	case float64:
		return value != 0
	default:
		return false
	}
}

// collectToolParameters aggregates component input parameters with selector pagination (limit/offset) when available.
func (r *Router) collectToolParameters(component *repository.Component) []*state.Parameter {
	var all []*state.Parameter
	all = append(all, component.Input.Type.Parameters...)
	if component.View != nil && component.View.Selector != nil {
		if p := component.View.Selector.LimitParameter; p != nil {
			all = append(all, p)
		}
		if p := component.View.Selector.OffsetParameter; p != nil {
			all = append(all, p)
		}
		if p := component.View.Selector.FieldsParameter; p != nil {
			all = append(all, p)
		}
		if p := component.View.Selector.OrderByParameter; p != nil {
			all = append(all, p)
		}
		if p := component.View.Selector.PageParameter; p != nil {
			all = append(all, p)
		}
	}
	return all
}

// coerceNumericValue normalizes numeric values to integers when appropriate.
func (r *Router) coerceNumericValue(value interface{}, paramType reflect.Type) interface{} {
	switch paramType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Float64:
		if value == nil {
			return nil
		}
		return toolbox.AsInt(value)
	}
	return value
}

// applyParamToRequest applies a single parameter into path placeholders, query/form values, or request body.
func (r *Router) applyParamToRequest(baseURL string, values url.Values, p *state.Parameter, value interface{}, uniquePath, uniqueQuery map[string]bool, body io.Reader) (string, io.Reader, *jsonrpc.Error) {
	switch p.In.Kind {
	case state.KindPath:
		if uniquePath[p.In.Name] {
			return baseURL, body, nil
		}
		uniquePath[p.In.Name] = true
		if value == nil {
			// If parameter has its own URI segment configured, treat as optional and strip the placeholder.
			if p.URI != "" {
				baseURL = strings.ReplaceAll(baseURL, "/{"+p.In.Name+"}", "")
				baseURL = strings.ReplaceAll(baseURL, "{"+p.In.Name+"}", "")
				return baseURL, body, nil
			}
			return baseURL, body, jsonrpc.NewInvalidRequest("missing path parameter: "+p.In.Name, nil)
		}
		pathValue, ok := mcpPathArgumentString(value)
		if !ok {
			return baseURL, body, jsonrpc.NewInvalidRequest("missing path parameter: "+p.In.Name, nil)
		}
		baseURL = strings.ReplaceAll(baseURL, "{"+p.In.Name+"}", pathValue)
	case state.KindQuery, state.KindForm:
		queryName := requestParamName(p)
		if uniqueQuery[queryName] {
			return baseURL, body, nil
		}
		uniqueQuery[queryName] = true
		if value == nil || value == "" {
			return baseURL, body, nil
		}
		var encodedValue string
		if slice, ok := value.([]interface{}); ok {
			var items []string
			for _, item := range slice {
				if f, ok := item.(float64); ok {
					items = append(items, fmt.Sprintf("%v", int64(f)))
				} else {
					items = append(items, fmt.Sprintf("%v", item))
				}
			}
			encodedValue = strings.Join(items, ",")
		} else {
			encodedValue = fmt.Sprintf("%v", value)
		}
		values.Add(queryName, encodedValue)
		// MCP exposes stable public selector names (limit/offset/etc.), while
		// Datly's uncustomized HTTP selector bindings use _limit/_offset/etc.
		// Send both when they differ so the routed request reaches the actual
		// bound parameter without breaking integrations that read the public alias.
		if boundName := strings.TrimSpace(p.In.Name); boundName != "" && boundName != queryName {
			values.Add(boundName, encodedValue)
		}
	case state.KindRequestBody:
		bodyValue := value
		if p != nil && !p.IsAnonymous() {
			if bodyName := strings.TrimSpace(p.In.Name); bodyName != "" {
				bodyValue = map[string]interface{}{bodyName: value}
			}
		}
		if text, ok := bodyValue.(string); ok {
			body = strings.NewReader(text)
		} else {
			data, err := json.Marshal(bodyValue)
			if err != nil {
				return baseURL, body, jsonrpc.NewInvalidParamsError("failed to marshal request body", nil)
			}
			body = strings.NewReader(string(data))
		}
	}
	return baseURL, body, nil
}

func requestParamName(p *state.Parameter) string {
	if p == nil || p.In == nil {
		return ""
	}
	if public, ok := selectorPublicParamName(p); ok {
		return public
	}
	return p.In.Name
}

func mcpPathArgumentString(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	rValue := reflect.ValueOf(value)
	for rValue.IsValid() && (rValue.Kind() == reflect.Interface || rValue.Kind() == reflect.Ptr) {
		if rValue.IsNil() {
			return "", false
		}
		rValue = rValue.Elem()
	}
	if !rValue.IsValid() {
		return "", false
	}
	if rValue.Kind() != reflect.Slice && rValue.Kind() != reflect.Array {
		result := strings.TrimSpace(fmt.Sprintf("%v", rValue.Interface()))
		return result, result != ""
	}
	items := make([]string, 0, rValue.Len())
	for i := 0; i < rValue.Len(); i++ {
		item := rValue.Index(i)
		for item.IsValid() && (item.Kind() == reflect.Interface || item.Kind() == reflect.Ptr) {
			if item.IsNil() {
				item = reflect.Value{}
				break
			}
			item = item.Elem()
		}
		if item.IsValid() {
			if text := strings.TrimSpace(fmt.Sprintf("%v", item.Interface())); text != "" {
				items = append(items, text)
			}
		}
	}
	return strings.Join(items, ","), len(items) > 0
}

func selectorPublicParamName(p *state.Parameter) (string, bool) {
	switch strings.TrimSpace(p.Name) {
	case "Limit":
		return "limit", true
	case "Offset":
		return "offset", true
	case "Page":
		return "page", true
	case "Fields":
		return "fields", true
	case "OrderBy":
		return "orderBy", true
	case "Criteria":
		return "criteria", true
	}
	return "", false
}

type mcpBlob struct {
	Data     string `json:"data" description:"Base64-encoded blob data"`
	Filename string `json:"filename,omitempty" description:"Multipart filename" optional:"true"`
	MIMEType string `json:"mimeType,omitempty" description:"Blob media type" optional:"true"`
}

type mcpEncodedBody struct {
	io.Reader
	contentType string
}

var multipartFileHeaderType = reflect.TypeOf(multipart.FileHeader{})
var xlsUnmarshallerType = reflect.TypeOf((*shared.XLSUnmarshaller)(nil)).Elem()

func componentSupportsMCPRawBlob(component *repository.Component) bool {
	if component == nil {
		return false
	}
	for _, parameter := range component.Input.Type.Parameters {
		if parameter == nil || parameter.In == nil || parameter.In.Kind != state.KindRequestBody || parameter.Schema == nil {
			continue
		}
		rType := parameter.Schema.Type()
		if rType == nil {
			continue
		}
		if rType.Implements(xlsUnmarshallerType) || (rType.Kind() != reflect.Ptr && reflect.PointerTo(rType).Implements(xlsUnmarshallerType)) {
			return true
		}
	}
	return false
}

func buildMCPRawBlobBody(component *repository.Component, arguments map[string]interface{}) (*mcpEncodedBody, *jsonrpc.Error) {
	if !componentSupportsMCPRawBlob(component) {
		return nil, nil
	}
	value, ok := arguments["Blob"]
	if !ok {
		value, ok = arguments["blob"]
	}
	if !ok || value == nil {
		return nil, nil
	}
	blobs, err := decodeMCPBlobs(value)
	if err != nil || len(blobs) != 1 {
		return nil, jsonrpc.NewInvalidParamsError("Blob must be one base64-encoded blob descriptor", nil)
	}
	data, err := decodeMCPBlobData(blobs[0].Data)
	if err != nil {
		return nil, jsonrpc.NewInvalidParamsError(err.Error(), nil)
	}
	mediaType := strings.TrimSpace(blobs[0].MIMEType)
	if mediaType == "" {
		mediaType = content.XLSContentType
	}
	return &mcpEncodedBody{Reader: bytes.NewReader(data), contentType: mediaType}, nil
}

func isMCPMultipartFileParameter(parameter *state.Parameter) bool {
	if parameter == nil || parameter.In == nil || parameter.In.Kind != state.KindForm || parameter.Schema == nil {
		return false
	}
	rType := parameter.Schema.Type()
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType == multipartFileHeaderType {
		return true
	}
	return rType != nil && rType.Kind() == reflect.Slice && rType.Elem() == reflect.PointerTo(multipartFileHeaderType)
}

func mcpToolParameterType(parameter *state.Parameter) reflect.Type {
	if !isMCPMultipartFileParameter(parameter) {
		return parameter.Schema.Type()
	}
	rType := parameter.Schema.Type()
	if rType.Kind() == reflect.Slice {
		return reflect.SliceOf(reflect.TypeOf(mcpBlob{}))
	}
	return reflect.TypeOf(mcpBlob{})
}

func buildMCPMultipartBody(parameters []*state.Parameter, arguments map[string]interface{}) (*mcpEncodedBody, *jsonrpc.Error) {
	hasBlob := false
	for _, parameter := range parameters {
		if isMCPMultipartFileParameter(parameter) && toolArgumentValue(parameter, arguments) != nil {
			hasBlob = true
			break
		}
	}
	if !hasBlob {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	writer := multipart.NewWriter(buffer)
	for _, parameter := range parameters {
		if parameter == nil || parameter.In == nil {
			continue
		}
		value := toolArgumentValue(parameter, arguments)
		if value == nil {
			continue
		}
		switch parameter.In.Kind {
		case state.KindForm:
			if isMCPMultipartFileParameter(parameter) {
				if err := writeMCPBlobParts(writer, parameter.In.Name, value); err != nil {
					_ = writer.Close()
					return nil, jsonrpc.NewInvalidParamsError(err.Error(), nil)
				}
				continue
			}
			if err := writeMCPFormValues(writer, requestParamName(parameter), value); err != nil {
				_ = writer.Close()
				return nil, jsonrpc.NewInvalidParamsError(err.Error(), nil)
			}
		case state.KindRequestBody:
			name := strings.TrimSpace(parameter.In.Name)
			if name == "" {
				name = "json"
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				_ = writer.Close()
				return nil, jsonrpc.NewInvalidParamsError("failed to marshal multipart JSON body", nil)
			}
			if err = writer.WriteField(name, string(encoded)); err != nil {
				_ = writer.Close()
				return nil, jsonrpc.NewInvalidParamsError("failed to write multipart JSON body", nil)
			}
		}
	}
	if err := writer.Close(); err != nil {
		return nil, jsonrpc.NewInvalidParamsError("failed to finalize multipart body", nil)
	}
	return &mcpEncodedBody{Reader: bytes.NewReader(buffer.Bytes()), contentType: writer.FormDataContentType()}, nil
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

// newToolHTTPRequest constructs an HTTP request for routed tool invocation.
func (r *Router) newToolHTTPRequest(ctx context.Context, method, URL string, body io.Reader) (*http.Request, *jsonrpc.Error) {
	httpRequest, err := http.NewRequestWithContext(ctx, method, URL, body)
	if err != nil {
		return nil, jsonrpc.NewInvalidRequest(err.Error(), nil)
	}
	if body != nil {
		contentType := "application/json"
		if encoded, ok := body.(*mcpEncodedBody); ok && encoded.contentType != "" {
			contentType = encoded.contentType
		}
		httpRequest.Header.Set("Content-Type", contentType)
	}
	return httpRequest, nil
}

func decodeToolResponseBody(responseWriter *proxy.Writer) ([]byte, error) {
	data := responseWriter.Body.Bytes()
	encoding := strings.TrimSpace(responseWriter.HeaderMap.Get("Content-Encoding"))
	if !strings.EqualFold(encoding, "gzip") {
		return data, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress gzip body: %w", err)
	}
	return decoded, nil
}

// buildToolCallResult composes a CallToolResult with text content and structured error info if status is not OK.
func (r *Router) buildToolCallResult(responseWriter *proxy.Writer, URL, method string) *schema.CallToolResult {
	var result = &schema.CallToolResult{}
	mimeType := responseWriter.HeaderMap.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/json"
	}
	data, err := decodeToolResponseBody(responseWriter)
	if err != nil {
		data = []byte(err.Error())
	}
	result.Content = append(result.Content, schema.CallToolResultContentElem(
		schema.TextContent{
			Type: "text",
			Text: string(data),
		},
	))
	_ = json.Unmarshal(data, &result.StructuredContent)
	if responseWriter.Code >= http.StatusBadRequest {
		isErr := true
		result.IsError = &isErr
		result.StructuredContent = map[string]interface{}{
			"status":  responseWriter.Code,
			"error":   true,
			"message": string(data),
			"headers": responseWriter.HeaderMap,
			"uri":     URL,
			"method":  method,
		}
	}
	return result
}

func (r *Router) matchToolCallComponentURI(aRoute *Route, component *repository.Component, params schema.CallToolRequestParams) string {
	URI := furl.Path(aRoute.Path.URI)
	for _, parameter := range component.Input.Type.Parameters {
		if parameter.URI == "" {
			continue
		}
		value := toolArgumentValue(parameter, params.Arguments)
		if _, ok := mcpPathArgumentString(value); !ok {
			continue
		}
		URI = furl.Path(parameter.URI)
		break
	}
	return URI
}

func (r *Router) addAuthTokenIfPresent(ctx context.Context, httpRequest *http.Request) {
	if tokenValue := ctx.Value(authorization.TokenKey); tokenValue != nil {
		if token, ok := tokenValue.(*authorization.Token); ok {
			if !strings.HasPrefix(token.Token, "Bearer ") {
				token.Token = "Bearer " + token.Token
			}
			httpRequest.Header.Set("Authorization", fmt.Sprintf("%s", token.Token))
		}
	}
}

func (r *Router) mcpUnauthorizedError() *jsonrpc.Error {
	if r == nil || r.config == nil || r.config.MCP == nil {
		return jsonrpc.NewError(schema.Unauthorized, "Unauthorized", nil)
	}
	issuerURL := strings.TrimSpace(r.config.MCP.IssuerURL)
	if issuerURL == "" {
		return jsonrpc.NewError(schema.Unauthorized, "Unauthorized", nil)
	}
	return jsonrpc.NewError(schema.Unauthorized, "Unauthorized", &authorization.Authorization{
		RequiredScopes: []string{},
		UseIdToken:     true,
		ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{
			Resource:             r.config.MCP.ProtectedResourceURL(),
			AuthorizationServers: []string{issuerURL},
		},
	})
}

func (r *Router) buildToolInputType(components *repository.Component) reflect.Type {
	return r.buildToolInputTypeForPath(components, nil)
}

func (r *Router) buildToolInputTypeForPath(components *repository.Component, toolPath *dpath.Path) reflect.Type {
	var inputFields []reflect.StructField
	var uniqueFieldName = make(map[string]bool)
	var uniqueQuery = make(map[string]bool)
	var uniquePath = make(map[string]bool)
	hasMultipartFiles := false
	hasRawBlobBody := componentSupportsMCPRawBlob(components)
	for _, parameter := range components.Input.Type.Parameters {
		if isMCPMultipartFileParameter(parameter) {
			hasMultipartFiles = true
			break
		}
	}
	appendField := func(name string, fieldType reflect.Type, tag reflect.StructTag) {
		if name == "" || fieldType == nil {
			return
		}
		if uniqueFieldName[name] {
			return
		}
		uniqueFieldName[name] = true
		inputFields = append(inputFields, reflect.StructField{Name: name, Type: fieldType, Tag: tag})
	}
	// Include component input parameters
	for _, parameter := range components.Input.Type.Parameters {
		name := strings.Title(parameter.Name)
		switch parameter.In.Kind {
		case state.KindPath:
			forceRequired := false
			if toolPath != nil && parameter.URI != "" {
				if !mcpParameterURIPathMatches(parameter.URI, toolPath.URI) {
					continue
				}
				forceRequired = true
			}
			if uniquePath[parameter.In.Name] {
				continue
			}
			uniquePath[parameter.In.Name] = true
			tag := buildMCPFieldTagWithRequired(parameter, false, forceRequired)
			appendField(name, parameter.Schema.Type(), tag)
		case state.KindQuery, state.KindForm:

			if uniqueQuery[parameter.In.Name] {
				continue
			}
			uniqueQuery[parameter.In.Name] = true
			tag := buildMCPFieldTag(parameter, true)
			appendField(name, mcpToolParameterType(parameter), tag)
		case state.KindRequestBody:
			if parameter.IsAnonymous() {
				appendAnonymousBodyFields(&inputFields, uniqueFieldName, parameter.Schema.Type(), hasMultipartFiles || hasRawBlobBody)
				continue
			}
			tag := buildMCPFieldTag(parameter, hasMultipartFiles || hasRawBlobBody)
			appendField(name, parameter.Schema.Type(), tag)
		}
	}
	if hasRawBlobBody {
		appendField("Blob", reflect.TypeOf(mcpBlob{}), reflect.StructTag(`json:",omitempty" optional:"true" description:"Base64-encoded binary request body; use instead of JSON when supported"`))
	}

	// Include selector (limit/offset/fields/page) for read components when available
	if components.View != nil && components.View.Selector != nil {
		if p := components.View.Selector.LimitParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] { // avoid duplicates
				uniqueQuery[p.In.Name] = true
				appendField(strings.Title(p.Name), p.Schema.Type(), `json:",omitempty"`)
			}
		}
		if p := components.View.Selector.OffsetParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] {
				uniqueQuery[p.In.Name] = true
				appendField(strings.Title(p.Name), p.Schema.Type(), `json:",omitempty"`)
			}
		}
		if p := components.View.Selector.FieldsParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] {
				uniqueQuery[p.In.Name] = true
				// Fields is a []string – ensure optional in schema
				appendField(strings.Title(p.Name), p.Schema.Type(), `json:",omitempty" optional:"true"`)
			}
		}
		if p := components.View.Selector.OrderByParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] {
				uniqueQuery[p.In.Name] = true
				appendField(strings.Title(p.Name), p.Schema.Type(), `json:",omitempty" optional:"true"`)
			}
		}
		if p := components.View.Selector.PageParameter; p != nil && p.In != nil && p.In.Name != "" {
			if !uniqueQuery[p.In.Name] {
				uniqueQuery[p.In.Name] = true
				appendField(strings.Title(p.Name), p.Schema.Type(), `json:",omitempty"`)
			}
		}
	}

	return reflect.StructOf(inputFields)
}

func mcpParameterURIPathMatches(parameterURI, routeURI string) bool {
	parameterPath := strings.TrimRight(strings.TrimSpace(parameterURI), "/")
	routePath := strings.TrimRight(strings.TrimSpace(routeURI), "/")
	if parameterPath == "" || routePath == "" {
		return false
	}
	if parameterPath == routePath {
		return true
	}
	// WithURI accepts a relative route suffix (for example "/{id}") as well
	// as a fully-qualified component path. Generated route paths are always
	// qualified, so retain the path parameter when the qualified route ends in
	// the declared suffix at a segment boundary.
	return strings.HasPrefix(parameterPath, "/") &&
		strings.HasSuffix(routePath, parameterPath) &&
		len(routePath) > len(parameterPath)
}

func buildMCPFieldTag(parameter *state.Parameter, defaultOptional bool) reflect.StructTag {
	return buildMCPFieldTagWithRequired(parameter, defaultOptional, false)
}

func buildMCPFieldTagWithRequired(parameter *state.Parameter, defaultOptional, forceRequired bool) reflect.StructTag {
	if parameter == nil {
		return reflect.StructTag(`json:",omitempty"`)
	}
	var parts []string
	jsonTag := `json:",omitempty"`
	if forceRequired {
		jsonTag = `json:""`
	}
	if !forceRequired && parameter.Schema != nil && parameter.Schema.Type() != nil && parameter.Schema.Type().Kind() == reflect.Slice {
		parts = append(parts, jsonTag, `optional:"true"`)
	} else {
		parts = append(parts, jsonTag)
		if !forceRequired && (strings.Contains(parameter.Tag, "optional") || strings.Contains(parameter.Tag, `required:"false"`) || defaultOptional) {
			parts = append(parts, `optional:"true"`)
		}
	}
	if description := strings.TrimSpace(parameter.Description); description != "" {
		parts = append(parts, `description:`+strconv.Quote(description))
	}
	if example := strings.TrimSpace(parameter.Example); example != "" {
		parts = append(parts, `example:`+strconv.Quote(example))
	}
	return reflect.StructTag(strings.Join(parts, " "))
}

func toolArgumentValue(parameter *state.Parameter, arguments map[string]interface{}) interface{} {
	if parameter == nil {
		return nil
	}
	if parameter.In != nil && parameter.In.Kind == state.KindRequestBody && parameter.IsAnonymous() && parameter.Schema != nil {
		return anonymousBodyArgumentValue(arguments, parameter.Schema.Type())
	}
	for _, candidate := range toolArgumentCandidates(parameter) {
		if value, ok := arguments[candidate]; ok {
			return value
		}
	}
	return nil
}

func toolArgumentCandidates(parameter *state.Parameter) []string {
	if parameter == nil {
		return nil
	}
	var result []string
	seen := map[string]bool{}
	appendCandidate := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		result = append(result, value)
	}

	appendCandidate(strings.Title(parameter.Name))
	appendCandidate(parameter.Name)
	appendCandidate(toPascalIdentifier(parameter.Name))
	appendCandidate(toLowerCamelIdentifier(parameter.Name))
	if public := requestParamName(parameter); public != "" {
		appendCandidate(public)
		appendCandidate(strings.Title(public))
		appendCandidate(toPascalIdentifier(public))
		appendCandidate(toLowerCamelIdentifier(public))
	}
	return result
}

func toPascalIdentifier(value string) string {
	parts := splitIdentifierParts(value)
	for i, part := range parts {
		parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}
	return strings.Join(parts, "")
}

func toLowerCamelIdentifier(value string) string {
	pascal := toPascalIdentifier(value)
	if pascal == "" {
		return ""
	}
	return strings.ToLower(pascal[:1]) + pascal[1:]
}

func splitIdentifierParts(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var result []string
	var current []rune
	flush := func() {
		if len(current) == 0 {
			return
		}
		result = append(result, string(current))
		current = current[:0]
	}
	for i, r := range value {
		switch {
		case r == '_' || r == '-' || r == ' ':
			flush()
		case i > 0 && r >= 'A' && r <= 'Z':
			prev := rune(value[i-1])
			if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') {
				flush()
			}
			current = append(current, r)
		default:
			current = append(current, r)
		}
	}
	flush()
	return result
}

func appendAnonymousBodyFields(fields *[]reflect.StructField, unique map[string]bool, bodyType reflect.Type, optional bool) {
	bodyType = indirectType(bodyType)
	if bodyType == nil || bodyType.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < bodyType.NumField(); i++ {
		field := bodyType.Field(i)
		if !field.IsExported() {
			continue
		}
		if unique[field.Name] {
			continue
		}
		if optional && !strings.Contains(string(field.Tag), `optional:"true"`) {
			field.Tag = reflect.StructTag(strings.TrimSpace(string(field.Tag) + ` optional:"true"`))
		}
		unique[field.Name] = true
		*fields = append(*fields, field)
	}
}

func anonymousBodyArgumentValue(arguments map[string]interface{}, bodyType reflect.Type) interface{} {
	bodyType = indirectType(bodyType)
	if bodyType == nil || bodyType.Kind() != reflect.Struct {
		return nil
	}
	payload := map[string]interface{}{}
	for i := 0; i < bodyType.NumField(); i++ {
		field := bodyType.Field(i)
		if !field.IsExported() {
			continue
		}
		value, ok := arguments[field.Name]
		if !ok {
			value, ok = arguments[jsonFieldName(field)]
		}
		if !ok {
			continue
		}
		payload[jsonFieldName(field)] = value
	}
	if len(payload) == 0 {
		return nil
	}
	return payload
}

func jsonFieldName(field reflect.StructField) string {
	if tag := field.Tag.Get("json"); tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] != "" && parts[0] != "-" {
			return parts[0]
		}
	}
	return strings.ToLower(field.Name[:1]) + field.Name[1:]
}

func indirectType(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func (r *Router) buildTemplateResourceIntegration(item *dpath.Item, aPath *dpath.Path, aRoute *Route, provider *repository.Provider) error {
	if aPath.Internal {
		return nil
	}
	var parameterNames []string
	for _, parameter := range item.Resource.Parameters {
		switch parameter.In.Kind {
		case state.KindQuery, state.KindForm:
			parameterNames = append(parameterNames, parameter.In.Name)
		}
	}
	// Also expose view selector pagination controls in URI template if present
	if provider != nil {
		if comp, err := provider.Component(context.Background()); err == nil && comp.View != nil && comp.View.Selector != nil {
			if p := comp.View.Selector.LimitParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
			if p := comp.View.Selector.OffsetParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
			if p := comp.View.Selector.FieldsParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
			if p := comp.View.Selector.OrderByParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
			if p := comp.View.Selector.PageParameter; p != nil && p.In != nil && p.In.Name != "" {
				parameterNames = append(parameterNames, p.In.Name)
			}
		}
	}
	canBuildTemplateResource := len(parameterNames) > 0 || strings.Contains(aPath.URI, "{")
	if !canBuildTemplateResource {
		return nil
	}

	URL := furl.Join("datly://localhost", aPath.URI)
	if len(parameterNames) > 0 {
		// append query parameters to the URL
		URL += "{?" + strings.Join(parameterNames, ",") + "}"
	}
	meta := aPath.Meta.Build(aPath.View.Ref, aPath.View.Ref, &aPath.Path)
	mimeType := "application/json"
	mcpResourceTemplate := schema.ResourceTemplate{
		UriTemplate: URL,
		Name:        strings.ReplaceAll(meta.Name, " ", ""),
		Description: &meta.Description,
		MimeType:    &mimeType,
	}

	// Check if the resource template is already registered
	handler := r.reactMcpResourceHandler(mcpResourceTemplate, aRoute, provider)
	if r.hasMcpResource(mcpResourceTemplate.UriTemplate) {
		return nil
	}

	// Build the integration for the resource
	r.mcpRegistry.RegisterResourceTemplate(mcpResourceTemplate, handler)
	return nil
}

func (r *Router) reactMcpResourceHandler(mcpResourceTemplate schema.ResourceTemplate, aRoute *Route, provider *repository.Provider) func(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	handler := func(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
		result, rpcErr := r.handleMcpRead(ctx, &request.Params, &mcpResourceTemplate, aRoute, provider)
		if rpcErr != nil {
			return nil, rpcErr
		}
		if len(result) == 0 {
			return &schema.ReadResourceResult{Contents: []schema.ReadResourceResultContentsElem{}}, nil
		}
		return &schema.ReadResourceResult{Contents: result}, nil
	}
	return handler
}

func (r *Router) buildResourceIntegration(item *dpath.Item, aPath *dpath.Path, aRoute *Route, provider *repository.Provider) error {
	if aPath.Internal {
		return nil
	}
	var parameterNames []string
	for _, parameter := range item.Resource.Parameters {
		switch parameter.In.Kind {
		case state.KindQuery, state.KindForm:
			parameterNames = append(parameterNames, parameter.In.Name)
		}
	}
	hasPathParameter := strings.Contains(aPath.URI, "{")
	if hasPathParameter {
		return nil
	}

	URL := furl.Join("datly://localhost", aPath.URI)
	meta := aPath.Meta.Build(aPath.View.Ref, aPath.View.Ref, &aPath.Path)
	mimeType := "application/json"
	mcpResource := schema.Resource{
		Uri:         URL,
		Name:        strings.ReplaceAll(meta.Name, " ", ""),
		Description: &meta.Description,
		MimeType:    &mimeType,
	}
	mcpResourceTemplate := schema.ResourceTemplate{
		UriTemplate: URL,
		Name:        meta.Name,
		Description: &meta.Description,
		MimeType:    &mimeType,
	}

	// Check if the resource template is already registered
	handler := r.reactMcpResourceHandler(mcpResourceTemplate, aRoute, provider)
	if r.hasMcpResource(mcpResourceTemplate.UriTemplate) {
		return nil
	}
	// Build the integration for the mcpResource
	r.mcpRegistry.RegisterResource(mcpResource, handler)
	return nil
}

func (r *Router) hasMcpResource(URI string) bool {
	if _, ok := r.mcpRegistry.ResourceRegistry.Get(URI); ok {
		return true //already registered
	}
	if _, ok := r.mcpRegistry.ResourceTemplateRegistry.Get(URI); ok {
		return true //already registered
	}
	return false
}

func (r *Router) handleMcpRead(ctx context.Context, params *schema.ReadResourceRequestParams, template *schema.ResourceTemplate, aRoute *Route, provider *repository.Provider) ([]schema.ReadResourceResultContentsElem, *jsonrpc.Error) {
	URI := furl.Path(params.Uri)
	URL := fmt.Sprintf("http://localhost/%v", URI) // fallback to a local URL for now, this should be replaced with the actual service URL
	component, err := provider.Component(ctx)      // ensure the provider is initialized
	if err != nil {
		return nil, jsonrpc.NewInternalError(fmt.Errorf("failed to get component from provider: %w", err).Error(), nil)
	}
	byLoc := make(map[string]*state.Parameter)
	for _, param := range component.View.GetResource().Parameters {
		byLoc[param.In.Name] = param
	}

	responseWriter := proxy.NewWriter()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, URL, nil)
	if err != nil {
		return nil, jsonrpc.NewInvalidRequest(err.Error(), nil)
	}
	r.addAuthTokenIfPresent(ctx, httpRequest)
	aRoute.Handle(responseWriter, httpRequest) // route the request to the actual handler
	if responseWriter.Code == http.StatusUnauthorized {
		return nil, r.mcpUnauthorizedError()
	}
	var result []schema.ReadResourceResultContentsElem
	mimeType := ""
	if template.MimeType != nil {
		mimeType = *template.MimeType
	}
	result = append(result, schema.ReadResourceResultContentsElem{
		Uri:      URL,                          // The URI of the resource
		Text:     responseWriter.Body.String(), // The actual data returned from the request
		MimeType: &mimeType,                    // The MIME type of the resource
		Blob:     responseWriter.Body.String(),
	})
	return result, nil

}
