package router

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/sanitize"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

type viewHandler func(response http.ResponseWriter, request *http.Request)

type (
	Routes []*Route
	Route  struct {
		Visitor *Visitor
		URI     string
		Method  string
		View    *data.View
		Config
	}

	Config struct {
		ReturnSingle bool
	}

	Visitor struct {
		shared.Reference
		Name     string
		_visitor LifecycleVisitor
	}

	Router struct {
		*Resource
		serviceRouter *toolbox.ServiceRouter
	}
)

func NewVisitor(name string, visitor LifecycleVisitor) *Visitor {
	return &Visitor{
		Name:     name,
		_visitor: visitor,
	}
}

func (r *Route) Init(ctx context.Context, resource *Resource) error {
	if err := r.View.Init(ctx, resource.Resource); err != nil {
		return err
	}

	if err := r.initVisitor(resource); err != nil {
		return err
	}

	return nil
}

func (r *Route) initVisitor(resource *Resource) error {
	if r.Visitor == nil {
		r.Visitor = &Visitor{}
		return nil
	}

	if r.Visitor.Reference.Ref != "" {
		refVisitor, err := resource._visitors.Lookup(r.Visitor.Ref)
		if err != nil {
			return err
		}

		r.Visitor.inherit(refVisitor)
	}

	return nil
}

func (v *Visitor) inherit(visitor *Visitor) {
	v._visitor = visitor._visitor
}

func (r *Router) Handle(response http.ResponseWriter, request *http.Request) error {
	if err := r.serviceRouter.Route(response, request); err != nil {
		return err
	}

	return nil
}

func New(resource *Resource) *Router {
	router := &Router{
		Resource: resource,
	}

	router.Init(resource.Routes)

	return router
}

func (r *Router) Init(routes Routes) {
	r.initServiceRouter(routes)
}

func (r *Router) initServiceRouter(routes Routes) {
	routings := make([]toolbox.ServiceRouting, len(routes))
	for i, route := range routes {
		routings[i] = toolbox.ServiceRouting{
			URI:        route.URI,
			Handler:    r.viewHandler(routes[i]),
			HTTPMethod: route.Method,
			Parameters: []string{"@httpResponseWriter", "@httpRequest"},
		}
	}

	r.serviceRouter = toolbox.NewServiceRouter(routings...)
}

func (r *Router) Serve(serverPath string) error {
	return http.ListenAndServe(serverPath, r)
}

func (r *Router) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	err := r.serviceRouter.Route(writer, request)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func (r *Router) viewHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request) {
		if !r.runBeforeFetch(response, request, route) {
			return
		}

		destValue := reflect.New(route.View.Schema.SliceType())
		dest := destValue.Interface()
		session := reader.NewSession(dest, route.View)
		session.HttpRequest = request
		session.MatchedPath = route.URI

		selectors, err := r.createSelectors(route, request)
		if err != nil {
			response.Write([]byte(err.Error()))
			response.WriteHeader(http.StatusBadRequest)
			return
		}

		session.Selectors = selectors

		service := reader.New()
		if err := service.Read(context.TODO(), session); err != nil {
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
			return
		}

		if !r.runAfterFetch(response, request, route, dest) {
			return
		}

		r.writeResponse(route, request, response, destValue)
	}
}

func (r *Router) runBeforeFetch(response http.ResponseWriter, request *http.Request, route *Route) (shouldContinue bool) {
	if actual, ok := route.Visitor._visitor.(BeforeFetcher); ok {
		closed, err := actual.BeforeFetch(response, request)
		if closed {
			return false
		}

		if err != nil {
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
			return false
		}
	}
	return true
}

func (r *Router) runAfterFetch(response http.ResponseWriter, request *http.Request, route *Route, dest interface{}) (shouldContinue bool) {
	if actual, ok := route.Visitor._visitor.(AfterFetcher); ok {
		responseClosed, err := actual.AfterFetch(dest, response, request)
		if responseClosed {
			return false
		}

		if err != nil {
			response.WriteHeader(http.StatusBadRequest)
			response.Write([]byte(err.Error()))
			return false
		}
	}

	return true
}

func (r *Router) writeResponse(route *Route, request *http.Request, response http.ResponseWriter, destValue reflect.Value) {
	asBytes, httpStatus, err := r.result(route, request, destValue)

	if err != nil {
		response.Write([]byte(err.Error()))
		response.WriteHeader(httpStatus)
		return
	}

	response.Write(asBytes)
	response.WriteHeader(httpStatus)
}

func (r *Router) result(route *Route, request *http.Request, destValue reflect.Value) ([]byte, int, error) {
	if !route.ReturnSingle {
		asBytes, err := json.Marshal(destValue.Elem().Interface())
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil
	}

	slicePtr := unsafe.Pointer(destValue.Pointer())
	sliceSize := route.View.Schema.Slice().Len(slicePtr)
	switch sliceSize {
	case 0:
		return nil, http.StatusNotFound, nil
	case 1:
		asBytes, err := json.Marshal(route.View.Schema.Slice().ValuePointerAt(slicePtr, 0))
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return asBytes, http.StatusOK, nil

	default:
		return nil, http.StatusInternalServerError, fmt.Errorf("for route %v expected query to return zero or one result but returned %v", request.RequestURI, sliceSize)
	}
}

func (r *Router) createSelectors(route *Route, request *http.Request) (data.Selectors, error) {
	selectors := data.Selectors{}

	params := request.URL.Query()
	for paramName, paramValue := range params {
		paramName = strings.ToLower(paramName)

		switch paramName {
		case string(Fields):
			if err := r.buildFields(&selectors, route, paramValue[0]); err != nil {
				return nil, err
			}

		case string(Offset):
			if err := r.buildOffset(&selectors, route, paramValue[0]); err != nil {
				return nil, err
			}

		case string(Limit):
			if err := r.buildLimit(&selectors, route, paramValue[0]); err != nil {
				return nil, err
			}

		case string(OrderBy):
			if err := r.buildOrderBy(&selectors, route, paramValue[0]); err != nil {
				return nil, err
			}

		case string(Criteria):
			if err := r.buildCriteria(&selectors, route, paramValue[0]); err != nil {
				return nil, err
			}

		}
	}

	return selectors, nil
}

func (r *Router) buildFields(selectors *data.Selectors, route *Route, fieldsQuery string) error {
	for _, field := range strings.Split(fieldsQuery, "|") {
		viewField := strings.Split(field, ".")

		switch len(viewField) {
		case 1:
			if err := r.canUseColumn(route.View, viewField[0]); err != nil {
				return err
			}

			selector := selectors.GetOrCreate(route.View.Name)
			selector.Columns = append(selector.Columns, field)

		case 2:
			view, err := r.viewByPrefix(viewField[0], route)
			if err != nil {
				return err
			}

			if err = r.canUseColumn(view, viewField[1]); err != nil {
				return err
			}

			selector := selectors.GetOrCreate(view.Name)
			selector.Columns = append(selector.Columns, viewField[1])

		default:
			return NewUnsupportedFormat(string(Fields), field)
		}
	}

	return nil
}

func (r *Router) canUseColumn(view *data.View, columnName string) error {
	column, ok := view.ColumnByName(columnName)
	if !ok {
		return fmt.Errorf("not found column %v in view %v", columnName, view.Name)
	}

	if !column.Filterable {
		return fmt.Errorf("column %v is not filterable", columnName)
	}

	return nil
}

func (r *Router) viewByPrefix(prefix string, route *Route) (*data.View, error) {
	viewName, ok := r.ViewPrefix[prefix]
	if !ok {
		return nil, NewUnspecifiedPrefix(prefix)
	}

	view, err := route.View.AnyOfViews(viewName)
	if err != nil {
		return nil, err
	}

	return view, nil
}

func (r *Router) buildOffset(selectors *data.Selectors, route *Route, offsetQuery string) error {
	for _, offset := range strings.Split(offsetQuery, "|") {
		viewOffset := strings.Split(offset, ".")
		switch len(viewOffset) {
		case 1:
			if !route.View.CanUseSelectorOffset() {
				return fmt.Errorf("can't use selector offset on %v view", route.View.Name)
			}

			if err := r.updateSelectorOffset(selectors, viewOffset[1], route.View.Name); err != nil {
				return err
			}

		case 2:
			view, err := r.viewByPrefix(viewOffset[0], route)
			if err != nil {
				return err
			}

			if !view.CanUseSelectorOffset() {
				return fmt.Errorf("can't use selector offset on %v view", route.View.Name)
			}

			if err = r.updateSelectorOffset(selectors, viewOffset[1], view.Name); err != nil {
				return err
			}

		default:
			return NewUnsupportedFormat(string(Offset), offset)
		}
	}

	return nil
}

func (r *Router) updateSelectorOffset(selectors *data.Selectors, offset string, viewName string) error {
	offsetConv, err := strconv.Atoi(offset)
	if err != nil {
		return err
	}

	selector := selectors.GetOrCreate(viewName)
	selector.Offset = offsetConv
	return nil
}

func (r *Router) buildLimit(selectors *data.Selectors, route *Route, limitQuery string) error {
	for _, limit := range strings.Split(limitQuery, "|") {
		viewLimit := strings.Split(limit, ".")
		switch len(viewLimit) {
		case 1:
			if !route.View.CanUseSelectorLimit() {
				return fmt.Errorf("can't use selector limit on %v view", route.View.Name)
			}

			if err := r.updateSelectorLimit(selectors, viewLimit[0], route.View.Name); err != nil {
				return err
			}

		case 2:
			view, err := r.viewByPrefix(viewLimit[0], route)
			if err != nil {
				return err
			}

			if !view.CanUseSelectorLimit() {
				return fmt.Errorf("can't use selector limit on %v view", route.View.Name)
			}

			if err = r.updateSelectorLimit(selectors, viewLimit[1], view.Name); err != nil {
				return err
			}

		default:
			return NewUnsupportedFormat(string(Limit), limit)
		}
	}

	return nil
}

func (r *Router) updateSelectorLimit(selectors *data.Selectors, limit string, viewName string) error {
	limitConv, err := strconv.Atoi(limit)
	if err != nil {
		return err
	}

	selector := selectors.GetOrCreate(viewName)
	selector.Limit = limitConv
	return nil
}

func (r *Router) buildOrderBy(selectors *data.Selectors, route *Route, orderByQuery string) error {
	for _, orderBy := range strings.Split(orderByQuery, "|") {
		viewOrderBy := strings.Split(orderBy, ".")

		switch len(viewOrderBy) {
		case 1:
			if err := r.canUseOrderBy(route.View, viewOrderBy[0]); err != nil {
				return err
			}

			selector := selectors.GetOrCreate(route.View.Name)
			selector.OrderBy = viewOrderBy[0]

		case 2:
			view, err := r.viewByPrefix(viewOrderBy[0], route)
			if err != nil {
				return err
			}

			if err = r.canUseOrderBy(view, viewOrderBy[1]); err != nil {
				return err
			}

			selector := selectors.GetOrCreate(route.View.Name)
			selector.OrderBy = viewOrderBy[1]

		default:
			return NewUnsupportedFormat(string(OrderBy), orderBy)
		}
	}
	return nil
}

func (r *Router) canUseOrderBy(view *data.View, orderBy string) error {
	if !view.CanUseSelectorOrderBy() {
		return fmt.Errorf("can't use orderBy %v on view %v", orderBy, view.Name)
	}

	_, ok := view.ColumnByName(orderBy)
	if !ok {
		return fmt.Errorf("not found column %v on view %v", orderBy, view.Name)
	}

	return nil
}

func (r *Router) buildCriteria(selectors *data.Selectors, route *Route, criteriaQuery string) error {
	for _, criteria := range strings.Split(criteriaQuery, "|") {
		viewCriteria := strings.Split(criteria, ".")

		switch len(viewCriteria) {
		case 1:
			if err := r.addSelectorCriteria(selectors, route.View, viewCriteria[0]); err != nil {
				return err
			}

		case 2:
			view, err := r.viewByPrefix(viewCriteria[0], route)
			if err != nil {
				return err
			}

			if err = r.addSelectorCriteria(selectors, view, viewCriteria[1]); err != nil {
				return err
			}

		default:
			return NewUnsupportedFormat(string(Criteria), criteria)
		}
	}

	return nil
}

func (r *Router) addSelectorCriteria(selectors *data.Selectors, view *data.View, criteria string) error {
	if !view.CanUseSelectorCriteria() {
		return fmt.Errorf("can't use criteria on view %v", view.Name)
	}

	criteriaSanitized, err := r.sanitizeCriteria(criteria, view)
	if err != nil {
		return err
	}

	selector := selectors.GetOrCreate(view.Name)
	selector.Criteria = &data.Criteria{
		Expression: criteriaSanitized,
	}
	return nil
}

func (r *Router) sanitizeCriteria(criteria string, view *data.View) (string, error) {
	node, err := sanitize.Parse([]byte(criteria))
	if err != nil {
		return "", err
	}

	sb := strings.Builder{}
	if err = node.Sanitize(&sb, view.IndexedColumns()); err != nil {
		return "", err
	}

	return sb.String(), nil
}
