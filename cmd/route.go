package cmd

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/cmd/option"
)

func (s *serverBuilder) BuildRoute(ctx context.Context) (*option.Route, error) {
	routeOption := &option.Route{}
	if !(s.options.SQLXLocation != "" && url.Scheme(s.options.SQLLocation, "e") == "e") {
		return routeOption, nil
	}

	sourceURL := normalizeURL(s.options.SQLXLocation)
	SQLData, err := s.fs.DownloadWithURL(context.Background(), sourceURL)
	if err != nil {
		return nil, err
	}

	SQL, err := s.extractAndUpdateURIParams(routeOption, SQLData)
	if err != nil {
		return routeOption, err
	}

	SQL = s.extractAndUpdateParamHints(routeOption, SQL)
	routeOption.SetErr(s.updateRouteSQLMeta(ctx, routeOption, SQL))
	return routeOption, nil
}

func (s *serverBuilder) extractAndUpdateURIParams(routeOption *option.Route, SQLData []byte) (string, error) {
	SQL, uriParams, err := extractSetting(string(SQLData), routeOption)
	if err != nil {
		return "", fmt.Errorf("invalid settings: %v", err)
	}

	routeOption.URIParams = uriParams
	return SQL, nil
}

func (s *serverBuilder) extractAndUpdateParamHints(r *option.Route, SQL string) string {
	r.ParameterHints = ast.ExtractParameterHints(SQL)

	if len(r.ParameterHints) > 0 {
		SQL = ast.RemoveParameterHints(SQL, r.ParameterHints)
	}

	return SQL
}

func (s *serverBuilder) updateRouteSQLMeta(ctx context.Context, route *option.Route, SQL string) error {
	if ast.IsSQLExecMode(SQL) {
		return s.updateRouteInExecMode(ctx, route, SQL)
	}

	return s.updateRouteInReadMode(route, SQL)
}

func (s *serverBuilder) updateRouteInExecMode(ctx context.Context, route *option.Route, SQL string) error {
	sqlExecModeView, err := ast.Parse(SQL, route, route.ParameterHints)
	if err != nil {
		return err
	}

	route.ExecData = &option.ExecData{Meta: sqlExecModeView}
	s.updateMetaColumnTypes(ctx, sqlExecModeView, route)
	return nil
}

func (s *serverBuilder) updateRouteInReadMode(route *option.Route, SQL string) error {
	rData := &option.ReadData{}
	var err error

	rData.Table, rData.DataViewParams, err = ParseSQLx(SQL, route, route.ParameterHints)
	if err != nil {
		return err
	}

	if rData.Table != nil {
		updateGenerateOption(&s.options.Generate, rData.Table)
	}

	return nil
}
