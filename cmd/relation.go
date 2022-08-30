package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	option "github.com/viant/datly/cmd/option"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"strings"
)

func lookupView(resource *view.Resource, name string) *view.View {
	for _, candidate := range resource.Views {
		if candidate.Name == name {
			return candidate
		}
		if candidate.Table == name {
			return candidate
		}
	}
	return nil
}

func (s *serverBuilder) buildXRelations(ctx context.Context, viewRoute *router.Route, xTable *option.Table) error {
	if len(xTable.Joins) == 0 {
		return nil
	}

	for _, join := range xTable.Joins {
		relView := &view.View{
			Name:  join.Table.Alias,
			Table: join.Table.Name,
			Selector: &view.Config{
				Limit: 40,
			},
		}

		if _, err := s.addViewConn(s.options.Connector.DbName, relView); err != nil {
			return err
		}

		if err := s.updateView(ctx, join.Table, relView); err != nil {
			return err
		}

		s.route.Resource.AddViews(relView)
		var cardinality = view.Many
		if join.ToOne {
			cardinality = view.One
		}

		ownerView := lookupView(s.route.Resource, join.Owner.Ref)
		if ownerView == nil {
			return fmt.Errorf("failed to lookup view: %v", join.Owner.Name)
		}

		columnNames := make([]string, 0)
		for _, column := range xTable.Inner {
			columnName := column.Alias
			if columnName == "" {
				columnName = column.Name
			}

			columnNames = append(columnNames, columnName)
		}

		newCase, err := detectCaseFormat(xTable).Caser()
		if err != nil {
			return err
		}

		withView := &view.Relation{
			Name: ownerView.Name + "_" + join.Table.Alias,
			Of: &view.ReferenceView{
				View:   view.View{Reference: shared.Reference{Ref: join.Table.Alias}, Name: join.Table.Alias + "#"},
				Column: join.Key,
				Field:  join.Field,
			},
			Cardinality: cardinality,
			Column:      join.OwnerKey,
			ColumnAlias: join.KeyAlias,
			Holder:      newCase.Format(join.Table.Alias, format.CaseUpperCamel),

			IncludeColumn: true,
		}
		if join.Connector != "" {
			relView.Connector = connectorRef(join.Connector)
		}
		if join.Cache != nil {
			s.addCacheWithWarmup(relView, join)
		}
		ownerView.With = append(ownerView.With, withView)
		viewRoute.Index.Namespace[namespace(join.Table.Alias)] = join.Table.Alias + "#"

		if len(join.Table.Joins) > 0 {
			if err := s.buildXRelations(ctx, viewRoute, join.Table); err != nil {
				return err
			}
		}

	}
	return nil
}

func (s *serverBuilder) addCacheWithWarmup(relView *view.View, join *option.Join) {
	relView.Cache = join.Cache
	relView.SelfReference = join.Self
	if warmup := join.Warmup; len(warmup) > 0 {
		relView.Cache.Warmup = &view.Warmup{IndexColumn: join.Key}

		multiSet := &view.CacheParameters{}
		for k, v := range warmup {
			switch actual := v.(type) {
			case []interface{}:
				multiSet.Set = append(multiSet.Set, &view.ParamValue{Name: k, Values: actual})
			default:
				multiSet.Set = append(multiSet.Set, &view.ParamValue{Name: k, Values: []interface{}{actual}})
			}
		}
		relView.Cache.Warmup.Cases = append(relView.Cache.Warmup.Cases, multiSet)
	}
}

func connectorRef(name string) *view.Connector {
	return &view.Connector{Reference: shared.Reference{Ref: name}}
}

func (s *serverBuilder) updateView(ctx context.Context, table *option.Table, aView *view.View) error {
	if table == nil {
		return nil
	}

	s.logger.Write([]byte(fmt.Sprintf("Discovering  %v metadata ...\n", aView.Name)))
	s.updateTableColumnTypes(ctx, table)
	s.updateParameterTypes(table)
	if err := s.updateViewMeta(table, aView); err != nil {
		return err
	}

	if err := s.updateColumnsConfig(table, aView); err != nil {
		return err
	}

	if table.ViewMeta == nil {
		return nil
	}

	if err := s.buildSQLSource(aView, table); err != nil {
		return err
	}
	return nil
}

func (s *serverBuilder) updateViewMeta(table *option.Table, aView *view.View) error {
	fmt.Printf("TABLE HINT: %v %v\n", table.Name, table.ViewHint)
	tableMeta, err := s.tableMeta(table)
	if err != nil {
		return err
	}

	if tableMeta.Selector != nil {
		aView.Selector = tableMeta.Selector
	}

	if tableMeta.Cache != nil {
		aView.Cache = tableMeta.Cache
	}

	fmt.Printf("tableMeta: %v\n", tableMeta)
	if tableMeta.Self != nil {
		aView.SelfReference = tableMeta.Self
	}
	if tableMeta.AllowNulls != nil {
		aView.AllowNulls = tableMeta.AllowNulls
	}

	if tableMeta.Connector != "" {
		if _, err := s.addViewConn(tableMeta.Connector, aView); err != nil {
			return err
		}
	}

	return nil
}

func (s *serverBuilder) tableMeta(table *option.Table) (*option.TableMeta, error) {
	viewHint := strings.TrimSpace(strings.Trim(table.ViewHint, "/**/"))
	if viewHint == "" {
		return &table.TableMeta, nil
	}

	if err := json.Unmarshal([]byte(viewHint), &table.TableMeta); err != nil {
		return nil, err
	}
	return &table.TableMeta, nil
}

func (s *serverBuilder) buildSQLSource(aView *view.View, table *option.Table) error {
	templateParams := make([]*view.Parameter, len(table.ViewMeta.Parameters))
	for i, param := range table.ViewMeta.Parameters {
		templateParams[i] = convertMetaParameter(param)
	}

	template := &view.Template{
		Parameters: templateParams,
	}

	aView.Template = template

	if err := s.updateTemplateSource(template, table); err != nil {
		return err
	}

	if err := s.updateViewSource(aView, table); err != nil {
		return err
	}

	return nil
}

func convertMetaParameter(param *option.Parameter) *view.Parameter {
	var aCodec *view.Codec
	if param.Codec != "" {
		aCodec = &view.Codec{Reference: shared.Reference{Ref: param.Codec}}
	}

	return &view.Parameter{
		Name:  param.Id,
		Codec: aCodec,
		Schema: &view.Schema{
			DataType:    param.DataType,
			Cardinality: param.Cardinality,
		},
		In: &view.Location{
			Kind: view.Kind(param.Kind),
			Name: param.Name,
		},
		Required: param.Required,
	}
}

func (s *serverBuilder) updateViewSource(aView *view.View, table *option.Table) error {
	if table.ViewMeta.From == "" {
		return nil
	}
	URI, err := s.uploadSQL(table.Alias, table.ViewMeta.From)
	if err != nil {
		return err
	}

	aView.FromURL = URI
	return nil
}

func (s *serverBuilder) updateTemplateSource(template *view.Template, table *option.Table) error {
	if table.ViewMeta.Source == "" {
		return nil
	}

	URI, err := s.uploadSQL(table.Alias, table.ViewMeta.Source)
	if err != nil {
		return err
	}

	template.SourceURL = URI
	return nil
}

func (s *serverBuilder) uploadSQL(fileName string, SQL string) (string, error) {
	sourceURL := s.options.SQLURL(fileName, true)
	fs := afs.New()
	if err := fs.Upload(context.Background(), sourceURL, file.DefaultFileOsMode, strings.NewReader(SQL)); err != nil {
		return "", err
	}

	skipped := 0
	anIndex := strings.LastIndexFunc(sourceURL, func(r rune) bool {
		if r == '/' {
			skipped++
		}

		if skipped == 2 {
			return true
		}
		return false
	})
	sourceURL = sourceURL[anIndex+1:]
	return sourceURL, nil
}

func (s *serverBuilder) updateColumnsConfig(table *option.Table, aView *view.View) error {

	aView.ColumnsConfig = map[string]*view.ColumnConfig{}
	for _, item := range table.Inner {
		if item.Comments == "" {
			continue
		}

		configJSON := strings.TrimPrefix(item.Comments, "/*")
		configJSON = strings.TrimSuffix(configJSON, "*/")
		configJSON = strings.TrimSpace(configJSON)

		aConfig := &view.ColumnConfig{}
		if err := json.Unmarshal([]byte(configJSON), aConfig); err != nil {
			fmt.Printf(err.Error())
			continue
		}

		aView.ColumnsConfig[view.NotEmptyOf(item.Alias, item.Name)] = aConfig
	}
	return nil
}
