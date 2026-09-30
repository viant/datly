package server

import "context"

func authorizeCatalogTool(ctx context.Context, service Service, name, action string) error {
	if guard, ok := service.(interface {
		AuthorizeCatalogTool(context.Context, string, string) error
	}); ok {
		return guard.AuthorizeCatalogTool(ctx, name, action)
	}
	return nil
}

func authorizeCatalogResource(ctx context.Context, service Service, uri, action string) error {
	if guard, ok := service.(interface {
		AuthorizeCatalogResource(context.Context, string, string) error
	}); ok {
		return guard.AuthorizeCatalogResource(ctx, uri, action)
	}
	return nil
}
