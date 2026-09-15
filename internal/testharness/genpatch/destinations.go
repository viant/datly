package genpatch

import "strings"

func DestinationDQL(module string) string {
	text := `#package('api/orders')
#import('requests','github.com/viant/datly/genfixture/requests')
#import('responses','github.com/viant/datly/genfixture/responses')
#import('rows','github.com/viant/datly/genfixture/entities')
#import('items','github.com/viant/datly/genfixture/items')
#import('rh','github.com/viant/datly/genfixture/hooks/root')
#import('ch','github.com/viant/datly/genfixture/hooks/child')
#setting($_ = $input_type('requests.OrdersInput'))
#setting($_ = $output_type('responses.OrdersOutput'))
` + strings.Replace(strings.TrimPrefix(DQL, PackageDirective+"\n"), "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*, type(o,'rows.Order'), dest(o,'order.go'), type(Items,'items.Item'), dest(Items,'item.go'), entity_hooks(o,'rh.Hooks'),entity_hooks(Items,'ch.Hooks'),", 1)
	return strings.ReplaceAll(text, "github.com/viant/datly/genfixture", module)
}
