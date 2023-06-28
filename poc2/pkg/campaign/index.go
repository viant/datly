package campaign

import "fmt"

type IndexedCampaigns map[int]*Campaign
type CampaignsSlice []*Campaign

func (c CampaignsSlice) IndexBy(field string) IndexedCampaigns {
	var result = IndexedCampaigns{}
	switch field {
	case "id":
		for i, item := range c {
			result[item.Id] = c[i]
		}
	default:
		panic(fmt.Sprintf("unsupported index field: %v", field))
	}
	return result
}
func (c IndexedCampaigns) Has(key int) bool {
	_, ok := c[key]
	return ok
}
