package http

import (
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func TestHTTPMultiViewFormats(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, format := range []string{"csv", "xml", "tabular", "xls", "xlsx"} {
			for _, tc := range []struct {
				name, query  string
				csv          [][]string
				tabular, xml string
				sheets       [][]string
			}{
				{"root", "_fields=id&_fields=name", [][]string{{"Name", "MultiviewIdentity.ID"}, {"inventory", "1"}}, `[["MultiviewIdentity","Name"],[[["ID"],[1]],"inventory"]]`, `<MultiviewIdentity><ID>1</ID></MultiviewIdentity><Name>inventory</Name>`, [][]string{{"MultiviewIdentity", "Name"}, {"ID"}, {"1", "inventory"}}},
				{"zero_nil", "_fields=id&_fields=Product&product_fields=price&product_fields=note&product_limit=1", [][]string{{"MultiviewIdentity.ID", "Product.Price", "Product.Note"}, {"1", "0", "null"}}, `[["MultiviewIdentity","Product"],[[["ID"],[1]],[["Price","Note"],[0,null]]]]`, `<MultiviewIdentity><ID>1</ID></MultiviewIdentity><Product><Price>0</Price><Note nil="true"/></Product>`, [][]string{{"MultiviewIdentity", "Product"}, {"ID", "Price", "Note"}, {"1", "0"}}},
				{"relation_only", "_fields=Product&product_fields=name&product_limit=1", [][]string{{"Product.Name"}, {"second"}}, `[["Product"],[[["Name"],["second"]]]]`, `<Product><Name>second</Name></Product>`, [][]string{{"Product"}, {"Name"}, {"second"}}},
			} {
				t.Run(tc.name+"/"+format+"/direct="+map[bool]string{true: "true", false: "false"}[direct], func(t *testing.T) {
					var extra []string
					if tc.name == "root" {
						extra = []string{"DROP TABLE products", "DROP TABLE tags"}
					}
					h := multiViewHandler(t, direct, true, extra...)
					response := httptest.NewRecorder()
					h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?"+tc.query+"&_format="+format, nil))
					require.Equal(t, 200, response.Code, response.Body.String())
					switch format {
					case "csv":
						rows, err := csv.NewReader(bytes.NewReader(response.Body.Bytes())).ReadAll()
						require.NoError(t, err)
						require.Equal(t, tc.csv, rows)
					case "tabular":
						want := tc.tabular
						if !direct {
							want = `{"rows":` + want + `}`
						}
						require.JSONEq(t, want, response.Body.String())
					case "xml":
						holder := "Rows"
						if direct {
							holder = "row"
						}
						require.Equal(t, `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><result><`+holder+`>`+tc.xml+`</`+holder+`></result>`, response.Body.String())
						decoder := xml.NewDecoder(bytes.NewReader(response.Body.Bytes()))
						roots, depth := 0, 0
						for {
							token, err := decoder.Token()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							switch token.(type) {
							case xml.StartElement:
								if depth == 0 {
									roots++
								}
								depth++
							case xml.EndElement:
								depth--
							}
						}
						require.Equal(t, 1, roots)
					default:
						book, err := excelize.OpenReader(bytes.NewReader(response.Body.Bytes()))
						require.NoError(t, err)
						defer book.Close()
						require.Len(t, book.GetSheetList(), 1)
						rows, err := book.GetRows(book.GetSheetList()[0])
						require.NoError(t, err)
						require.Equal(t, tc.sheets, rows)
					}
				})
			}
		}
	}
}

func TestHTTPMultiViewCSVRelationProduct(t *testing.T) {
	h := multiViewHandler(t, false, true)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?_fields=Alternative&_fields=Product&alternative_fields=name&product_fields=name&_format=csv", nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	rows, err := csv.NewReader(bytes.NewReader(response.Body.Bytes())).ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]string{{"Alternative.Name", "Product.Name"}, {"first", "second"}, {"second", "second"}, {"first", "first"}, {"second", "first"}}, rows)
}

func TestHTTPMultiViewFormatConcurrentIsolation(t *testing.T) {
	h := multiViewHandler(t, false, true)
	var wg sync.WaitGroup
	failures := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			format := "csv"
			if i%2 == 1 {
				format = "xml"
			}
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?_fields=Product&product_fields=name&product_limit=1&_format="+format, nil))
			body := response.Body.String()
			if response.Code != 200 || !strings.Contains(body, "second") || strings.Contains(body, "InventoryID") || strings.Contains(body, "Price") {
				failures <- body
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

func TestHTTPMultiViewEmptyRelationFormats(t *testing.T) {
	h := multiViewHandler(t, false, true)
	for _, format := range []string{"csv", "xml", "tabular", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest("GET", "/inventory?_fields=Product&product_fields=name&product_criteria=id%3D999&_format="+format, nil))
			require.Equal(t, 200, response.Code, response.Body.String())
			switch format {
			case "csv":
				require.Equal(t, "\"Product.Name\"\nnull", response.Body.String())
			case "xml":
				require.Equal(t, `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><result><Rows><Product nil="true"/></Rows></result>`, response.Body.String())
			case "tabular":
				require.JSONEq(t, `{"rows":[["Product"],[null]]}`, response.Body.String())
			default:
				book, err := excelize.OpenReader(bytes.NewReader(response.Body.Bytes()))
				require.NoError(t, err)
				defer book.Close()
				rows, err := book.GetRows(book.GetSheetList()[0])
				require.NoError(t, err)
				require.Equal(t, [][]string{{"Product"}}, rows)
			}

		})
	}
}
