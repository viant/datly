package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestGeneratorBufferedDeleteCollections(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	const source = `#package('pkg/fees')
#setting($_ = $route('/fees','PATCH'))
#define($_ = $OwnerID<int>(param/owner))
#define($_ = $CampaignIDs<[]int>(param/campaignIds))
#define($_ = $Name<string>(param/name))
#define($_ = $FeeTypeIDs<[]int>(param/feeTypeIds))
#define($_ = $CategoryID<int>(param/category))
#define($_ = $Status<string>(output/status))
#define($_ = $Data<any>(output/body))
$dml.Execute("DELETE FROM fees WHERE owner_id=? AND (fees.campaign_id IN (?)) AND name=? AND fees.fee_type_id IN (?) AND category_id=? AND note='?' AND tenant_id=:datlyNativeCollection /* ? :datlyNativeCollection_ */", $Input.OwnerID, $Input.CampaignIDs, $Input.Name, $Input.FeeTypeIDs, $Input.CategoryID)`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Fees", Scope: "github.com/viant/datly/genfixture/pkg/fees", Connector: "main", Text: source}}
	generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.Settings.Mutation != "" {
		t.Fatal("statement acquired another mutation owner")
	}
	pkg := filepath.Join(root, "pkg/fees")
	before, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (Generator{Operation: "patch"}).Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil || string(before) != string(after) {
		t.Fatal("collection generation unstable")
	}
	runtime := `package fees
import("errors";"reflect";"strings";"testing")
type recordingDelete struct{sql string;args []any;calls int;failure error}
func(p *recordingDelete)Execute(sql string,args ...any)error{p.sql=sql;p.args=append([]any(nil),args...);p.calls++;return p.failure}
func TestDeleteBindings(t *testing.T){
 for _,n:=range []int{0,1,3,101}{for _,m:=range []int{0,1,4}{
  input:=&FeesInput{OwnerID:71,Name:"?",CategoryID:99}
  for i:=0;i<n;i++{input.CampaignIDs=append(input.CampaignIDs,i%3)}
  for i:=0;i<m;i++{input.FeeTypeIDs=append(input.FeeTypeIDs,10+i%2)}
  p:=&recordingDelete{}
  err:=NewFeesHandlerQueueStatement(p,input,&FeesOutput{})
  if n==0||m==0 {if err==nil||p.calls!=0{t.Fatal("empty collection reached enqueue")};continue}
  if err!=nil{t.Fatal(err)}
  marks:=func(n int)string{return strings.TrimSuffix(strings.Repeat("?,",n),",")}
  want:="DELETE FROM fees WHERE owner_id=? AND (fees.campaign_id IN ("+marks(n)+")) AND name=? AND fees.fee_type_id IN ("+marks(m)+") AND category_id=? AND note='?' AND tenant_id=:datlyNativeCollection /* ? :datlyNativeCollection_ */"
  args:=[]any{71};for _,id:=range input.CampaignIDs{args=append(args,id)};args=append(args,"?");for _,id:=range input.FeeTypeIDs{args=append(args,id)};args=append(args,99)
  if p.calls!=1||p.sql!=want||!reflect.DeepEqual(p.args,args){t.Fatalf("source order/protected SQL changed: %s %#v",p.sql,p.args)}
  failure:=errors.New("enqueue failed");p.failure=failure
  if err:=NewFeesHandlerQueueStatement(p,input,&FeesOutput{});!errors.Is(err,failure){t.Fatal("enqueue error lost")}
 }}
}
`
	runtime = strings.ReplaceAll(runtime, "FeesInput", generated.Result.Plan.Input.Type)
	runtime = strings.ReplaceAll(runtime, "FeesOutput", generated.Result.Plan.Output.Type)
	if err = os.WriteFile(filepath.Join(pkg, "delete_collections_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated delete collections: %v\n%s", err, output)
	}
}
