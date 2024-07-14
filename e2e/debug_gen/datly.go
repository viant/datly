package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/gops/agent"
	"github.com/viant/afs"
	_ "github.com/viant/afs/embed"
	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/cmd"
	"github.com/viant/datly/cmd/env"
	_ "github.com/viant/dyndb"
	_ "github.com/viant/scy/kms/blowfish"
	_ "github.com/viant/sqlx/metadata/product/bigquery"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/toolbox"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	Version      = "development"
	BuildTimeInS string
)

func init() {
	if BuildTimeInS != "" {
		seconds, err := strconv.Atoi(BuildTimeInS)
		if err != nil {
			panic(err)
		}

		env.BuildTime = time.Unix(int64(seconds), 0)
	}
}

type ConsoleWriter struct {
}

func (c *ConsoleWriter) Write(data []byte) (n int, err error) {
	fmt.Println(string(data))
	return len(data), nil
}

type Gen struct {
	Name, URL, Args string
}

func main() {

	baseDir := filepath.Join(toolbox.CallerDirectory(3), "..")
	fmt.Printf("base: %v\n", baseDir)
	caseName := "029_generate_put_one_many"
	caseFolder := filepath.Join(baseDir, "local/regression/cases/", caseName)
	gen, err := loadGen(caseFolder, caseName)
	if err != nil {
		log.Fatal(err)
	}
	toolbox.Dump(gen)
	os.Args = []string{"",
		"-N=" + gen.Name,
		"-X=" + gen.URL,
		"-C=dev|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
		"-C=dyndb|dynamodb|dynamodb://localhost:8000/us-west-1?key=dummy&secret=dummy",
		fmt.Sprintf("-j='%v/local/jwt/public.enc|blowfish://default'", baseDir),
		"-w=autogen",
	}

	fmt.Printf("[INFO] Build time: %v\n", env.BuildTime.String())

	fmt.Printf("%v\n", os.Args)
	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()
	os.Chdir(path.Join(baseDir, "local"))

	err = cmd.RunApp(Version, os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

}

func loadGen(baseURL string, name string) (*Gen, error) {
	gen := &Gen{}
	fs := afs.New()
	data, err := fs.DownloadWithURL(context.Background(), path.Join(baseURL, "gen.json"))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, gen)
	if err != nil {
		return nil, err
	}
	caseID := name[4:]
	gen.URL = strings.ReplaceAll(gen.URL, "$path", baseURL)
	gen.Name = strings.ReplaceAll(gen.Name, "$tagId", caseID)
	return gen, nil
}
