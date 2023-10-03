package main

import (
	"github.com/viant/datly/cmd/env"
	"strconv"
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

	env.BuildType = env.BuildTypeKindLambdaS3
}
