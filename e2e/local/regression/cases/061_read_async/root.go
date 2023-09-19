package _61_read_async

import "github.com/viant/xdatly/handler/response"

type Detail struct {
	Result interface{}     `parameter:"kind=output,in=data"`
	Status response.Status `parameter:"kind=output,in=status"`
	Job    interface{}     `parameter:"kind=output,in=job"`
}

type Root struct {
	AllDone bool   `parameter:"kind=output,in=async.done" json:",omitempty"`
	Detail  Detail `parameter:"kind=group" json:",omitempty"`
}
