package io

//Output represents an updater
type Output interface {
	Put(key string, value interface{})
}
