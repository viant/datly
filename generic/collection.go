package generic

//Collection represents generic collection
type Collection interface {
	//Add adds item to collection
	Add(values map[string]interface{})
	//AddObject add an object
	AddObject(object *Object)
	//Range calls handler with collection item
	Range(handler func(item interface{}) (toContinue bool, err error)) error
	//Objects calls handler with collection item object
	Objects(handler func(item *Object) (toContinue bool, err error)) error
	//Size returns collection size
	Size() int
	//Proto return collection component prototype
	Proto() *Proto
	//First returns first object
	First() *Object
	//Compact returns compacted collection representation
	Compact() *Compatcted
}
