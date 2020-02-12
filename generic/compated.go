package generic

//Compact represents encodable collection
type Compatcted struct {
	Fields []*Field
	Data   [][]interface{}
}

//Update updates collection
func (c Compatcted) Update(collection Collection) error {
	proto := collection.Proto()
	for i := range c.Fields {
		proto.AddField(c.Fields[i])
	}
	for i := range c.Data {
		object, err := proto.Object(c.Data[i])
		if err != nil {
			return err
		}
		collection.AddObject(object)

	}
	return nil
}
