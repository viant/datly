package shared

//Reference wraps Ref, objects with Reference can be build based on other object of the same type.
type Reference struct {
	Ref string `json:",omitempty" yaml:"ref,omitempty"`
}
