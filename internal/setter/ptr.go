package setter

func StringPtr(v string) *string {
	return &v
}

func StringsPtr(v ...string) *[]string {
	return &v
}

func BoolPtr(v bool) *bool {
	return &v
}

func IntPtr(v int) *int {
	return &v
}
