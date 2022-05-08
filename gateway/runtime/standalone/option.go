package standalone

type Options struct {
	ConfigURL string `short:"c" long:"cfg" description:"config URI"`
	Version   bool   `short:"v" long:"version" description:"Version"`
}
