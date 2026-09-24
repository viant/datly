package remote

import "testing"

func TestDecodeConfigRequiresOneObject(t *testing.T) {
	for _, input := range []string{
		`null`, `[]`, `"config"`, ``,
		`{} {}`, `{} null`, `{} trailing`,
		`{"client":{"surprise":true}}`,
		`{"cache":{"name":"auth","ttl":"5m","maxEntries":2}}`,
	} {
		t.Run(input, func(t *testing.T) {
			if config, err := DecodeConfig([]byte(input)); err == nil {
				t.Fatalf("accepted malformed configuration %q: %#v", input, config)
			}
		})
	}
	config, err := DecodeConfig([]byte("{\"client\":{\"transport\":\"http\"}} \n\t"))
	if err != nil || config == nil || config.Client.Transport != TransportHTTP {
		t.Fatalf("valid object with trailing whitespace: config=%#v err=%v", config, err)
	}
}
