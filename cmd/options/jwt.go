package options

type (
	JwtVerifier struct {
		HMAC string `short:"A" long:"jwtHMAC" description:"HMACKeyPath|EncKey" `
		RSA  string `short:"J" long:"jwtRSA" description:"PublicKeyPath|EncKey" `
	}
)

func (v *JwtVerifier) Init() {
	v.HMAC = ensureAbsPath(v.HMAC)
	v.RSA = ensureAbsPath(v.RSA)
}
