package options

type (
	JwtVerifier struct {
		HMAC string `short:"a" long:"jwtHMAC" description:"HMACKeyPath|EncKey" `
		RSA  string `short:"j" long:"jwtRSA" description:"PublicKeyPath|EncKey" `
	}
)

func (v *JwtVerifier) Init() {
	v.HMAC = ensureAbsPath(v.HMAC)
	v.RSA = ensureAbsPath(v.RSA)
}
