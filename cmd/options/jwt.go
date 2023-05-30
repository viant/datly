package options

type (
	JwtVerifier struct {
		HMAC SecretKey `short:"j" long:"jwtHMAC" description:"HMACKeyPath|EncKey" `
		RSA  SecretKey `short:"r" long:"jwtRSA" description:"PublicKeyPath|EncKey" `
	}

	SecretKey string
)
