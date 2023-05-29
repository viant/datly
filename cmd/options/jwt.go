package options

type (
	JwtVerifier struct {
		HMAC SecretKey `short:"j" long:"jwtHMAC" description:"HMACKeyPath|EncKey" `
		RSA  SecretKey `short:"r" long:"jwtRSA" description:"PublicKeyPath|EncKey" `
	}

	SecretKey string
)

//-j = '${appPath}/e2e/local/jwt/public.enc|blowfish://default' -m = '${appPath}/e2e/local/jwt/hmac.enc|blowfish://default'
