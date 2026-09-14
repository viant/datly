package auth

import xcodec "github.com/viant/xdatly/codec"

// VerifiesJWT recognizes the configured codec actually constructed by Service.
// Names, marker interfaces, and unrelated codec implementations cannot assert
// this authority. No keys or verifier configuration are exposed.
func VerifiesJWT(instance xcodec.Instance) bool {
	codec, ok := instance.(*claimsCodec)
	return ok && codec != nil && codec.verifier != nil
}
