package protobuf

// FeatureBit is the type for feature bits
type FeatureBit uint64

const ()

// HasFeature returns whether a particular bit is set in the ping message
func (p *PingMessage) HasFeature(feature FeatureBit) bool {
	return p.GetFeatureBits()&uint64(feature) != 0
}

// SupportedFeatures returns the set of features supported by this build
func SupportedFeatures() uint64 {
	return uint64(0)
}
