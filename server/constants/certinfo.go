package constants

import "encoding/asn1"

type CertPermission string

var (
	CertPermissionCA    CertPermission = "CA"
	CertPermissionNode  CertPermission = "node"
	CertPermissionAdmin CertPermission = "admin"
	CertPermissionRead  CertPermission = "read"
	CertPermissionWrite CertPermission = "write"
)

var (
	OIDClusterName = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 37476, 9000, 56, 1, 1}
	OIDRegionName  = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 37476, 9000, 56, 1, 2}
	OIDNodeName    = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 37476, 9000, 56, 1, 3}
	OIDNodeID      = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 37476, 9000, 56, 1, 4}
	OIDPermission  = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 37476, 9000, 56, 1, 5}
	OIDRepoName    = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 37476, 9000, 56, 1, 6}
)
