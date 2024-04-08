package sdk

import goplugin "github.com/hashicorp/go-plugin"

var Handshake = goplugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "USSD_PLUGIN",
	MagicCookieValue: "hello",
}
