package encoding

// Format codes from the AMQP 1.0 type system.
const (
	codeDescribed = 0x00

	codeNull       = 0x40
	codeBoolTrue   = 0x41
	codeBoolFalse  = 0x42
	codeUint0      = 0x43
	codeUlong0     = 0x44
	codeList0      = 0x45
	codeUbyte      = 0x50
	codeByte       = 0x51
	codeSmallUint  = 0x52
	codeSmallUlong = 0x53
	codeSmallInt   = 0x54
	codeSmallLong  = 0x55
	codeBool       = 0x56
	codeUshort     = 0x60
	codeShort      = 0x61
	codeUint       = 0x70
	codeInt        = 0x71
	codeFloat      = 0x72
	codeChar       = 0x73
	codeDecimal32  = 0x74
	codeUlong      = 0x80
	codeLong       = 0x81
	codeDouble     = 0x82
	codeTimestamp  = 0x83
	codeDecimal64  = 0x84
	codeDecimal128 = 0x94
	codeUUID       = 0x98
	codeVbin8      = 0xa0
	codeStr8       = 0xa1
	codeSym8       = 0xa3
	codeVbin32     = 0xb0
	codeStr32      = 0xb1
	codeSym32      = 0xb3
	codeList8      = 0xc0
	codeMap8       = 0xc1
	codeList32     = 0xd0
	codeMap32      = 0xd1
	codeArray8     = 0xe0
	codeArray32    = 0xf0
)
