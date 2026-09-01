package elasticsearch

import _ "embed" // enable embedding

//go:embed v6/visibility/index_template.json
var IndexTemplateV6 []byte

//go:embed v7/visibility/index_template.json
var IndexTemplateV7 []byte

//go:embed os2/visibility/index_template.json
var IndexTemplateOS2 []byte
