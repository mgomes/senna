package lua

import (
	_ "embed"

	"github.com/mgomes/senna/internal/script"
)

//go:embed batch_complete.lua
var batchCompleteLua string

var BatchCompleteScript = script.New("batch_complete", batchCompleteLua)
