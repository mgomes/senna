package client

import (
	_ "embed"

	"github.com/mgomes/senna/internal/script"
)

//go:embed lua/batch_add_child.lua
var batchAddChildLua string

var batchAddChildScript = script.New("batch_add_child", batchAddChildLua)
