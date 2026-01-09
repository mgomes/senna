package client

import (
	_ "embed"

	"github.com/mgomes/senna/internal/lua"
	"github.com/mgomes/senna/internal/script"
)

//go:embed lua/batch_add_child.lua
var batchAddChildLua string

//go:embed lua/batch_remove_child.lua
var batchRemoveChildLua string

var batchAddChildScript = script.New("batch_add_child", batchAddChildLua)
var batchRemoveChildScript = script.New("batch_remove_child", batchRemoveChildLua)
var batchCompleteScript = lua.BatchCompleteScript
