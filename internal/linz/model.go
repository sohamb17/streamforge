// Package linz holds the sequential specification used to check recorded
// client histories for linearizability with Porcupine.
//
// The model is a map of independent registers. A history is linearizable if
// there is a single order of all operations, consistent with real time
// (an operation that returned before another was called comes first), in
// which every Get returns the value of the latest preceding Put.
package linz

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"
)

// Op kinds.
const (
	OpGet = 0
	OpPut = 1
)

// Input of one operation.
type Input struct {
	Op    int
	Key   string
	Value string
}

// Output of one operation. Unknown marks a Put whose outcome the client
// never learned (timeout, crash): it may or may not have taken effect, so
// it is recorded with an infinite return time and accepted anywhere after
// its call. Gets with unknown outcome are simply left out of the history.
type Output struct {
	Value   string
	Found   bool
	Unknown bool
}

type register struct {
	value string
	found bool
}

// Model is the register-map specification, partitioned by key.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(Input).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return register{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(register)
		in := input.(Input)
		out := output.(Output)
		if in.Op == OpPut {
			return true, register{value: in.Value, found: true}
		}
		return out.Value == st.value && out.Found == st.found, st
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(Input)
		out := output.(Output)
		if in.Op == OpPut {
			if out.Unknown {
				return fmt.Sprintf("put(%s, %s) -> ?", in.Key, in.Value)
			}
			return fmt.Sprintf("put(%s, %s)", in.Key, in.Value)
		}
		if !out.Found {
			return fmt.Sprintf("get(%s) -> <none>", in.Key)
		}
		return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
	},
	DescribeState: func(state interface{}) string {
		st := state.(register)
		if !st.found {
			return "<none>"
		}
		return st.value
	},
}

// Infinity is the return time recorded for operations with unknown outcome.
const Infinity = int64(1) << 62
