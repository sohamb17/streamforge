//go:build js && wasm

// Command simwasm exposes the in-browser simulation to JavaScript:
//
//	streamforgeSim.advance(simMs)   run the simulation forward
//	streamforgeSim.snapshot()       JSON of console.Snapshot
//	streamforgeSim.chaos(action)    "" or a reason it was refused
//	streamforgeSim.setSpeed(x)      event-time speed-up (default 60)
//
// Build: GOOS=js GOARCH=wasm go build -o web/public/sim.wasm ./cmd/simwasm
package main

import (
	"encoding/json"
	"syscall/js"
	"time"

	"github.com/sohamb17/streamforge/internal/simdemo"
)

func main() {
	e, err := simdemo.New(time.Now().UnixNano())
	if err != nil {
		js.Global().Get("console").Call("error", "simdemo: "+err.Error())
		return
	}
	api := js.Global().Get("Object").New()
	api.Set("advance", js.FuncOf(func(_ js.Value, args []js.Value) any {
		e.Advance(args[0].Float())
		return nil
	}))
	api.Set("snapshot", js.FuncOf(func(_ js.Value, _ []js.Value) any {
		b, _ := json.Marshal(e.Snapshot())
		return string(b)
	}))
	api.Set("chaos", js.FuncOf(func(_ js.Value, args []js.Value) any {
		return e.Chaos(args[0].String())
	}))
	api.Set("setSpeed", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if x := args[0].Float(); x > 0 && x <= 600 {
			e.Speedup = x
		}
		return nil
	}))
	js.Global().Set("streamforgeSim", api)
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("streamforge-sim-ready"))
	select {}
}
