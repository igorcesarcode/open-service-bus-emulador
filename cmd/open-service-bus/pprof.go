package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
)

// servePprof serves the profiling endpoints on addr, apart from the
// emulator's ports. Block and mutex profiles are sampled, which costs
// little.
func servePprof(addr string, logger *slog.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	runtime.SetBlockProfileRate(1000)
	runtime.SetMutexProfileFraction(100)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	logger.Info("profiling enabled", "pprof", "http://"+ln.Addr().String()+"/debug/pprof/")
	go func() {
		if err := http.Serve(ln, mux); err != nil {
			logger.Error("pprof server stopped", "err", err)
		}
	}()
	return nil
}
