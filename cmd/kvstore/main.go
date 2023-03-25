package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/exograd/go-program"
	"github.com/galdor/go-raft/pkg/raft"
)

func main() {
	p := program.NewProgram("kvstore", "a simple key-value storage server")

	p.AddOption("c", "cfg", "path", "", "the path of the configuration file")
	p.AddOption("d", "data-dir", "path", ".", "the path of the data directory")
	p.AddArgument("id", "the server identifier")

	p.SetMain(run)

	p.ParseCommandLine()
	p.Run()
}

func run(p *program.Program) {
	// Configuration file
	cfg := DefaultCfg()
	if p.IsOptionSet("cfg") {
		cfgPath := p.OptionValue("cfg")

		p.Info("loading configuration from %q", cfgPath)

		if err := cfg.LoadFile(cfgPath); err != nil {
			p.Fatal("cannot load configuration: %v", err)
		}
	}

	id := raft.ServerId(p.ArgumentValue("id"))

	// Data directory
	dataDir0 := p.OptionValue("data-dir")
	dataDir := path.Join(dataDir0, fmt.Sprintf("kvstore-%s", id))
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		p.Fatal("cannot create %s: %v", dataDir, err)
	}

	p.Info("using data directory %q", dataDir)

	// Server
	serverCfg := raft.ServerCfg{
		Id:      id,
		Servers: cfg.Servers,

		DataDirectory: dataDir,

		Logger: p,
	}

	server, err := raft.NewServer(serverCfg)
	if err != nil {
		p.Fatal("cannot create server: %v", err)
	}

	// Main
	errChan := make(chan error)
	defer close(errChan)

	if err := server.Start(errChan); err != nil {
		p.Fatal("cannot start server: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errChan:
		p.Fatal("server error: %v", err)

	case signo := <-sigChan:
		fmt.Fprintln(os.Stderr)
		p.Info("received signal %d (%v)", signo, signo)
		server.Stop()
	}
}
