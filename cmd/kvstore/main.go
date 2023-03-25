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

		p.Info("loading configuration from %s", cfgPath)

		if err := cfg.LoadFile(cfgPath); err != nil {
			p.Fatal("cannot load configuration: %v", err)
		}
	}

	id := raft.ServerId(p.ArgumentValue("id"))
	if _, found := cfg.Servers[id]; !found {
		p.Fatal("id %q not found in server set", id)
	}

	// Data directory
	dataDirPath0 := p.OptionValue("data-dir")
	dataDirPath := path.Join(dataDirPath0, fmt.Sprintf("kvstore-%s", id))
	if err := os.MkdirAll(dataDirPath, 0700); err != nil {
		p.Fatal("cannot create %s: %v", dataDirPath, err)
	}

	p.Info("using data directory %s", dataDirPath)

	// Server
	serverCfg := raft.ServerCfg{
		Id:      id,
		Servers: cfg.Servers,

		Logger: p,
	}

	server := raft.NewServer(serverCfg)

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
