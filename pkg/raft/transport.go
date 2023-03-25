package raft

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

func newHTTPClient() *http.Client {
	transport := http.Transport{
		Proxy: http.ProxyFromEnvironment,

		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,

		MaxIdleConns: 30,

		IdleConnTimeout:       60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := http.Client{
		Timeout:   10 * time.Second,
		Transport: &transport,
	}

	return &client
}

func (s *Server) startHTTPServer() error {
	listener, err := net.Listen("tcp", string(s.LocalAddress))
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w", s.LocalAddress, err)
	}

	s.Log.Info("listening on %s", s.LocalAddress)

	s.httpServer = &http.Server{
		Addr:              string(s.LocalAddress),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		defer func() {
			if value := recover(); value != nil {
				msg := RecoverValueString(value)
				trace := StackTrace(10)
				s.Log.Error("panic: %s\n%s", msg, trace)
			}
		}()

		if err := s.httpServer.Serve(listener); err != http.ErrServerClosed {
			s.errChan <- fmt.Errorf("server error: %w", err)
			return
		}
	}()

	return nil
}

func (s *Server) stopHTTPServer() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s.httpServer.Shutdown(ctx)
}
