package raft

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
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

func (s *Server) sendMsg(recipientId ServerId, msg RPCMsg) error {
	s.Log.Debug(2, "sending %v to %s", msg, recipientId)

	// Encode the message
	msgData, err := EncodeRPCMsg(msg)
	if err != nil {
		return fmt.Errorf("cannot encode message: %w", err)
	}

	// Obtain the address of the recipient
	recipient, found := s.Cfg.Servers[recipientId]
	if !found {
		return fmt.Errorf("unknown recipient id %q", recipientId)
	}

	address := recipient.PublicAddress

	// Create the HTTP request
	uri := url.URL{
		Scheme: "http",
		Host:   string(address),
	}

	req, err := http.NewRequest("POST", uri.String(), bytes.NewReader(msgData))
	if err != nil {
		return fmt.Errorf("cannot create http request: %w", err)
	}

	req.Header.Set("X-Raft-Source-Id", string(s.Id))

	// Send the request asynchronously to avoid blocking the server
	go s.sendMsgRequest(address, msg, req)

	return nil
}

func (s *Server) sendMsgRequest(address ServerAddress, msg RPCMsg, req *http.Request) {
	defer func() {
		if value := recover(); value != nil {
			msg := RecoverValueString(value)
			trace := StackTrace(10)
			s.Log.Error("cannot send request: panic: %s\n%s", msg, trace)
		}
	}()

	// Send the request and wait for the response
	res, err := s.httpClient.Do(req)
	if err != nil {
		s.Log.Error("cannot send %v to %s: %v", msg, address, err)
		return
	}
	defer res.Body.Close()

	// Check the response status
	if res.StatusCode != 204 {
		var msg string

		body, err := ioutil.ReadAll(res.Body)
		if err == nil {
			msg = string(body)

			if idx := strings.IndexAny(msg, "\r\n"); idx > 0 {
				msg = msg[:idx]
			}

			if msg != "" {
				msg = ": " + msg
			}
		} else {
			s.Log.Error("cannot read response from %s: %v", address, err)
		}

		s.Log.Error("http request to %s failed with status %d%s",
			address, res.StatusCode, msg)
	}
}

func (s *Server) broadcastMsg(msg RPCMsg) {
	for id := range s.Cfg.Servers {
		if id == s.Id {
			continue
		}

		s.sendMsg(id, msg)
	}
}
