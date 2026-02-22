package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dkv/kv"
	"dkv/raft"
	"dkv/storage"
	"dkv/transport"
)

func main() {
	var (
		id      = flag.String("id", "", "Node ID (e.g. n1)")
		listen  = flag.String("listen", "", "Listen address (e.g. 127.0.0.1:9001)")
		peers   = flag.String("peers", "", "Cluster peers as id=addr,id=addr")
		dataDir = flag.String("data-dir", "", "Node data directory")
	)
	flag.Parse()

	if *id == "" || *listen == "" || *peers == "" || *dataDir == "" {
		fmt.Fprintln(os.Stderr, "required flags: --id --listen --peers --data-dir")
		os.Exit(2)
	}

	listenAddr, selfURL, err := normalizeListen(*listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --listen: %v\n", err)
		os.Exit(2)
	}
	cluster, err := parsePeers(*peers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --peers: %v\n", err)
		os.Exit(2)
	}
	cluster[*id] = selfURL

	wal, err := storage.OpenWAL(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open storage: %v\n", err)
		os.Exit(1)
	}

	rpcClient := transport.NewRaftHTTPClient()
	node, err := raft.NewNode(raft.Config{
		ID:        *id,
		Address:   selfURL,
		Cluster:   cluster,
		Storage:   wal,
		RPCClient: rpcClient,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init raft node: %v\n", err)
		os.Exit(1)
	}

	store := kv.NewStore()
	applyDone := make(chan struct{})
	go func() {
		defer close(applyDone)
		for msg := range node.ApplyCh() {
			if len(msg.Command) == 0 {
				continue
			}
			cmd, err := kv.DecodeCommand(msg.Command)
			if err != nil {
				log.Printf("node=%s apply decode error index=%d err=%v", *id, msg.Index, err)
				continue
			}
			store.Apply(cmd)
		}
	}()

	if err := node.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start raft node: %v\n", err)
		os.Exit(1)
	}

	srv := transport.NewServer(listenAddr, node, store)
	errCh := make(chan error, 1)
	go srv.Run(errCh)

	log.Printf("node=%s listen=%s dataDir=%s peers=%d started", *id, listenAddr, *dataDir, len(cluster))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("node=%s received signal=%s, shutting down", *id, sig.String())
	case err := <-errCh:
		log.Printf("node=%s server error: %v", *id, err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("node=%s shutdown http error: %v", *id, err)
	}
	if err := node.Stop(); err != nil {
		log.Printf("node=%s shutdown raft error: %v", *id, err)
	}
	<-applyDone
	log.Printf("node=%s stopped", *id)
}

func normalizeListen(listen string) (listenAddr string, baseURL string, err error) {
	if strings.HasPrefix(listen, "http://") || strings.HasPrefix(listen, "https://") {
		u, parseErr := url.Parse(listen)
		if parseErr != nil {
			return "", "", parseErr
		}
		if u.Host == "" {
			return "", "", fmt.Errorf("missing host")
		}
		return u.Host, u.Scheme + "://" + u.Host, nil
	}
	if strings.TrimSpace(listen) == "" {
		return "", "", fmt.Errorf("empty listen address")
	}
	return listen, "http://" + listen, nil
}

func parsePeers(raw string) (map[string]string, error) {
	out := make(map[string]string)
	parts := strings.Split(raw, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		pair := strings.SplitN(p, "=", 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf("peer must be id=addr: %q", p)
		}
		id := strings.TrimSpace(pair[0])
		addr := strings.TrimSpace(pair[1])
		if id == "" || addr == "" {
			return nil, fmt.Errorf("peer id and addr required: %q", p)
		}
		_, normalized, err := normalizeListen(addr)
		if err != nil {
			return nil, fmt.Errorf("peer %q: %w", id, err)
		}
		out[id] = normalized
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty peer list")
	}
	return out, nil
}
