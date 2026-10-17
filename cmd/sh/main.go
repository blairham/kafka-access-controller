// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command sh is the image's /bin/sh: blairham/sh's gated shell, so that
// `kubectl exec <pod> -- /bin/sh -c '<script>'` can read /proc and
// /sys/fs/cgroup in a distroless image. It cannot exec, signal or write.
// See https://github.com/blairham/sh/blob/main/gated/README.md.
//
// Only install it in an image with no other shell or userland: beside
// busybox the gate protects nothing, because everything else can still exec.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/blairham/sh/gated"
)

func main() {
	os.Exit(run())
}

// run exists so the deferred stop runs before os.Exit.
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return gated.Main(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv("HOSTNAME"))
}
