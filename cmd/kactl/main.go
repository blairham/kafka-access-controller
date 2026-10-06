// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command kactl runs the controller's plan from a terminal. `kactl plan`
// prints the operations the controller would run against the real cluster
// and changes nothing; `kactl apply` runs them; `kactl iam-policy` prints the
// IAM policy a resource under authorization iam needs.
package main

import (
	"fmt"
	"os"

	"github.com/hashicorp/cli"
)

var version = "dev"

func main() {
	c := cli.NewCLI("kactl", version)
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"plan":       func() (cli.Command, error) { return &planCommand{}, nil },
		"apply":      func() (cli.Command, error) { return &applyCommand{}, nil },
		"iam-policy": func() (cli.Command, error) { return &iamPolicyCommand{}, nil },
	}

	status, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "kactl: %v\n", err)
	}
	os.Exit(status)
}
