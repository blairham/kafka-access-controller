// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafkaaccess

import (
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// renderRules renders the chart's PrometheusRule with the given extra --set
// flags and returns the manifest text.
func renderRules(t *testing.T, sets ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not on PATH")
	}
	chart, err := filepath.Abs(filepath.Join("..", "..", "..", "charts", "kafka-access-controller"))
	if err != nil {
		t.Fatal(err)
	}
	args := make([]string, 0, 7+2*len(sets))
	args = append(args,
		"template", "test", chart,
		"--show-only", "templates/prometheusrule.yaml",
		"--set", "prometheusRule.enabled=true",
	)
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput() //nolint:gosec // fixed binary, test-only args
	if err != nil {
		t.Fatalf("rendering the PrometheusRule: %v\n%s", err, out)
	}
	return string(out)
}

// recorder captures what NewMetrics registers.
type recorder struct{ names map[string]bool }

var fqName = regexp.MustCompile(`fqName: "([^"]+)"`)

func (r *recorder) Register(c prometheus.Collector) error {
	ch := make(chan *prometheus.Desc, 8)
	go func() { c.Describe(ch); close(ch) }()
	for d := range ch {
		if m := fqName.FindStringSubmatch(d.String()); m != nil {
			r.names[m[1]] = true
		}
	}
	return nil
}

func (r *recorder) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		_ = r.Register(c)
	}
}

func (*recorder) Unregister(prometheus.Collector) bool { return true }

// Every series an alert reads must be one the controller exports: the chart
// was copied from database-access-controller, and a stale metric name makes an alert
// that can never fire.
func TestAlertsReadExportedSeries(t *testing.T) {
	rules := renderRules(t, "prometheusRule.rules.notConverged.enabled=true")
	rec := &recorder{names: map[string]bool{}}
	New(Config{Registry: rec})
	if len(rec.names) != 5 {
		t.Fatalf("registered %d series, want 5: %v", len(rec.names), rec.names)
	}

	used := regexp.MustCompile(`\b[a-z_]+_controller_[a-z_]+\b`).FindAllString(rules, -1)
	if len(used) < 3 {
		t.Fatalf("found %d metric references in the rules; the extraction is broken:\n%s", len(used), rules)
	}
	for _, m := range used {
		if !rec.names[m] {
			t.Errorf("an alert reads %s, which the controller does not export (it exports %v)", m, rec.names)
		}
	}
	for _, alert := range []string{"KafkaAccessNotReady", "KafkaAccessStale", "KafkaAccessNotConverged"} {
		if !strings.Contains(rules, "- alert: "+alert+"\n") {
			t.Errorf("%s is not rendered", alert)
		}
	}
}
