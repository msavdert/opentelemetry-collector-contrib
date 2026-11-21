// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package oracledbreceiver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/oracledbreceiver/internal/metadata"
)

// scriptedResult is one answer of a scriptedClient: rows or an error.
type scriptedResult struct {
	rows []metricRow
	err  error
}

// scriptedClient answers successive queries with successive results; the last one repeats.
type scriptedClient struct {
	results  []scriptedResult
	calls    int
	deadline time.Time // deadline of the context of the last query, zero when it had none
}

func (c *scriptedClient) metricRows(ctx context.Context, _ ...any) ([]metricRow, error) {
	c.deadline, _ = ctx.Deadline()
	idx := min(c.calls, len(c.results)-1)
	c.calls++
	return c.results[idx].rows, c.results[idx].err
}

// closedConnector lets a test hold a non-nil *sql.DB; no connection is opened because the
// clients that would use it are replaced by scriptedClient.
type closedConnector struct{}

func (closedConnector) Connect(context.Context) (driver.Conn, error) { return nil, errQuery }
func (closedConnector) Driver() driver.Driver                        { return nil }

func okRow(role, mode string) scriptedResult {
	return scriptedResult{rows: []metricRow{{colDatabaseRole: role, colOpenMode: mode}}}
}

// newRefreshScraper builds a scraper with a non-nil database whose role query is scripted;
// every other query answers with an error.
func newRefreshScraper(t *testing.T, cfg metadata.MetricsBuilderConfig, start oracleInstanceInfo, role *scriptedClient) (*oracleScraper, *observer.ObservedLogs) {
	t.Helper()
	db := sql.OpenDB(closedConnector{})
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	core, logs := observer.New(zapcore.DebugLevel)
	return &oracleScraper{
		mb:                   metadata.NewMetricsBuilder(cfg, receivertest.NewNopSettings(metadata.Type)),
		metricsBuilderConfig: cfg,
		logger:               zap.New(core),
		db:                   db,
		instanceInfo:         start,
		clientProviderFunc: func(_ *sql.DB, q string, _ *zap.Logger) dbClient {
			if q == instanceRoleSQL {
				return role
			}
			return &scriptedClient{results: []scriptedResult{{err: errQuery}}}
		},
	}, logs
}

func scrapeAttrs(t *testing.T, s *oracleScraper) map[string]string {
	t.Helper()
	info := s.currentInstanceInfo(t.Context(), s.metricsBuilderConfig.ResourceAttributes)
	res := s.setupResourceBuilder(s.mb.NewResourceBuilder(), info).Emit()
	out := map[string]string{}
	for _, k := range []string{"oracle.db.version", "oracle.db.role", "oracle.db.open_mode"} {
		if v, ok := res.Attributes().Get(k); ok {
			out[k] = v.Str()
		}
	}
	return out
}

var start19 = oracleInstanceInfo{dbVersion: "19.0.0.0.0", databaseRole: "PRIMARY", openMode: "READ WRITE"}

func TestCurrentInstanceInfo_RoleChangeBetweenScrapes(t *testing.T) {
	role := &scriptedClient{results: []scriptedResult{okRow("PRIMARY", "READ WRITE"), okRow("PHYSICAL STANDBY", "MOUNTED")}}
	s, _ := newRefreshScraper(t, metadata.NewDefaultMetricsBuilderConfig(), start19, role)

	assert.Equal(t, map[string]string{"oracle.db.version": "19.0.0.0.0", "oracle.db.role": "PRIMARY", "oracle.db.open_mode": "READ WRITE"}, scrapeAttrs(t, s))
	assert.Equal(t, map[string]string{"oracle.db.version": "19.0.0.0.0", "oracle.db.role": "PHYSICAL STANDBY", "oracle.db.open_mode": "MOUNTED"}, scrapeAttrs(t, s))
	assert.Equal(t, 2, role.calls)
}

func TestCurrentInstanceInfo_FailureOmitsBothAndRecovers(t *testing.T) {
	role := &scriptedClient{results: []scriptedResult{
		okRow("PRIMARY", "READ WRITE"),
		{err: errQuery},
		{err: errQuery},
		okRow("PHYSICAL STANDBY", "MOUNTED"),
		{err: errQuery},
		okRow("PRIMARY", "READ WRITE"),
	}}
	s, logs := newRefreshScraper(t, metadata.NewDefaultMetricsBuilderConfig(), start19, role)

	assert.Equal(t, "PRIMARY", scrapeAttrs(t, s)["oracle.db.role"])
	for range 2 {
		attrs := scrapeAttrs(t, s)
		assert.NotContains(t, attrs, "oracle.db.role")
		assert.NotContains(t, attrs, "oracle.db.open_mode")
		assert.Equal(t, "19.0.0.0.0", attrs["oracle.db.version"])
	}
	// First episode: one warning, the second failure only at debug level.
	assert.Equal(t, 1, logs.FilterLevelExact(zapcore.WarnLevel).Len())
	assert.Equal(t, 1, logs.FilterLevelExact(zapcore.DebugLevel).Len())

	assert.Equal(t, "PHYSICAL STANDBY", scrapeAttrs(t, s)["oracle.db.role"])
	assert.Equal(t, 1, logs.FilterLevelExact(zapcore.InfoLevel).Len())

	// Second episode warns again and recovers again.
	assert.NotContains(t, scrapeAttrs(t, s), "oracle.db.role")
	assert.Equal(t, "PRIMARY", scrapeAttrs(t, s)["oracle.db.role"])
	assert.Equal(t, 2, logs.FilterLevelExact(zapcore.WarnLevel).Len())
	assert.Equal(t, 2, logs.FilterLevelExact(zapcore.InfoLevel).Len())
}

func TestCurrentInstanceInfo_NoRowsOmitsBoth(t *testing.T) {
	s, logs := newRefreshScraper(t, metadata.NewDefaultMetricsBuilderConfig(), start19, &scriptedClient{results: []scriptedResult{{rows: nil}}})

	attrs := scrapeAttrs(t, s)
	assert.NotContains(t, attrs, "oracle.db.role")
	assert.NotContains(t, attrs, "oracle.db.open_mode")
	assert.Equal(t, "19.0.0.0.0", attrs["oracle.db.version"])
	assert.Equal(t, 1, logs.FilterLevelExact(zapcore.WarnLevel).Len())
}

func TestCurrentInstanceInfo_CancelledScrapeDoesNotWarn(t *testing.T) {
	s, logs := newRefreshScraper(t, metadata.NewDefaultMetricsBuilderConfig(), start19, &scriptedClient{results: []scriptedResult{{err: context.Canceled}}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	info := s.currentInstanceInfo(ctx, s.metricsBuilderConfig.ResourceAttributes)
	assert.Empty(t, info.databaseRole)
	assert.Zero(t, logs.FilterLevelExact(zapcore.WarnLevel).Len())
	assert.False(t, s.roleRefreshFailing)
}

func TestCurrentInstanceInfo_DisabledOrOldVersionSkipsQuery(t *testing.T) {
	both := metadata.NewDefaultMetricsBuilderConfig()
	both.ResourceAttributes.OracleDbRole.Enabled = false
	both.ResourceAttributes.OracleDbOpenMode.Enabled = false
	role := &scriptedClient{results: []scriptedResult{okRow("PRIMARY", "READ WRITE")}}
	s, _ := newRefreshScraper(t, both, start19, role)
	assert.Equal(t, map[string]string{"oracle.db.version": "19.0.0.0.0"}, scrapeAttrs(t, s))
	assert.Zero(t, role.calls)

	// One of the two enabled: the query runs.
	one := metadata.NewDefaultMetricsBuilderConfig()
	one.ResourceAttributes.OracleDbRole.Enabled = false
	s, _ = newRefreshScraper(t, one, start19, role)
	assert.Equal(t, "READ WRITE", scrapeAttrs(t, s)["oracle.db.open_mode"])
	assert.Equal(t, 1, role.calls)

	// Pre-12c and a start-time detection that read no version: unchanged, no query.
	role = &scriptedClient{results: []scriptedResult{okRow("PRIMARY", "READ WRITE")}}
	for _, v := range []string{"11.2.0.4.0", ""} {
		s, _ = newRefreshScraper(t, metadata.NewDefaultMetricsBuilderConfig(), oracleInstanceInfo{dbVersion: v}, role)
		scrapeAttrs(t, s)
	}
	assert.Zero(t, role.calls)
}

// TestScrape_ReadsRoleAtEveryScrape runs the metrics scrape twice across a role change.
func TestScrape_ReadsRoleAtEveryScrape(t *testing.T) {
	role := &scriptedClient{results: []scriptedResult{okRow("PRIMARY", "READ WRITE"), okRow("PHYSICAL STANDBY", "MOUNTED")}}
	instance := map[string]*scriptedClient{
		instanceVersionSQL: {results: []scriptedResult{{rows: versionRow("19.0.0.0.0")}}},
		instanceCDBSQL:     {results: []scriptedResult{{rows: cdbRow("NO", "PRIMARY", "READ WRITE")}}},
		instanceRoleSQL:    role,
	}
	db := sql.OpenDB(closedConnector{})
	cfg := metadata.NewDefaultMetricsBuilderConfig()
	scrpr := oracleScraper{
		logger:         zap.NewNop(),
		mb:             metadata.NewMetricsBuilder(cfg, receivertest.NewNopSettings(metadata.Type)),
		dbProviderFunc: func() (*sql.DB, error) { return db, nil },
		clientProviderFunc: func(_ *sql.DB, q string, _ *zap.Logger) dbClient {
			if c, ok := instance[q]; ok {
				return c
			}
			if q == instanceRDSSQL || q == instanceOCISQL || q == instanceOCICDBServicesSQL || q == instanceConTypeSQL || q == instanceConNameSQL {
				return &scriptedClient{results: []scriptedResult{{rows: nil}}}
			}
			return &scriptedClient{results: []scriptedResult{{rows: queryResponses[q]}}}
		},
		metricsBuilderConfig: cfg,
		instanceName:         "oraclehost:1521/ORCL",
		hostName:             "oraclehost:1521",
		serverAddress:        "oraclehost",
		serverPort:           1521,
		serviceInstanceID:    testInstanceID("oraclehost:1521", "oraclehost:1521/ORCL"),
	}
	require.NoError(t, scrpr.start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { assert.NoError(t, scrpr.shutdown(t.Context())) })

	for _, want := range [][2]string{{"PRIMARY", "READ WRITE"}, {"PHYSICAL STANDBY", "MOUNTED"}} {
		before := role.calls
		m, err := scrpr.scrape(t.Context())
		require.NoError(t, err)
		attrs := m.ResourceMetrics().At(0).Resource().Attributes()
		gotRole, _ := attrs.Get("oracle.db.role")
		gotMode, _ := attrs.Get("oracle.db.open_mode")
		assert.Equal(t, want, [2]string{gotRole.Str(), gotMode.Str()})
		version, _ := attrs.Get("oracle.db.version")
		assert.Equal(t, "19.0.0.0.0", version.Str())
		assert.Equal(t, 1, role.calls-before, "one role query per scrape")
	}
}

// TestScrapeLogs_ReadsRoleOncePerScrape runs all four log collectors in one scrapeLogs call, twice, with
// the role changing in between. Every collector's resource must carry the role of its scrape, and the role
// query must run once per scrape, not once per collector.
func TestScrapeLogs_ReadsRoleOncePerScrape(t *testing.T) {
	cdb := &scriptedClient{results: []scriptedResult{
		okRow("PRIMARY", "READ WRITE"),       // scrape 1
		okRow("PHYSICAL STANDBY", "MOUNTED"), // scrape 2
	}}
	scripts := map[string]*scriptedClient{
		instanceVersionSQL: {results: []scriptedResult{{rows: versionRow("19.0.0.0.0")}}},
		instanceCDBSQL:     {results: []scriptedResult{{rows: cdbRow("NO", "PRIMARY", "READ WRITE")}}},
		instanceRoleSQL:    cdb,
	}
	loadRows := func(file string) []metricRow {
		var rows []metricRow
		require.NoError(t, json.Unmarshal(readFile(file), &rows))
		return rows
	}
	// Created once in start() and queried on every scrape: they answer with the same rows each time.
	sessionClient := &scriptedClient{results: []scriptedResult{{rows: cloneRows(sessionEventQueryResponses[sessionEventQuery])}}}
	sampleClient := &scriptedClient{results: []scriptedResult{{rows: cloneRows(samplesQueryResponses[samplesQuery])}}}
	provider := func(_ *sql.DB, q string, _ *zap.Logger) dbClient {
		if c, ok := scripts[q]; ok {
			return c
		}
		switch {
		case q == instanceRDSSQL || q == instanceOCISQL || q == instanceOCICDBServicesSQL || q == instanceConTypeSQL || q == instanceConNameSQL:
			return &fakeDbClient{Responses: [][]metricRow{nil}}
		case q == sessionEventQuery:
			return sessionClient
		case q == samplesQuery:
			return sampleClient
		case q == oracleProcedureMetricsSQL:
			return &fakeDbClient{Responses: [][]metricRow{loadRows("oracleProcedureMetricsData.txt")}}
		case strings.Contains(q, SQLPlanTable):
			return &fakeDbClient{Responses: [][]metricRow{loadRows("oracleQueryPlanData.txt")}}
		case q == oracleQueryMetricsSQL:
			return &fakeDbClient{Responses: [][]metricRow{loadRows("oracleQueryMetricsData.txt")}}
		case strings.Contains(q, "FROM V$SQL") && strings.Contains(q, "SQL_ID IN"):
			return &fakeDbClient{Responses: [][]metricRow{cloneRows(sampleStatsRows)}}
		}
		return &fakeDbClient{Responses: [][]metricRow{nil}}
	}

	logsCfg := metadata.DefaultLogsBuilderConfig()
	logsCfg.Events.DbServerTopQuery.Enabled = true
	logsCfg.Events.DbServerQuerySample.Enabled = true
	logsCfg.Events.DbServerSessionWaitSample.Enabled = true
	logsCfg.Events.DbServerTopProcedure.Enabled = true
	queryCache, err := lru.New[string, map[string]int64](500)
	require.NoError(t, err)
	procCache, err := lru.New[string, map[string]int64](500)
	require.NoError(t, err)
	seed := func() {
		queryCache.Add("fxk8aq3nds8aw:0", cacheValue)
		procCache.Add(procedureFixtureCacheKey, procedureCacheValue)
	}

	db := sql.OpenDB(closedConnector{})
	scrpr := oracleScraper{
		logger:               zap.NewNop(),
		lb:                   metadata.NewLogsBuilder(logsCfg, receivertest.NewNopSettings(metadata.Type)),
		dbProviderFunc:       func() (*sql.DB, error) { return db, nil },
		clientProviderFunc:   provider,
		logsBuilderConfig:    logsCfg,
		metricCache:          queryCache,
		procedureMetricCache: procCache,
		topQueryCollectCfg:   TopQueryCollection{MaxQuerySampleCount: 5000, TopQueryCount: 200},
		procedureMetricsCfg:  ProcedureMetrics{MaxProcedureSampleCount: 1000, TopProcedureCount: 250},
		sessionWaitEventCfg:  SessionWaitEvent{MaxRowsPerQuery: 200},
		instanceName:         "oraclehost:1521/ORCL",
		hostName:             "oraclehost:1521",
		serverAddress:        "oraclehost",
		serverPort:           1521,
		obfuscator:           newObfuscator(),
		serviceInstanceID:    testInstanceID("oraclehost:1521", "oraclehost:1521/ORCL"),
	}
	require.NoError(t, scrpr.start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { assert.NoError(t, scrpr.shutdown(t.Context())) })
	require.Zero(t, cdb.calls, "start() does not use the refresh query")

	// roleByEvent maps every emitted event name to the role/open_mode/version on its resource.
	roleByEvent := func(logs plog.Logs) map[string]string {
		out := map[string]string{}
		for i := range logs.ResourceLogs().Len() {
			rl := logs.ResourceLogs().At(i)
			get := func(k string) string {
				v, _ := rl.Resource().Attributes().Get(k)
				return v.Str()
			}
			desc := get("oracle.db.role") + "|" + get("oracle.db.open_mode") + "|" + get("oracle.db.version")
			for j := range rl.ScopeLogs().Len() {
				for k := range rl.ScopeLogs().At(j).LogRecords().Len() {
					out[rl.ScopeLogs().At(j).LogRecords().At(k).EventName()] = desc
				}
			}
		}
		return out
	}
	events := []string{"db.server.top_query", "db.server.query_sample", "db.server.session.wait_sample", "db.server.top_procedure"}

	for n, want := range []string{"PRIMARY|READ WRITE|19.0.0.0.0", "PHYSICAL STANDBY|MOUNTED|19.0.0.0.0"} {
		seed()
		before := cdb.calls
		logs, err := scrpr.scrapeLogs(t.Context())
		require.NoError(t, err)
		got := roleByEvent(logs)
		for _, e := range events {
			assert.Equal(t, want, got[e], "scrape %d, event %s", n+1, e)
		}
		assert.Equal(t, 1, cdb.calls-before, "scrape %d: one role query for all collectors", n+1)
	}
}

// TestScrapeLogs_RoleQueryOnlyWhenACollectorRuns pins the gate: one collector enabled per row.
func TestScrapeLogs_RoleQueryOnlyWhenACollectorRuns(t *testing.T) {
	notElapsed := time.Now()
	topQuery := func(c *metadata.LogsBuilderConfig) { c.Events.DbServerTopQuery.Enabled = true }
	topProcedure := func(c *metadata.LogsBuilderConfig) { c.Events.DbServerTopProcedure.Enabled = true }
	querySample := func(c *metadata.LogsBuilderConfig) { c.Events.DbServerQuerySample.Enabled = true }
	sessionWait := func(c *metadata.LogsBuilderConfig) { c.Events.DbServerSessionWaitSample.Enabled = true }
	queryRan := func(s *oracleScraper) { s.lastExecutionTimestamp = notElapsed }
	procedureRan := func(s *oracleScraper) { s.lastProcedureMetricsTimestamp = notElapsed }
	tests := []struct {
		name    string
		enable  func(*metadata.LogsBuilderConfig)
		stamp   func(*oracleScraper)
		queries int
	}{
		{"none", func(*metadata.LogsBuilderConfig) {}, nil, 0},
		{"top query due", topQuery, nil, 1},
		{"top query not elapsed", topQuery, queryRan, 0},
		{"query sample", querySample, nil, 1},
		{"session wait", sessionWait, nil, 1},
		{"top procedure due", topProcedure, nil, 1},
		{"top procedure not elapsed", topProcedure, procedureRan, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			role := &scriptedClient{results: []scriptedResult{okRow("PRIMARY", "READ WRITE")}}
			s, _ := newRefreshScraper(t, metadata.NewDefaultMetricsBuilderConfig(), start19, role)
			logsCfg := metadata.DefaultLogsBuilderConfig()
			logsCfg.Events.DbServerTopQuery.Enabled = false
			tc.enable(&logsCfg)
			s.logsBuilderConfig = logsCfg
			s.lb = metadata.NewLogsBuilder(logsCfg, receivertest.NewNopSettings(metadata.Type))
			s.topQueryCollectCfg.CollectionInterval = time.Hour
			s.procedureMetricsCfg.CollectionInterval = time.Hour
			s.obfuscator = newObfuscator()
			failing := &scriptedClient{results: []scriptedResult{{err: errQuery}}}
			s.samplesQueryClient, s.sessionEventClient = failing, failing // set in start() in production
			if tc.stamp != nil {
				tc.stamp(s)
			}

			_, _ = s.scrapeLogs(t.Context()) // the collectors fail on their unscripted queries; only the role query counts
			assert.Equal(t, tc.queries, role.calls)
		})
	}
}

func TestCurrentInstanceInfo_QueryDeadlineCappedAtFiveSeconds(t *testing.T) {
	role := &scriptedClient{results: []scriptedResult{okRow("PRIMARY", "READ WRITE")}}
	s, _ := newRefreshScraper(t, metadata.NewDefaultMetricsBuilderConfig(), start19, role)

	s.currentInstanceInfo(t.Context(), s.metricsBuilderConfig.ResourceAttributes)
	require.False(t, role.deadline.IsZero(), "the role query has a deadline")
	assert.LessOrEqual(t, time.Until(role.deadline), instanceInfoDetectTimeout)
}
