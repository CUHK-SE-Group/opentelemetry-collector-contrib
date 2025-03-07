// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

// NewFactory creates a factory for ClickHouse exporter.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xexporter.WithLogs(createLogsExporter, metadata.LogsStability),
		xexporter.WithTraces(createTracesExporter, metadata.TracesStability),
		xexporter.WithMetrics(createMetricExporter, metadata.MetricsStability),
		xexporter.WithProfiles(createProfilesExporter, metadata.ProfilesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		collectorVersion: "unknown",

		TimeoutSettings:  exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:    exporterhelper.NewDefaultQueueConfig(),
		BackOffConfig:    configretry.NewDefaultBackOffConfig(),
		ConnectionParams: map[string]string{},
		Database:         defaultDatabase,
		LogsTableName:    "otel_logs",
		TracesTableName:  "otel_traces",
		TTL:              0,
		CreateSchema:     true,
		AsyncInsert:      true,
		MetricsTables: MetricTablesConfig{
			Gauge:                internal.MetricTypeConfig{Name: defaultMetricTableName + defaultGaugeSuffix},
			Sum:                  internal.MetricTypeConfig{Name: defaultMetricTableName + defaultSumSuffix},
			Summary:              internal.MetricTypeConfig{Name: defaultMetricTableName + defaultSummarySuffix},
			Histogram:            internal.MetricTypeConfig{Name: defaultMetricTableName + defaultHistogramSuffix},
			ExponentialHistogram: internal.MetricTypeConfig{Name: defaultMetricTableName + defaultExpHistogramSuffix},
		},
		ProfilesTables: ProfilesTablesConfig{
			Profiles:  defaultProfilesTableName,
			Samples:   defaultProfilesTableName + "_samples",
			Locations: defaultProfilesTableName + "_locations",
			Functions: defaultProfilesTableName + "_functions",
			Mappings:  defaultProfilesTableName + "_mappings",
		},
	}
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly inserted into ClickHouse.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)
	c.collectorVersion = set.BuildInfo.Version
	exporter, err := newLogsExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse logs exporter: %w", err)
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exporter.pushLogsData,
		exporterhelper.WithStart(exporter.start),
		exporterhelper.WithShutdown(exporter.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly inserted into ClickHouse.
func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	c.collectorVersion = set.BuildInfo.Version
	exporter, err := newTracesExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse traces exporter: %w", err)
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exporter.pushTraceData,
		exporterhelper.WithStart(exporter.start),
		exporterhelper.WithShutdown(exporter.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createMetricExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	c.collectorVersion = set.BuildInfo.Version
	exporter, err := newMetricsExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse metrics exporter: %w", err)
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exporter.pushMetricsData,
		exporterhelper.WithStart(exporter.start),
		exporterhelper.WithShutdown(exporter.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createProfilesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (xexporter.Profiles, error) {
	c := cfg.(*Config)
	c.collectorVersion = set.BuildInfo.Version
	exporter, err := newProfilesExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse profiles exporter: %w", err)
	}

	return xexporterhelper.NewProfilesExporter(
		ctx,
		set,
		cfg,
		exporter.pushProfileData,
		exporterhelper.WithStart(exporter.start),
		exporterhelper.WithShutdown(exporter.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func generateTTLExpr(ttl time.Duration, timeField string) string {
	if ttl > 0 {
		switch {
		case ttl%(24*time.Hour) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalDay(%d)`, timeField, ttl/(24*time.Hour))
		case ttl%(time.Hour) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalHour(%d)`, timeField, ttl/time.Hour)
		case ttl%(time.Minute) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalMinute(%d)`, timeField, ttl/time.Minute)
		default:
			return fmt.Sprintf(`TTL %s + toIntervalSecond(%d)`, timeField, ttl/time.Second)
		}
	}
	return ""
}
