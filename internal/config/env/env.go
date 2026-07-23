// Package env reads and parses the service's configuration from environment
// variables, exposing each setting as a typed, ready-to-use value.
package env

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/a-novel-kit/golib/config"
)

// errReaperIntervalNonPositive is returned when REAPER_INTERVAL parses but is zero or negative — a
// value that is syntactically a duration yet cannot be a sweep cadence.
var errReaperIntervalNonPositive = errors.New("REAPER_INTERVAL must be positive")

// prefix is prepended to every configuration variable name that this package reads.
// Set SERVICE_JOBS_ENV_PREFIX when embedding the service in another project,
// where unprefixed variable names could collide with the host project's own.
var prefix = os.Getenv("SERVICE_JOBS_ENV_PREFIX")

func getEnv(name string) string {
	return os.Getenv(prefix + name)
}

// Default values applied when an environment variable is unset.
const (
	AppNameDefault = "service-jobs"

	GrpcPortDefault = 8080
	GrpcDefaultPing = time.Second * 5

	// ReaperIntervalDefault is the sweep cadence used when REAPER_INTERVAL is unset. It is short
	// relative to a typical lease, so a job a dead worker stranded is recovered within one interval
	// of its lease lapsing, while an idle queue costs only an index probe per sweep.
	ReaperIntervalDefault = time.Second * 30

	// PostgresMaxOpenConnsDefault keeps the pool well under a stock PostgreSQL
	// max_connections of 100 once multiplied by a service's replica count, leaving
	// room for the migration job and a psql session. Go's own default is unlimited,
	// which turns a spike into connection refusals for everything on that database
	// rather than queueing inside this process.
	PostgresMaxOpenConnsDefault = 20
	// PostgresMaxIdleConnsDefault matches the open limit so a burst does not close
	// connections it is about to reopen.
	PostgresMaxIdleConnsDefault = 20
)

// Raw values for environment variables.
var (
	postgresDsn          = getEnv("POSTGRES_DSN")
	postgresMaxOpenConns = getEnv("POSTGRES_MAX_OPEN_CONNS")
	postgresMaxIdleConns = getEnv("POSTGRES_MAX_IDLE_CONNS")

	appName = getEnv("APP_NAME")
	otel    = getEnv("OTEL")

	grpcPort = getEnv("GRPC_PORT")
	grpcUrl  = getEnv("GRPC_URL")
	grpcPing = getEnv("GRPC_PING")

	reaperInterval = getEnv("REAPER_INTERVAL")

	gcloudProjectId = getEnv("GCLOUD_PROJECT_ID")
)

var (
	// PostgresDsn is the URL used to connect to the PostgreSQL database instance.
	// Typically formatted as:
	//	postgres://<user>:<password>@<host>:<port>/<database>
	PostgresDsn = postgresDsn

	// PostgresMaxOpenConns is the maximum number of open connections to the database.
	PostgresMaxOpenConns = config.LoadEnv(postgresMaxOpenConns, PostgresMaxOpenConnsDefault, config.IntParser)
	// PostgresMaxIdleConns is the maximum number of connections kept open while idle.
	PostgresMaxIdleConns = config.LoadEnv(postgresMaxIdleConns, PostgresMaxIdleConnsDefault, config.IntParser)

	// AppName is the name of the application, as it appears in logs and tracing.
	AppName = config.LoadEnv(appName, AppNameDefault, config.StringParser)
	// Otel enables OpenTelemetry instrumentation.
	//
	// See: https://opentelemetry.io/
	Otel = config.LoadEnv(otel, false, config.BoolParser)

	// GrpcPort is the port on which the gRPC server listens for incoming requests.
	GrpcPort = config.LoadEnv(grpcPort, GrpcPortDefault, config.IntParser)
	// GrpcUrl is the URL of the gRPC service, typically <host>:<port>.
	GrpcUrl = grpcUrl
	// GrpcPing is the refresh interval for the gRPC server's internal health check.
	GrpcPing = config.LoadEnv(grpcPing, GrpcDefaultPing, config.DurationParser)

	// GcloudProjectId names the Google Cloud project the service runs in. Setting
	// it switches logging and tracing from the local console to Google Cloud.
	//
	// See: https://docs.cloud.google.com/resource-manager/docs/creating-managing-projects
	GcloudProjectId = gcloudProjectId
)

// RawReaperInterval is the unparsed REAPER_INTERVAL value. The boot parses it through
// [ReaperInterval] rather than config.LoadEnv, which would return the default on a parse error with
// no log and no failure — so an interval written without its unit ("30" instead of "30s") would boot
// at the default cadence silently.
func RawReaperInterval() string { return reaperInterval }

// ReaperInterval parses raw — a REAPER_INTERVAL value — into the reaper's sweep cadence. An empty
// string yields [ReaperIntervalDefault]. A malformed or non-positive value is an error, so the boot
// can refuse to start rather than sweep at a cadence nobody chose.
func ReaperInterval(raw string) (time.Duration, error) {
	if raw == "" {
		return ReaperIntervalDefault, nil
	}

	interval, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse REAPER_INTERVAL %q: %w", raw, err)
	}

	if interval <= 0 {
		return 0, fmt.Errorf("%w, got %q", errReaperIntervalNonPositive, raw)
	}

	return interval, nil
}
