// Command grpc serves the jobs queue over gRPC. It is the service's request-facing entrypoint;
// cmd/migrations applies the database schema.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/a-novel-kit/golib/grpcf"
	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config"
	"github.com/a-novel/service-jobs/internal/config/env"
	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/handlers"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
	"github.com/a-novel/service-jobs/internal/lib"
)

const (
	// jobRetentionDays is how long a settled job is kept before the scheduled purge deletes it. Seven
	// days is the retry window, past which a repeated request is a new request rather than a retry.
	jobRetentionDays = 7
	// jobWatchPollInterval is how often a JobWatch stream re-reads the job it is watching. Nothing in
	// the stack pushes a change, so the stream learns of one by polling; the interval bounds the
	// detection delay a watching caller sees.
	jobWatchPollInterval = time.Second
	// statusQueueCacheTTL is how long the status surface caches the backlog measurement. It bounds how
	// stale the reported depth can be while stopping a polled health probe from running the query on
	// every call.
	statusQueueCacheTTL = time.Second
)

func main() {
	cfg := config.AppPresetDefault
	ctx := context.Background()

	// Validate the reaper cadence before anything else starts. env.ReaperInterval refuses a malformed
	// value rather than falling back the way config.LoadEnv would, so the boot fails loudly here
	// instead of sweeping at a cadence nobody chose. Validated before the first defer, so a fatal here
	// skips no cleanup.
	reaperInterval, err := env.ReaperInterval(env.RawReaperInterval())
	if err != nil {
		log.Fatalf("reaper: %v", err)
	}

	otel.SetAppName(cfg.App.Name)

	lo.Must0(otel.Init(cfg.Otel))
	defer cfg.Otel.Flush()

	if env.GcloudProjectId == "" {
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}

	ctx = lo.Must(postgres.NewContext(ctx, config.PostgresPresetDefault))

	// =================================================================================================================
	// DAO
	// =================================================================================================================

	daoJobEnqueue := dao.NewJobEnqueue()
	daoJobGet := dao.NewJobGet()
	daoJobGetByID := dao.NewJobGetByID()
	daoJobClaim := dao.NewJobClaim()
	daoJobSettle := dao.NewJobSettle()
	daoJobRequeue := dao.NewJobRequeue()
	daoJobReap := dao.NewJobReap()
	daoJobQueueDepth := dao.NewJobQueueDepth()

	// =================================================================================================================
	// SERVICES
	// =================================================================================================================

	// The transactor makes the settle service's read-then-requeue-or-give-up one unit of work. It is
	// the only operation in the service that writes conditionally on what it read.
	transactor := postgres.NewTransactor(nil)

	serviceJobEnqueue := core.NewJobEnqueue(daoJobEnqueue)
	serviceJobGet := core.NewJobGet(daoJobGet)
	serviceJobClaim := core.NewJobClaim(daoJobClaim)
	serviceJobSettle := core.NewJobSettle(daoJobSettle, daoJobRequeue, daoJobGetByID, transactor, jobRetentionDays)
	serviceJobReap := core.NewJobReap(daoJobReap, jobRetentionDays)
	serviceJobQueueDepth := core.NewJobQueueDepth(daoJobQueueDepth)

	// =================================================================================================================
	// REAPER
	// =================================================================================================================

	reaper := lib.NewReaper(serviceJobReap, reaperInterval)
	log.Printf("reaper: sweeping every %s, abandoning at the %d-day retention horizon",
		reaperInterval, jobRetentionDays)

	// =================================================================================================================
	// HANDLERS
	// =================================================================================================================

	handlerStatus := handlers.NewGrpcStatus(serviceJobQueueDepth, statusQueueCacheTTL)
	handlerJobEnqueue := handlers.NewJobEnqueue(serviceJobEnqueue)
	handlerJobGet := handlers.NewJobGet(serviceJobGet)
	handlerJobClaim := handlers.NewJobClaim(serviceJobClaim)
	handlerJobSettle := handlers.NewJobSettle(serviceJobSettle)
	// Watch polls the owner-scoped read and streams each change, so it takes the same get service.
	handlerJobWatch := handlers.NewJobWatch(serviceJobGet, jobWatchPollInterval)

	// =================================================================================================================
	// SERVER
	// =================================================================================================================

	ctxInterceptor := func(rpCtx context.Context) context.Context {
		return postgres.TransferContext(ctx, rpCtx)
	}

	listenerConfig := new(net.ListenConfig)
	listener := lo.Must(listenerConfig.Listen(ctx, "tcp", fmt.Sprintf("0.0.0.0:%d", cfg.Grpc.Port)))
	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		cfg.Otel.RpcInterceptor(),
		grpc.ChainUnaryInterceptor(
			grpcf.BaseContextUnaryInterceptor(ctxInterceptor),
			cfg.Logger.UnaryInterceptor(),
			cfg.Logger.PanicUnaryInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			grpcf.BaseContextStreamInterceptor(ctxInterceptor),
			cfg.Logger.StreamInterceptor(),
			cfg.Logger.PanicStreamInterceptor(),
		),
	)

	grpcf.SetEchoServersContext(ctx, server, cfg.Grpc.Ping)

	protogen.RegisterStatusServiceServer(server, handlerStatus)
	protogen.RegisterJobEnqueueServiceServer(server, handlerJobEnqueue)
	protogen.RegisterJobGetServiceServer(server, handlerJobGet)
	protogen.RegisterJobClaimServiceServer(server, handlerJobClaim)
	protogen.RegisterJobSettleServiceServer(server, handlerJobSettle)
	protogen.RegisterJobWatchServiceServer(server, handlerJobWatch)

	reflection.Register(server)

	// =================================================================================================================
	// RUN
	// =================================================================================================================

	// Run the reaper alongside the server, on a context cancelled at shutdown. reaperDone lets the
	// shutdown wait for an in-flight sweep to finish before the process exits.
	reaperCtx, stopReaper := context.WithCancel(ctx)

	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)

		reaper.Run(reaperCtx)
	}()

	log.Println("Starting gRPC server on :" + strconv.Itoa(cfg.Grpc.Port))

	go func() {
		err := server.Serve(listener)
		if err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gRPC server...")
	stopReaper()
	<-reaperDone
	server.GracefulStop()
}
