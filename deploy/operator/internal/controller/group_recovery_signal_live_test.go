//go:build grouprecoverysignal

// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	configv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/config/v1alpha1"
	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	nvidiacomv1beta2 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta2"
	commonController "github.com/ai-dynamo/dynamo/deploy/operator/internal/controller_common"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/features"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/namespace_scope"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/podcache"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/secrets"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	crtcachen "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const (
	signalRequiredContext           = "kind-dynamo-17951-signal"
	signalRequiredBaselineNamespace = "recovery-signal-baseline"
	signalRequiredCandidateNamespace = "recovery-signal-candidate"

	signalTargetDCDName  = "recovery-dcd-target"
	signalControlDCDName = "recovery-dcd-control"

	signalModeBaseline  = "baseline"
	signalModeCandidate = "candidate"

	signalProjectionProduction = "production_podcache"
	signalProjectionCandidate  = "TEST_ONLY_CANDIDATE"

	signalLWSNameIndex = ".metadata.lwsName.testonly"

	signalMaxJSONLRecords   = 100000
	signalMaxJSONLBytes     = 64 << 20
	signalMaxDedupeEntries  = 10000
	signalClockSkew         = 5 * time.Second
	signalRequestTimeout    = 10 * time.Second
	signalStartupTimeout    = 120 * time.Second
	signalShutdownTimeout   = 60 * time.Second
	signalCacheTapBuffer    = 4096
	signalIntendedFaultExit = int32(23)

	signalLWSAPIVersion = "leaderworkerset.x-k8s.io/v1"
)

// TestGroupRecoverySignalLive measures real DCD manager cache/reconcile consumption of
// LWS group-recovery failure signals against an already-admitted CPU fixture.
func TestGroupRecoverySignalLive(t *testing.T) {
	t.Log("Validate explicit kube context and task namespaces before any mutation")
	env, err := loadSignalEnv()
	if err != nil {
		t.Fatalf("environment: %v", err)
	}
	restConfig, err := loadSignalRESTConfig(env)
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}
	scheme := newSignalScheme()
	apiClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("api client: %v", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("kubernetes client: %v", err)
	}

	t.Log("Create exclusive trace directory and JSONL outputs")
	if err := os.Mkdir(env.traceDir, 0o700); err != nil {
		t.Fatalf("exclusive SIGNAL_TRACE_DIR: %v", err)
	}
	baselineTrace, err := newSignalTrace(filepath.Join(env.traceDir, "baseline.jsonl"), signalModeBaseline, signalProjectionProduction)
	if err != nil {
		t.Fatalf("baseline.jsonl: %v", err)
	}
	candidateTrace, err := newSignalTrace(filepath.Join(env.traceDir, "candidate.jsonl"), signalModeCandidate, signalProjectionCandidate)
	if err != nil {
		t.Fatalf("candidate.jsonl: %v", err)
	}
	var baselineCloseErr, candidateCloseErr error
	defer func() {
		if baselineCloseErr == nil {
			baselineCloseErr = baselineTrace.Close()
		}
		if candidateCloseErr == nil {
			candidateCloseErr = candidateTrace.Close()
		}
	}()

	t.Log("Preflight root-admitted DCD identities and absent namespace-scope leases")
	baselineRoots, err := signalAPIPreflight(t.Context(), apiClient, kubeClient, env.baselineNamespace, baselineTrace)
	if err != nil {
		t.Fatalf("baseline preflight: %v", err)
	}
	candidateRoots, err := signalAPIPreflight(t.Context(), apiClient, kubeClient, env.candidateNamespace, candidateTrace)
	if err != nil {
		t.Fatalf("candidate preflight: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	t.Log("Start baseline and candidate namespace-restricted managers with real leases")
	baselineScope, err := startSignalScope(runCtx, restConfig, scheme, env.baselineNamespace, signalModeBaseline, signalProjectionProduction, baselineTrace, baselineRoots, false)
	if err != nil {
		t.Fatalf("baseline scope: %v", err)
	}
	defer func() {
		for _, stopErr := range baselineScope.stop() {
			t.Errorf("baseline scope cleanup: %v", stopErr)
		}
	}()

	candidateScope, err := startSignalScope(runCtx, restConfig, scheme, env.candidateNamespace, signalModeCandidate, signalProjectionCandidate, candidateTrace, candidateRoots, true)
	if err != nil {
		t.Fatalf("candidate scope: %v", err)
	}
	defer func() {
		for _, stopErr := range candidateScope.stop() {
			t.Errorf("candidate scope cleanup: %v", stopErr)
		}
	}()

	t.Log("Observe for the configured duration while lease/manager/trace errors remain fatal")
	deadline := time.After(env.duration)
	var runErr error
	select {
	case <-deadline:
	case err := <-baselineScope.fatal:
		runErr = fmt.Errorf("baseline fatal: %w", err)
	case err := <-candidateScope.fatal:
		runErr = fmt.Errorf("candidate fatal: %w", err)
	case err := <-baselineTrace.errCh:
		runErr = fmt.Errorf("baseline trace: %w", err)
	case err := <-candidateTrace.errCh:
		runErr = fmt.Errorf("candidate trace: %w", err)
	}
	runCancel()

	t.Log("Stop managers then release leases with a fresh bounded cleanup context")
	shutdownErrs := make([]error, 0, 8)
	shutdownErrs = append(shutdownErrs, baselineScope.stop()...)
	shutdownErrs = append(shutdownErrs, candidateScope.stop()...)
	baselineCloseErr = baselineTrace.Close()
	candidateCloseErr = candidateTrace.Close()
	if baselineCloseErr != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("baseline trace close: %w", baselineCloseErr))
	}
	if candidateCloseErr != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("candidate trace close: %w", candidateCloseErr))
	}

drainBaselineFatal:
	for {
		select {
		case err := <-baselineScope.fatal:
			runErr = errors.Join(runErr, fmt.Errorf("baseline fatal: %w", err))
		default:
			break drainBaselineFatal
		}
	}
drainCandidateFatal:
	for {
		select {
		case err := <-candidateScope.fatal:
			runErr = errors.Join(runErr, fmt.Errorf("candidate fatal: %w", err))
		default:
			break drainCandidateFatal
		}
	}

	harnessOK := runErr == nil &&
		!baselineTrace.failed.Load() &&
		!candidateTrace.failed.Load() &&
		len(shutdownErrs) == 0
	summary := signalSummary{
		HarnessSuccess:           harnessOK,
		ObservationDurationSec:   int(env.duration.Seconds()),
		Baseline:                 baselineTrace.summarySnapshot(baselineScope),
		Candidate:                candidateTrace.summarySnapshot(candidateScope),
		ShutdownErrors:           errorStrings(shutdownErrs),
		ProcessLocalLowerBound:    true,
		NoDurableLedger:          true,
		ScenarioPassIsExternal:   true,
		Limitation:               "healthy Ready.LastTransitionTime may be recorded; full reset/dwell/TTL ledger is not implemented; only main current exit23 is experimental eligible evidence",
		MeasurementCompletedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		RunError:                 errString(runErr),
	}
	if err := writeSignalSummary(filepath.Join(env.traceDir, "summary.json"), summary); err != nil {
		t.Fatalf("summary.json: %v", err)
	}
	if runErr != nil {
		t.Fatalf("measurement failed: %v", runErr)
	}
	for _, err := range shutdownErrs {
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}
	if baselineTrace.failed.Load() || candidateTrace.failed.Load() {
		t.Fatalf("trace overflow or IO failure")
	}
}

type signalEnv struct {
	kubeconfig         string
	contextName        string
	baselineNamespace  string
	candidateNamespace string
	traceDir           string
	duration           time.Duration
}

func loadSignalEnv() (signalEnv, error) {
	env := signalEnv{
		kubeconfig:         strings.TrimSpace(os.Getenv("KUBECONFIG")),
		contextName:        strings.TrimSpace(os.Getenv("DYNAMO_CLUSTERTEST_CONTEXT")),
		baselineNamespace:  strings.TrimSpace(os.Getenv("SIGNAL_BASELINE_NAMESPACE")),
		candidateNamespace: strings.TrimSpace(os.Getenv("SIGNAL_CANDIDATE_NAMESPACE")),
		traceDir:           strings.TrimSpace(os.Getenv("SIGNAL_TRACE_DIR")),
	}
	if env.kubeconfig == "" {
		return env, errors.New("KUBECONFIG is required")
	}
	if env.contextName != signalRequiredContext {
		return env, fmt.Errorf("DYNAMO_CLUSTERTEST_CONTEXT must be %q", signalRequiredContext)
	}
	if env.baselineNamespace != signalRequiredBaselineNamespace {
		return env, fmt.Errorf("SIGNAL_BASELINE_NAMESPACE must be %q", signalRequiredBaselineNamespace)
	}
	if env.candidateNamespace != signalRequiredCandidateNamespace {
		return env, fmt.Errorf("SIGNAL_CANDIDATE_NAMESPACE must be %q", signalRequiredCandidateNamespace)
	}
	if env.traceDir == "" || !filepath.IsAbs(env.traceDir) {
		return env, errors.New("SIGNAL_TRACE_DIR must be an absolute fresh path")
	}
	seconds := 900
	if raw := strings.TrimSpace(os.Getenv("SIGNAL_DURATION_SECONDS")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return env, fmt.Errorf("SIGNAL_DURATION_SECONDS: %w", err)
		}
		seconds = parsed
	}
	if seconds < 30 || seconds > 1800 {
		return env, fmt.Errorf("SIGNAL_DURATION_SECONDS must be in 30..1800, got %d", seconds)
	}
	env.duration = time.Duration(seconds) * time.Second
	return env, nil
}

func loadSignalRESTConfig(env signalEnv) (*rest.Config, error) {
	loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: env.kubeconfig}
	raw, err := loadingRules.Load()
	if err != nil {
		return nil, err
	}
	if _, ok := raw.Contexts[env.contextName]; !ok {
		return nil, fmt.Errorf("DYNAMO_CLUSTERTEST_CONTEXT %q is missing from kubeconfig", env.contextName)
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: env.contextName}
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, err
	}
	if err := requireLoopbackHTTPSHost(restConfig.Host); err != nil {
		return nil, err
	}
	restConfig.Timeout = signalRequestTimeout
	return restConfig, nil
}

func requireLoopbackHTTPSHost(host string) error {
	u, err := url.Parse(host)
	if err != nil {
		return fmt.Errorf("parse restConfig.Host: %w", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("restConfig.Host scheme must be https, got %q", u.Scheme)
	}
	hostname := u.Hostname()
	ip := net.ParseIP(hostname)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("restConfig.Host must be loopback IP, got %q", hostname)
	}
	if u.Port() == "" {
		return errors.New("restConfig.Host must include an explicit nonzero port")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port <= 0 {
		return fmt.Errorf("restConfig.Host port invalid: %q", u.Port())
	}
	return nil
}

func newSignalScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nvidiacomv1alpha1.AddToScheme(scheme))
	utilruntime.Must(nvidiacomv1beta1.AddToScheme(scheme))
	utilruntime.Must(nvidiacomv1beta2.AddToScheme(scheme))
	utilruntime.Must(leaderworkersetv1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(networkingv1.AddToScheme(scheme))
	utilruntime.Must(autoscalingv2.AddToScheme(scheme))
	utilruntime.Must(coordinationv1.AddToScheme(scheme))
	return scheme
}

type signalRootIdentity struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
	LWSName    string `json:"lwsName"`
	LWSUID     string `json:"lwsUID"`
	LWSGen     int64  `json:"lwsGeneration"`
}

type signalRootSet struct {
	Target  signalRootIdentity `json:"target"`
	Control signalRootIdentity `json:"control"`
}

func signalAPIPreflight(ctx context.Context, apiClient client.Client, kubeClient kubernetes.Interface, namespace string, trace *signalTrace) (signalRootSet, error) {
	ctx, cancel := context.WithTimeout(ctx, signalRequestTimeout)
	defer cancel()

	lease, err := kubeClient.CoordinationV1().Leases(namespace).Get(ctx, namespace_scope.LeaseName, metav1.GetOptions{})
	if err == nil {
		return signalRootSet{}, fmt.Errorf("namespace %s already has lease %s uid=%s", namespace, lease.Name, lease.UID)
	}
	if !apierrors.IsNotFound(err) {
		return signalRootSet{}, err
	}

	roots := signalRootSet{}
	for _, name := range []string{signalTargetDCDName, signalControlDCDName} {
		identity, err := readSignalRootIdentity(ctx, apiClient, namespace, name)
		if err != nil {
			return signalRootSet{}, err
		}
		if name == signalTargetDCDName {
			roots.Target = identity
		} else {
			roots.Control = identity
		}
		trace.record(map[string]any{
			"layer":       "api_preflight",
			"read_origin": "api_preflight",
			"namespace":   namespace,
			"dcd":         identity,
		})
	}
	return roots, nil
}

func readSignalRootIdentity(ctx context.Context, reader client.Reader, namespace, name string) (signalRootIdentity, error) {
	dcd := &nvidiacomv1beta1.DynamoComponentDeployment{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, dcd); err != nil {
		return signalRootIdentity{}, err
	}
	lwsName := leaderWorkerSetName(dcd)
	lws := &leaderworkersetv1.LeaderWorkerSet{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: lwsName}, lws); err != nil {
		return signalRootIdentity{}, err
	}
	if !controllerOwnedBy(lws, "LeaderWorkerSet", []string{signalLWSAPIVersion}, dcd, "DynamoComponentDeployment", []string{
		nvidiacomv1beta1.GroupVersion.String(),
		nvidiacomv1alpha1.GroupVersion.String(),
	}) {
		return signalRootIdentity{}, fmt.Errorf("lws %s controller owner is not dcd %s", lwsName, name)
	}
	return signalRootIdentity{
		Name:       dcd.Name,
		UID:        string(dcd.UID),
		Generation: dcd.Generation,
		LWSName:    lws.Name,
		LWSUID:     string(lws.UID),
		LWSGen:     lws.Generation,
	}, nil
}

func confirmSignalRoots(ctx context.Context, reader client.Reader, namespace string, want signalRootSet) error {
	for _, identity := range []signalRootIdentity{want.Target, want.Control} {
		got, err := readSignalRootIdentity(ctx, reader, namespace, identity.Name)
		if err != nil {
			return err
		}
		if got.UID != identity.UID || got.Generation != identity.Generation || got.LWSUID != identity.LWSUID || got.LWSGen != identity.LWSGen {
			return fmt.Errorf("root identity drift for %s: got=%+v want=%+v", identity.Name, got, identity)
		}
	}
	return nil
}

type signalCacheTap struct {
	mu        sync.Mutex
	accepting bool
	closed    bool
	reg       toolscache.ResourceEventHandlerRegistration
	inf       crtcachen.Informer
	events    chan map[string]any
	done      chan struct{}
	started   bool
}

type signalScope struct {
	mode       string
	namespace  string
	projection string
	leaseName  string
	leaseUID   string
	roots      signalRootSet
	cancel     context.CancelFunc
	done       chan error
	mgrStarted bool
	lease      *namespace_scope.LeaseManager
	leaseOwned bool
	tap        *signalCacheTap
	fatal      chan error
	trace      *signalTrace
	counts     *signalProdReadCounts
	dedupe     *signalDedupe
	stopped    atomic.Bool
}

func startSignalScope(
	parent context.Context,
	restConfig *rest.Config,
	scheme *runtime.Scheme,
	namespace, mode, projection string,
	trace *signalTrace,
	roots signalRootSet,
	candidate bool,
) (scope *signalScope, err error) {
	cfg := &configv1alpha1.OperatorConfiguration{}
	configv1alpha1.SetDefaultsOperatorConfiguration(cfg)
	cfg.Namespace.Restricted = namespace
	discoveryFalse := false
	cfg.GPU.DiscoveryEnabled = &discoveryFalse

	skipNameValidation := true
	mgrOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Controller:             controllerconfig.Controller{SkipNameValidation: &skipNameValidation},
		Cache: crtcachen.Options{
			DefaultNamespaces: map[string]crtcachen.Config{
				namespace: {},
			},
		},
	}
	if candidate {
		mgrOpts.Cache.ByObject = map[client.Object]crtcachen.ByObject{
			&corev1.Pod{}: {Transform: candidateSignalTransform},
		}
	} else if err := podcache.Configure(&mgrOpts.Cache); err != nil {
		return nil, err
	}

	mgr, err := ctrl.NewManager(restConfig, mgrOpts)
	if err != nil {
		return nil, err
	}

	counts := &signalProdReadCounts{}
	dedupe := newSignalDedupe()
	baseClient := mgr.GetClient()
	counting := &signalCountingClient{Client: baseClient, trace: trace, mode: mode, projection: projection, counts: counts}
	dockerSecrets := secrets.NewDockerSecretIndexer(mgr.GetAPIReader(), namespace)
	refreshCtx, refreshCancel := context.WithTimeout(parent, signalRequestTimeout)
	refreshErr := dockerSecrets.RefreshIndex(refreshCtx)
	refreshCancel()
	if refreshErr != nil {
		return nil, fmt.Errorf("docker secret index refresh: %w", refreshErr)
	}

	runtimeConfig := &commonController.RuntimeConfig{Gate: features.Gates{LWS: true}}
	reconciler := &DynamoComponentDeploymentReconciler{
		Client:                counting,
		Recorder:              mgr.GetEventRecorder("dynamocomponentdeployment"),
		Config:                cfg,
		RuntimeConfig:         runtimeConfig,
		DockerSecretRetriever: dockerSecrets,
	}
	ctrlBuilder, err := reconciler.controllerBuilder(mgr)
	if err != nil {
		return nil, err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, signalLWSNameIndex, signalLWSNameIndexValues); err != nil {
		return nil, fmt.Errorf("register test-only LWS name index: %w", err)
	}

	instrumented := &signalReconcileWrapper{
		next:        reconciler,
		cacheReader: baseClient,
		trace:       trace,
		mode:        mode,
		projection:  projection,
		namespace:   namespace,
		dedupe:      dedupe,
	}
	if candidate {
		ctrlBuilder = ctrlBuilder.Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return mapCandidatePodToDCD(ctx, baseClient, obj)
			}),
			builder.WithPredicates(candidateSignificantPredicate()),
		)
	}
	if err := ctrlBuilder.Complete(instrumented); err != nil {
		return nil, err
	}

	leaseManager, err := namespace_scope.NewLeaseManager(
		restConfig,
		namespace,
		"group-recovery-signal-harness",
		cfg.Namespace.Scope.LeaseDuration.Duration,
		cfg.Namespace.Scope.LeaseRenewInterval.Duration,
	)
	if err != nil {
		return nil, err
	}

	scopeCtx, cancel := context.WithCancel(parent)
	fatal := make(chan error, 8)
	done := make(chan error, 1)
	scope = &signalScope{
		mode:       mode,
		namespace:  namespace,
		projection: projection,
		leaseName:  namespace_scope.LeaseName,
		roots:      roots,
		cancel:     cancel,
		done:       done,
		lease:      leaseManager,
		fatal:      fatal,
		trace:      trace,
		counts:     counts,
		dedupe:     dedupe,
	}
	transferred := false
	ownedScope := scope
	defer func() {
		if !transferred {
			err = errors.Join(err, errors.Join(ownedScope.abortStart()...))
		}
	}()

	if err = leaseManager.Start(scopeCtx); err != nil {
		cancel()
		return nil, err
	}
	scope.leaseOwned = true
	go func() {
		select {
		case leaseErr := <-leaseManager.Errors():
			if leaseErr != nil {
				reportSignalFatal(fatal, leaseErr)
			}
		case <-scopeCtx.Done():
		}
	}()

	leaseUID, err := readLeaseUID(scopeCtx, restConfig, namespace)
	if err != nil {
		return nil, err
	}
	scope.leaseUID = leaseUID

	go func() {
		startErr := mgr.Start(scopeCtx)
		select {
		case done <- startErr:
		default:
		}
		if scopeCtx.Err() == nil {
			if startErr == nil {
				startErr = errors.New("manager exited unexpectedly with nil error before cancellation")
			}
			reportSignalFatal(fatal, startErr)
		}
	}()
	scope.mgrStarted = true

	startupCtx, startupCancel := context.WithTimeout(scopeCtx, signalStartupTimeout)
	defer startupCancel()
	if err = waitSignalCacheReady(startupCtx, mgr, fatal); err != nil {
		return nil, err
	}
	for _, obj := range []client.Object{
		&corev1.Pod{},
		&appsv1.StatefulSet{},
		&leaderworkersetv1.LeaderWorkerSet{},
		&nvidiacomv1beta1.DynamoComponentDeployment{},
	} {
		if _, err = mgr.GetCache().GetInformer(startupCtx, obj); err != nil {
			return nil, fmt.Errorf("start informer for %T: %w", obj, err)
		}
	}
	if err = waitSignalCacheReady(startupCtx, mgr, fatal); err != nil {
		return nil, err
	}
	if err = confirmSignalRoots(startupCtx, baseClient, namespace, roots); err != nil {
		return nil, err
	}

	tap, err := attachSignalCacheTap(scopeCtx, mgr, trace, mode, projection, fatal)
	if err != nil {
		return nil, err
	}
	scope.tap = tap

	trace.record(map[string]any{
		"layer":          "scope_ready",
		"namespace":      namespace,
		"mode":           mode,
		"projection":     projection,
		"test_only":      candidate,
		"leaseName":      scope.leaseName,
		"leaseUID":       scope.leaseUID,
		"rootIdentities": roots,
		"cache_synced":   true,
		"read_origin":    "manager_cache",
	})
	transferred = true
	return scope, nil
}

func waitSignalCacheReady(ctx context.Context, mgr ctrl.Manager, fatal <-chan error) error {
	syncDone := make(chan bool, 1)
	go func() {
		syncDone <- mgr.GetCache().WaitForCacheSync(ctx)
	}()
	select {
	case ok := <-syncDone:
		if !ok {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("manager cache sync: %w", err)
			}
			return errors.New("manager cache sync returned false")
		}
		return nil
	case err := <-fatal:
		return fmt.Errorf("manager failed during cache sync: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("manager cache sync: %w", ctx.Err())
	}
}

func reportSignalFatal(fatal chan error, err error) {
	if err == nil {
		return
	}
	select {
	case fatal <- err:
	default:
	}
}

func (s *signalScope) abortStart() []error {
	return s.stop()
}

func (s *signalScope) stop() []error {
	if s == nil || !s.stopped.CompareAndSwap(false, true) {
		return nil
	}
	var errs []error
	if s.cancel != nil {
		s.cancel()
	}
	if s.mgrStarted {
		select {
		case err := <-s.done:
			if err != nil && !errors.Is(err, context.Canceled) {
				errs = append(errs, err)
			}
		case <-time.After(signalShutdownTimeout):
			errs = append(errs, fmt.Errorf("%s manager shutdown timed out", s.mode))
		}
	}
	if s.tap != nil {
		errs = append(errs, s.tap.stop()...)
	}
	if s.leaseOwned && s.lease != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), signalShutdownTimeout)
		if err := s.lease.Stop(cleanupCtx); err != nil {
			errs = append(errs, err)
		}
		cancel()
	}
	return errs
}

func readLeaseUID(ctx context.Context, restConfig *rest.Config, namespace string) (string, error) {
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, signalRequestTimeout)
	defer cancel()
	lease, err := kubeClient.CoordinationV1().Leases(namespace).Get(ctx, namespace_scope.LeaseName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return string(lease.UID), nil
}

func signalLWSNameIndexValues(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil
	}
	name := pod.Labels[leaderworkersetv1.SetNameLabelKey]
	if name == "" {
		return nil
	}
	return []string{name}
}

type containerTimeSnapshot struct {
	currentStarted  metav1.Time
	currentFinished metav1.Time
	hasCurrent      bool
	lastStarted     metav1.Time
	lastFinished    metav1.Time
	hasLast         bool
	runningStarted  metav1.Time
	hasRunning      bool
}

func candidateSignalTransform(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	readyLTT, hasReadyLTT := extractReadyLastTransitionTime(pod)
	appSnaps := snapshotContainerTimes(pod.Status.ContainerStatuses)
	initSnaps := snapshotContainerTimes(pod.Status.InitContainerStatuses)

	projected := podcache.Project(pod)
	if hasReadyLTT {
		setReadyLastTransitionTime(projected, readyLTT)
	}
	restoreContainerTimes(projected.Status.ContainerStatuses, appSnaps)
	restoreContainerTimes(projected.Status.InitContainerStatuses, initSnaps)
	return projected, nil
}

func snapshotContainerTimes(statuses []corev1.ContainerStatus) map[string]containerTimeSnapshot {
	out := make(map[string]containerTimeSnapshot, len(statuses))
	for i := range statuses {
		st := statuses[i]
		snap := containerTimeSnapshot{}
		if st.State.Terminated != nil {
			snap.hasCurrent = true
			snap.currentStarted = st.State.Terminated.StartedAt
			snap.currentFinished = st.State.Terminated.FinishedAt
		}
		if st.LastTerminationState.Terminated != nil {
			snap.hasLast = true
			snap.lastStarted = st.LastTerminationState.Terminated.StartedAt
			snap.lastFinished = st.LastTerminationState.Terminated.FinishedAt
		}
		if st.State.Running != nil {
			snap.hasRunning = true
			snap.runningStarted = st.State.Running.StartedAt
		}
		out[st.Name] = snap
	}
	return out
}

func restoreContainerTimes(statuses []corev1.ContainerStatus, snaps map[string]containerTimeSnapshot) {
	for i := range statuses {
		snap, ok := snaps[statuses[i].Name]
		if !ok {
			continue
		}
		if snap.hasCurrent && statuses[i].State.Terminated != nil {
			statuses[i].State.Terminated.StartedAt = snap.currentStarted
			statuses[i].State.Terminated.FinishedAt = snap.currentFinished
		}
		if snap.hasLast && statuses[i].LastTerminationState.Terminated != nil {
			statuses[i].LastTerminationState.Terminated.StartedAt = snap.lastStarted
			statuses[i].LastTerminationState.Terminated.FinishedAt = snap.lastFinished
		}
		if snap.hasRunning && statuses[i].State.Running != nil {
			statuses[i].State.Running.StartedAt = snap.runningStarted
		}
	}
}

func extractReadyLastTransitionTime(pod *corev1.Pod) (metav1.Time, bool) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return pod.Status.Conditions[i].LastTransitionTime, !pod.Status.Conditions[i].LastTransitionTime.IsZero()
		}
	}
	return metav1.Time{}, false
}

func setReadyLastTransitionTime(pod *corev1.Pod, ts metav1.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].LastTransitionTime = ts
			return
		}
	}
}

func candidateSignificantPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			pod, ok := e.Object.(*corev1.Pod)
			return ok && candidateFailureCue(pod)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			_, ok := e.Object.(*corev1.Pod)
			return ok
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, oldOK := e.ObjectOld.(*corev1.Pod)
			newPod, newOK := e.ObjectNew.(*corev1.Pod)
			if !oldOK || !newOK {
				return false
			}
			return candidatePodSignificantChange(oldPod, newPod)
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

func candidateFailureCue(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	if !podReadyFalse(pod) {
		return false
	}
	term := currentTerminatedNamed(pod, "main")
	return term != nil && term.ExitCode != 0
}

func candidatePodSignificantChange(oldPod, newPod *corev1.Pod) bool {
	if newPod.GetUID() != oldPod.GetUID() ||
		newPod.GetName() != oldPod.GetName() ||
		!ownerRefsEqual(oldPod.GetOwnerReferences(), newPod.GetOwnerReferences()) ||
		!labelsEqualSelected(oldPod.Labels, newPod.Labels) ||
		!deletionEqual(oldPod, newPod) ||
		podReadyStatus(oldPod) != podReadyStatus(newPod) ||
		!currentExitEqual(oldPod, newPod) ||
		restartCountOf(oldPod) != restartCountOf(newPod) {
		return true
	}
	return false
}

type signalResolvedIdentity struct {
	DCDName            string           `json:"dcdName"`
	DCDUID             string           `json:"dcdUID"`
	DCDGeneration      int64            `json:"dcdGeneration"`
	LWSName            string           `json:"lwsName"`
	LWSUID             string           `json:"lwsUID"`
	LWSGeneration      int64            `json:"lwsGeneration"`
	LeaderUID          string           `json:"leaderUID"`
	Group              string           `json:"group"`
	Worker             string           `json:"worker"`
	Revision           string           `json:"revision"`
	Owned              bool             `json:"owned"`
	RevisionCoverageOK bool             `json:"revisionCoverageOK"`
	RevisionReason     string           `json:"revisionReason,omitempty"`
	Reason             string           `json:"reason,omitempty"`
	ControllerChain    []map[string]any `json:"controllerChain"`
}

func mapCandidatePodToDCD(ctx context.Context, reader client.Reader, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil
	}
	resolved := resolveCandidateOwnerChain(ctx, reader, pod)
	if !resolved.Owned || resolved.DCDName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: resolved.DCDName}}}
}

func resolveCandidateOwnerChain(ctx context.Context, reader client.Reader, pod *corev1.Pod) signalResolvedIdentity {
	out := signalResolvedIdentity{
		Group:  pod.Labels[leaderworkersetv1.GroupIndexLabelKey],
		Worker: pod.Labels[leaderworkersetv1.WorkerIndexLabelKey],
		Revision: pod.Labels[leaderworkersetv1.RevisionKey],
	}
	if out.Group == "" || out.Worker == "" || out.Revision == "" || pod.Labels[leaderworkersetv1.SetNameLabelKey] == "" {
		out.Reason = "missing_lws_labels"
		return out
	}

	stsRef := controllerRef(pod)
	if err := validateOwnerRef(stsRef, "StatefulSet", []string{"apps/v1"}); err != nil {
		out.Reason = "pod_controller_not_sts:" + err.Error()
		return out
	}
	sts := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: stsRef.Name}, sts); err != nil {
		out.Reason = "sts_cache_miss"
		return out
	}
	if string(sts.UID) != string(stsRef.UID) || sts.Name != stsRef.Name || sts.Namespace != pod.Namespace {
		out.Reason = "sts_identity_mismatch"
		return out
	}
	out.ControllerChain = append(out.ControllerChain, ownerHop("Pod", pod, stsRef, sts.DeletionTimestamp != nil))

	stsOwner := controllerRef(sts)
	var lws *leaderworkersetv1.LeaderWorkerSet
	var leaderPod *corev1.Pod
	var leaderSTS *appsv1.StatefulSet
	switch {
	case stsOwner != nil && stsOwner.Kind == "LeaderWorkerSet":
		if out.Worker != "0" {
			out.Reason = "non_leader_pod_owned_by_lws"
			return out
		}
		if err := validateOwnerRef(stsOwner, "LeaderWorkerSet", []string{signalLWSAPIVersion}); err != nil {
			out.Reason = "sts_controller_not_lws:" + err.Error()
			return out
		}
		got := &leaderworkersetv1.LeaderWorkerSet{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: stsOwner.Name}, got); err != nil {
			out.Reason = "lws_cache_miss"
			return out
		}
		if string(got.UID) != string(stsOwner.UID) || got.Name != stsOwner.Name || got.Namespace != pod.Namespace {
			out.Reason = "lws_identity_mismatch"
			return out
		}
		lws = got
		leaderPod = pod
		leaderSTS = sts
		out.LeaderUID = string(pod.UID)
		out.ControllerChain = append(out.ControllerChain, ownerHop("StatefulSet", sts, stsOwner, got.DeletionTimestamp != nil))
	case stsOwner != nil && stsOwner.Kind == "Pod":
		if out.Worker == "0" {
			out.Reason = "leader_index_owned_by_pod"
			return out
		}
		if err := validateOwnerRef(stsOwner, "Pod", []string{"v1"}); err != nil {
			out.Reason = "worker_sts_controller_not_pod:" + err.Error()
			return out
		}
		lp := &corev1.Pod{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: stsOwner.Name}, lp); err != nil {
			out.Reason = "leader_pod_cache_miss"
			return out
		}
		if string(lp.UID) != string(stsOwner.UID) || lp.Name != stsOwner.Name || lp.Namespace != pod.Namespace {
			out.Reason = "leader_pod_identity_mismatch"
			return out
		}
		if lp.Labels[leaderworkersetv1.WorkerIndexLabelKey] != "0" {
			out.Reason = "leader_pod_worker_index_not_zero"
			return out
		}
		if lp.Labels[leaderworkersetv1.SetNameLabelKey] != pod.Labels[leaderworkersetv1.SetNameLabelKey] ||
			lp.Labels[leaderworkersetv1.GroupIndexLabelKey] != out.Group {
			out.Reason = "leader_set_or_group_mismatch"
			return out
		}
		if lp.Labels[leaderworkersetv1.RevisionKey] == "" || pod.Labels[leaderworkersetv1.RevisionKey] == "" ||
			lp.Labels[leaderworkersetv1.RevisionKey] != pod.Labels[leaderworkersetv1.RevisionKey] {
			out.Reason = "leader_revision_mismatch_or_missing"
			return out
		}
		leaderPod = lp
		out.LeaderUID = string(lp.UID)
		out.ControllerChain = append(out.ControllerChain, ownerHop("StatefulSet", sts, stsOwner, lp.DeletionTimestamp != nil))

		leaderSTSRef := controllerRef(lp)
		if err := validateOwnerRef(leaderSTSRef, "StatefulSet", []string{"apps/v1"}); err != nil {
			out.Reason = "leader_pod_controller_not_sts:" + err.Error()
			return out
		}
		ls := &appsv1.StatefulSet{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: leaderSTSRef.Name}, ls); err != nil {
			out.Reason = "leader_sts_cache_miss"
			return out
		}
		if string(ls.UID) != string(leaderSTSRef.UID) || ls.Name != leaderSTSRef.Name || ls.Namespace != pod.Namespace {
			out.Reason = "leader_sts_identity_mismatch"
			return out
		}
		leaderSTS = ls
		out.ControllerChain = append(out.ControllerChain, ownerHop("Pod", lp, leaderSTSRef, ls.DeletionTimestamp != nil))

		lwsOwner := controllerRef(ls)
		if err := validateOwnerRef(lwsOwner, "LeaderWorkerSet", []string{signalLWSAPIVersion}); err != nil {
			out.Reason = "leader_sts_controller_not_lws:" + err.Error()
			return out
		}
		got := &leaderworkersetv1.LeaderWorkerSet{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: lwsOwner.Name}, got); err != nil {
			out.Reason = "lws_cache_miss"
			return out
		}
		if string(got.UID) != string(lwsOwner.UID) || got.Name != lwsOwner.Name || got.Namespace != pod.Namespace {
			out.Reason = "lws_identity_mismatch"
			return out
		}
		lws = got
		out.ControllerChain = append(out.ControllerChain, ownerHop("StatefulSet", ls, lwsOwner, got.DeletionTimestamp != nil))
	default:
		out.Reason = "unexpected_sts_controller_kind"
		return out
	}

	if pod.Labels[leaderworkersetv1.SetNameLabelKey] != lws.Name {
		out.Reason = "pod_lws_name_mismatch"
		return out
	}
	out.LWSName = lws.Name
	out.LWSUID = string(lws.UID)
	out.LWSGeneration = lws.Generation

	dcdRef := controllerRef(lws)
	if err := validateOwnerRef(dcdRef, "DynamoComponentDeployment", []string{
		nvidiacomv1beta1.GroupVersion.String(),
		nvidiacomv1alpha1.GroupVersion.String(),
	}); err != nil {
		out.Reason = "lws_controller_not_dcd:" + err.Error()
		return out
	}
	dcd := &nvidiacomv1beta1.DynamoComponentDeployment{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: dcdRef.Name}, dcd); err != nil {
		out.Reason = "dcd_cache_miss"
		return out
	}
	if string(dcd.UID) != string(dcdRef.UID) || dcd.Name != dcdRef.Name || dcd.Namespace != pod.Namespace {
		out.Reason = "dcd_identity_mismatch"
		return out
	}
	out.ControllerChain = append(out.ControllerChain, ownerHop("LeaderWorkerSet", lws, dcdRef, dcd.DeletionTimestamp != nil))
	out.DCDName = dcd.Name
	out.DCDUID = string(dcd.UID)
	out.DCDGeneration = dcd.Generation
	out.Owned = true

	ok, reason := revisionCoverageOK(pod, sts, leaderPod, leaderSTS)
	out.RevisionCoverageOK = ok
	out.RevisionReason = reason
	return out
}

func revisionCoverageOK(pod *corev1.Pod, owningSTS *appsv1.StatefulSet, leaderPod *corev1.Pod, leaderSTS *appsv1.StatefulSet) (bool, string) {
	podRev := pod.Labels[leaderworkersetv1.RevisionKey]
	if podRev == "" {
		return false, "pod_revision_missing"
	}
	owningTemplateRev := ""
	if owningSTS != nil && owningSTS.Spec.Template.Labels != nil {
		owningTemplateRev = owningSTS.Spec.Template.Labels[leaderworkersetv1.RevisionKey]
	}
	if owningTemplateRev == "" {
		return false, "owning_sts_template_revision_missing"
	}
	if owningTemplateRev != podRev {
		return false, "owning_sts_template_revision_mismatch"
	}
	if leaderPod != nil {
		leaderRev := leaderPod.Labels[leaderworkersetv1.RevisionKey]
		if leaderRev == "" {
			return false, "leader_pod_revision_missing"
		}
		if leaderRev != podRev {
			return false, "leader_pod_revision_mismatch"
		}
	}
	if leaderSTS != nil {
		leaderTemplateRev := ""
		if leaderSTS.Spec.Template.Labels != nil {
			leaderTemplateRev = leaderSTS.Spec.Template.Labels[leaderworkersetv1.RevisionKey]
		}
		if leaderTemplateRev == "" {
			return false, "leader_sts_template_revision_missing"
		}
		if leaderTemplateRev != podRev {
			return false, "leader_sts_template_revision_mismatch"
		}
	}
	return true, ""
}

func ownerHop(childKind string, child client.Object, ref *metav1.OwnerReference, ownerDeleting bool) map[string]any {
	return map[string]any{
		"childKind":     childKind,
		"childName":     child.GetName(),
		"childUID":      string(child.GetUID()),
		"ownerKind":     ref.Kind,
		"ownerAPI":      ref.APIVersion,
		"ownerName":     ref.Name,
		"ownerUID":      string(ref.UID),
		"controller":    true,
		"ownerDeleting": ownerDeleting,
	}
}

func validateOwnerRef(ref *metav1.OwnerReference, kind string, apis []string) error {
	if ref == nil {
		return errors.New("missing controller owner")
	}
	if ref.Controller == nil || !*ref.Controller {
		return errors.New("owner controller bit false")
	}
	if ref.Name == "" || ref.UID == "" {
		return errors.New("owner name/uid empty")
	}
	if ref.Kind != kind {
		return fmt.Errorf("kind %q want %q", ref.Kind, kind)
	}
	ok := false
	for _, api := range apis {
		if ref.APIVersion == api {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("apiVersion %q not in %v", ref.APIVersion, apis)
	}
	return nil
}

type signalReconcileWrapper struct {
	next        *DynamoComponentDeploymentReconciler
	cacheReader client.Client
	trace       *signalTrace
	mode        string
	projection  string
	namespace   string
	dedupe      *signalDedupe
}

func (w *signalReconcileWrapper) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	begin := time.Now().UTC()
	pre := w.captureLevel(ctx, req)
	w.trace.record(map[string]any{
		"layer":       "queued_pre",
		"mode":        w.mode,
		"projection":  w.projection,
		"read_origin": "manager_cache",
		"begin":       begin.Format(time.RFC3339Nano),
		"request":     req,
		"snapshot":    pre,
	})
	w.classifySnapshot(pre)

	result, err := w.next.Reconcile(ctx, req)
	end := time.Now().UTC()
	post := w.captureLevel(ctx, req)
	w.trace.record(map[string]any{
		"layer":          "queued_post",
		"mode":           w.mode,
		"projection":     w.projection,
		"read_origin":    "manager_cache",
		"begin":          begin.Format(time.RFC3339Nano),
		"end":            end.Format(time.RFC3339Nano),
		"request":        req,
		"result":         result,
		"reconcileError": errString(err),
		"snapshot":       post,
	})
	w.classifySnapshot(post)
	return result, err
}

func (w *signalReconcileWrapper) captureLevel(ctx context.Context, req ctrl.Request) map[string]any {
	out := map[string]any{
		"namespace":   req.Namespace,
		"request_dcd": req.Name,
		"pods":        []map[string]any{},
		"unknowns":    []string{},
	}
	dcd := &nvidiacomv1beta1.DynamoComponentDeployment{}
	if err := w.cacheReader.Get(ctx, req.NamespacedName, dcd); err != nil {
		out["unknowns"] = append(out["unknowns"].([]string), "dcd_cache_miss:"+err.Error())
		return out
	}
	out["dcdUID"] = string(dcd.UID)
	out["dcdGeneration"] = dcd.Generation
	lwsName := leaderWorkerSetName(dcd)
	lws := &leaderworkersetv1.LeaderWorkerSet{}
	if err := w.cacheReader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: lwsName}, lws); err != nil {
		out["unknowns"] = append(out["unknowns"].([]string), "lws_cache_miss:"+err.Error())
		return out
	}
	if !controllerOwnedBy(lws, "LeaderWorkerSet", []string{signalLWSAPIVersion}, dcd, "DynamoComponentDeployment", []string{
		nvidiacomv1beta1.GroupVersion.String(),
		nvidiacomv1alpha1.GroupVersion.String(),
	}) {
		out["unknowns"] = append(out["unknowns"].([]string), "lws_owner_mismatch")
		return out
	}
	out["lwsName"] = lws.Name
	out["lwsUID"] = string(lws.UID)
	out["lwsGeneration"] = lws.Generation

	podList := &corev1.PodList{}
	if err := w.cacheReader.List(ctx, podList, client.InNamespace(req.Namespace), client.MatchingFields{signalLWSNameIndex: lws.Name}); err != nil {
		out["unknowns"] = append(out["unknowns"].([]string), "pod_list_failed:"+err.Error())
		return out
	}
	pods := make([]map[string]any, 0, len(podList.Items))
	for i := range podList.Items {
		pods = append(pods, observeSignalPod(ctx, w.cacheReader, &podList.Items[i], dcd, lws, w.mode, w.projection))
	}
	out["pods"] = pods
	return out
}

func (w *signalReconcileWrapper) classifySnapshot(snapshot map[string]any) {
	pods, _ := snapshot["pods"].([]map[string]any)
	for _, podObs := range pods {
		verdict, _ := podObs["verdict"].(string)
		if verdict != "eligible_failure" {
			if verdict == "unknown" {
				w.trace.unknown.Add(1)
			}
			continue
		}
		key, _ := podObs["epochKey"].(string)
		if key == "" {
			continue
		}
		first, overflow, firstSeen := w.dedupe.observe(key, podObs)
		if overflow {
			w.trace.fail(errors.New("process-local dedupe capacity overflow"))
			return
		}
		if !first {
			continue
		}
		podObs["firstSeen"] = firstSeen
		w.trace.eligible.Add(1)
		w.trace.record(map[string]any{
			"layer":       "summary",
			"kind":        "eligible_incarnation_first_seen",
			"mode":        w.mode,
			"projection":  w.projection,
			"read_origin": "manager_cache",
			"observation": podObs,
			"note":        "process_local_observed_lower_bound",
		})
	}
}

func observeSignalPod(ctx context.Context, reader client.Reader, pod *corev1.Pod, dcd *nvidiacomv1beta1.DynamoComponentDeployment, lws *leaderworkersetv1.LeaderWorkerSet, mode, projection string) map[string]any {
	obs := map[string]any{
		"namespace":             pod.Namespace,
		"request_dcd":           dcd.Name,
		"dcdUID":                string(dcd.UID),
		"dcdGeneration":         dcd.Generation,
		"lwsName":               lws.Name,
		"lwsUID":                string(lws.UID),
		"lwsGeneration":         lws.Generation,
		"podName":               pod.Name,
		"podUID":                string(pod.UID),
		"podRV":                 pod.ResourceVersion,
		"creationTimestamp":     formatMetaTime(pod.CreationTimestamp),
		"deletionTimestamp":     formatMetaTimePtr(pod.DeletionTimestamp),
		"controllerOwners":      ownerRefSnapshots(pod.OwnerReferences),
		"group":                 pod.Labels[leaderworkersetv1.GroupIndexLabelKey],
		"worker":                pod.Labels[leaderworkersetv1.WorkerIndexLabelKey],
		"revision":              pod.Labels[leaderworkersetv1.RevisionKey],
		"mode":                  mode,
		"projection":            projection,
		"read_origin":           "manager_cache",
	}
	readyStatus, readyLTT, readyKnown := podReadyObservation(pod)
	obs["podReady"] = readyStatus
	obs["podReadyLastTransitionTime"] = readyLTT
	waiting, running, terminated := containerStateSnapshot(pod)
	obs["waiting"] = waiting
	obs["running"] = running
	obs["terminated"] = terminated
	obs["restartCount"] = restartCountOf(pod)

	resolved := resolveCandidateOwnerChain(ctx, reader, pod)
	obs["leaderUID"] = resolved.LeaderUID
	obs["controllerChain"] = resolved.ControllerChain
	obs["resolved"] = resolved
	if !resolved.Owned {
		obs["verdict"] = "unknown"
		obs["reason"] = resolved.Reason
		return obs
	}
	if resolved.DCDUID != string(dcd.UID) || resolved.DCDGeneration != dcd.Generation ||
		resolved.LWSUID != string(lws.UID) || resolved.LWSGeneration != lws.Generation ||
		resolved.DCDName != dcd.Name || resolved.LWSName != lws.Name {
		obs["verdict"] = "unknown"
		obs["reason"] = "resolved_identity_mismatch"
		return obs
	}
	if !resolved.RevisionCoverageOK {
		obs["verdict"] = "unknown"
		obs["reason"] = resolved.RevisionReason
		return obs
	}
	if pod.DeletionTimestamp != nil {
		obs["verdict"] = "ignored"
		obs["reason"] = "pod_deleting"
		return obs
	}
	if !readyKnown || readyStatus != string(corev1.ConditionFalse) {
		if readyKnown && readyStatus == string(corev1.ConditionTrue) {
			obs["verdict"] = "healthy"
			obs["reason"] = "ready_true"
			return obs
		}
		obs["verdict"] = "ignored"
		obs["reason"] = "ready_not_false"
		return obs
	}
	term := currentTerminatedNamed(pod, "main")
	if term == nil {
		obs["verdict"] = "ignored"
		obs["reason"] = "no_current_main_terminated"
		return obs
	}
	obs["exitCode"] = term.ExitCode
	if term.ExitCode == 0 {
		obs["verdict"] = "ignored"
		obs["reason"] = "zero_exit"
		return obs
	}
	if term.ExitCode != signalIntendedFaultExit {
		obs["verdict"] = "unknown"
		obs["reason"] = "unsupported_nonzero_exit_not_fixture_exit23"
		return obs
	}
	if term.FinishedAt.IsZero() {
		obs["verdict"] = "unknown"
		obs["reason"] = "missing_finished_at"
		return obs
	}
	now := time.Now().UTC()
	if term.FinishedAt.Time.Before(pod.CreationTimestamp.Time) {
		obs["verdict"] = "unknown"
		obs["reason"] = "finished_before_creation"
		return obs
	}
	if term.FinishedAt.Time.After(now.Add(signalClockSkew)) {
		obs["verdict"] = "unknown"
		obs["reason"] = "finished_in_future"
		return obs
	}
	obs["finishedAt"] = formatMetaTime(term.FinishedAt)
	obs["epochKey"] = strings.Join([]string{
		resolved.DCDUID,
		strconv.FormatInt(resolved.DCDGeneration, 10),
		resolved.LWSUID,
		strconv.FormatInt(resolved.LWSGeneration, 10),
		resolved.Revision,
		resolved.Group,
		resolved.LeaderUID,
	}, "|")
	obs["verdict"] = "eligible_failure"
	obs["reason"] = "fixture_main_exit23_ready_false"
	obs["proof"] = map[string]any{
		"podUID":       string(pod.UID),
		"container":    "main",
		"exitCode":     term.ExitCode,
		"finishedAt":   formatMetaTime(term.FinishedAt),
		"restartCount": restartCountOf(pod),
	}
	return obs
}

type signalCountingClient struct {
	client.Client
	trace      *signalTrace
	mode       string
	projection string
	counts     *signalProdReadCounts
}

func (c *signalCountingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	if pod, ok := obj.(*corev1.Pod); ok {
		c.counts.gets.Add(1)
		rec := map[string]any{
			"layer":       "original_prod_read",
			"op":          "get",
			"mode":        c.mode,
			"projection":  c.projection,
			"read_origin": "manager_cache",
			"key":         key,
			"error":       errString(err),
		}
		if err == nil {
			rec["podUID"] = string(pod.UID)
			rec["podRV"] = pod.ResourceVersion
			rec["podReady"], rec["podReadyLastTransitionTime"], _ = podReadyObservation(pod)
			if term := currentTerminatedNamed(pod, "main"); term != nil {
				rec["exitCode"] = term.ExitCode
				rec["finishedAt"] = formatMetaTime(term.FinishedAt)
			}
		}
		c.trace.record(rec)
	}
	return err
}

func (c *signalCountingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := c.Client.List(ctx, list, opts...)
	if podList, ok := list.(*corev1.PodList); ok {
		c.counts.lists.Add(1)
		c.counts.listItems.Add(int64(len(podList.Items)))
		listOpts := &client.ListOptions{}
		for _, opt := range opts {
			opt.ApplyToList(listOpts)
		}
		fieldSelector := ""
		if listOpts.FieldSelector != nil {
			fieldSelector = listOpts.FieldSelector.String()
		}
		rec := map[string]any{
			"layer":         "original_prod_read",
			"op":            "list",
			"mode":          c.mode,
			"projection":    c.projection,
			"read_origin":   "manager_cache",
			"namespace":     listOpts.Namespace,
			"fieldSelector": fieldSelector,
			"itemCount":     len(podList.Items),
			"error":         errString(err),
		}
		items := make([]map[string]any, 0, len(podList.Items))
		for i := range podList.Items {
			item := map[string]any{
				"podName": podList.Items[i].Name,
				"podUID":  string(podList.Items[i].UID),
				"podRV":   podList.Items[i].ResourceVersion,
			}
			item["podReady"], item["podReadyLastTransitionTime"], _ = podReadyObservation(&podList.Items[i])
			items = append(items, item)
		}
		rec["items"] = items
		c.trace.record(rec)
	}
	return err
}

type signalProdReadCounts struct {
	gets      atomic.Int64
	lists     atomic.Int64
	listItems atomic.Int64
}

type signalDedupe struct {
	mu   sync.Mutex
	seen map[string]map[string]any
}

func newSignalDedupe() *signalDedupe {
	return &signalDedupe{seen: map[string]map[string]any{}}
}

func (d *signalDedupe) observe(key string, obs map[string]any) (first bool, overflow bool, firstSeen string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.seen[key]; ok {
		fs, _ := existing["firstSeen"].(string)
		return false, false, fs
	}
	if len(d.seen) >= signalMaxDedupeEntries {
		return false, true, ""
	}
	firstSeen = time.Now().UTC().Format(time.RFC3339Nano)
	cp := map[string]any{}
	for k, v := range obs {
		cp[k] = v
	}
	cp["firstSeen"] = firstSeen
	d.seen[key] = cp
	return true, false, firstSeen
}

func (d *signalDedupe) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

type signalTrace struct {
	mu         sync.Mutex
	file       *os.File
	enc        *json.Encoder
	mode       string
	projection string
	records    atomic.Int64
	bytes      atomic.Int64
	failed     atomic.Bool
	closed     atomic.Bool
	errCh      chan error
	layers     sync.Map
	eligible   atomic.Int64
	unknown    atomic.Int64
	overflow   atomic.Bool
}

func newSignalTrace(path, mode, projection string) (*signalTrace, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	return &signalTrace{
		file:       f,
		enc:        enc,
		mode:       mode,
		projection: projection,
		errCh:      make(chan error, 1),
	}, nil
}

func (t *signalTrace) record(obj map[string]any) {
	if t == nil || t.failed.Load() || t.closed.Load() {
		return
	}
	if obj["time"] == nil {
		obj["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if obj["mode"] == nil {
		obj["mode"] = t.mode
	}
	if obj["projection"] == nil {
		obj["projection"] = t.projection
	}
	obj["test_only"] = true

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failed.Load() || t.closed.Load() || t.file == nil {
		return
	}
	if t.records.Load() >= signalMaxJSONLRecords || t.bytes.Load() >= signalMaxJSONLBytes {
		t.overflow.Store(true)
		t.failLocked(errors.New("jsonl capacity overflow"))
		return
	}
	before, _ := t.file.Seek(0, 1)
	if err := t.enc.Encode(obj); err != nil {
		t.failLocked(err)
		return
	}
	after, _ := t.file.Seek(0, 1)
	t.records.Add(1)
	t.bytes.Add(after - before)
	if layer, ok := obj["layer"].(string); ok {
		v, _ := t.layers.LoadOrStore(layer, &atomic.Int64{})
		v.(*atomic.Int64).Add(1)
	}
}

func (t *signalTrace) fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failLocked(err)
}

func (t *signalTrace) failLocked(err error) {
	if t.failed.Swap(true) {
		return
	}
	select {
	case t.errCh <- err:
	default:
	}
}

func (t *signalTrace) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed.Swap(true) {
		return nil
	}
	if t.file == nil {
		return nil
	}
	syncErr := t.file.Sync()
	closeErr := t.file.Close()
	t.file = nil
	return errors.Join(syncErr, closeErr)
}

func (t *signalTrace) summarySnapshot(scope *signalScope) map[string]any {
	layers := map[string]int64{}
	t.layers.Range(func(key, value any) bool {
		layers[key.(string)] = value.(*atomic.Int64).Load()
		return true
	})
	out := map[string]any{
		"mode":                               t.mode,
		"projection":                         t.projection,
		"layerCounts":                        layers,
		"jsonlRecords":                       t.records.Load(),
		"jsonlBytes":                         t.bytes.Load(),
		"overflow":                           t.overflow.Load(),
		"failed":                             t.failed.Load(),
		"eligibleUniqueIncarnations":         t.eligible.Load(),
		"unknownObservations":                t.unknown.Load(),
		"originalProdPodGets":                int64(0),
		"originalProdPodLists":               int64(0),
		"originalProdPodListItems":           int64(0),
		"process_local_observed_lower_bound": true,
	}
	if scope != nil && scope.counts != nil {
		out["originalProdPodGets"] = scope.counts.gets.Load()
		out["originalProdPodLists"] = scope.counts.lists.Load()
		out["originalProdPodListItems"] = scope.counts.listItems.Load()
		out["dedupeSize"] = scope.dedupe.len()
		out["leaseName"] = scope.leaseName
		out["leaseUID"] = scope.leaseUID
		out["namespace"] = scope.namespace
		out["rootIdentities"] = scope.roots
	}
	return out
}

func attachSignalCacheTap(ctx context.Context, mgr ctrl.Manager, trace *signalTrace, mode, projection string, fatal chan error) (*signalCacheTap, error) {
	informer, err := mgr.GetCache().GetInformer(ctx, &corev1.Pod{})
	if err != nil {
		return nil, err
	}
	tap := &signalCacheTap{
		accepting: true,
		inf:       informer,
		events:    make(chan map[string]any, signalCacheTapBuffer),
		done:      make(chan struct{}),
		started:   true,
	}
	go func() {
		defer close(tap.done)
		for obs := range tap.events {
			trace.record(obs)
		}
	}()
	reg, err := informer.AddEventHandler(simplePodHandler(func(pod *corev1.Pod, op string) {
		receipt := time.Now().UTC().Format(time.RFC3339Nano)
		obs := map[string]any{
			"layer":             "cache_event",
			"op":                op,
			"mode":              mode,
			"projection":        projection,
			"read_origin":       "manager_cache",
			"receiptTimestamp":  receipt,
			"namespace":         pod.Namespace,
			"podName":           pod.Name,
			"podUID":            string(pod.UID),
			"podRV":             pod.ResourceVersion,
			"creationTimestamp": formatMetaTime(pod.CreationTimestamp),
			"deletionTimestamp": formatMetaTimePtr(pod.DeletionTimestamp),
			"controllerOwners":  ownerRefSnapshots(pod.OwnerReferences),
			"group":             pod.Labels[leaderworkersetv1.GroupIndexLabelKey],
			"worker":            pod.Labels[leaderworkersetv1.WorkerIndexLabelKey],
			"revision":          pod.Labels[leaderworkersetv1.RevisionKey],
			"restartCount":      restartCountOf(pod),
		}
		readyStatus, readyLTT, _ := podReadyObservation(pod)
		obs["podReady"] = readyStatus
		obs["podReadyLastTransitionTime"] = readyLTT
		if term := currentTerminatedNamed(pod, "main"); term != nil {
			obs["exitCode"] = term.ExitCode
			obs["finishedAt"] = formatMetaTime(term.FinishedAt)
		}
		tap.trySend(obs, fatal)
	}))
	if err != nil {
		return nil, errors.Join(err, errors.Join(tap.closeAndJoin()...))
	}
	tap.reg = reg
	return tap, nil
}

func (t *signalCacheTap) trySend(obs map[string]any, fatal chan error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.accepting {
		return
	}
	select {
	case t.events <- obs:
	default:
		reportSignalFatal(fatal, errors.New("cache_event channel overflow"))
	}
}

func (t *signalCacheTap) closeProducerLocked() {
	t.accepting = false
	if !t.closed {
		close(t.events)
		t.closed = true
	}
}

func (t *signalCacheTap) closeAndJoin() []error {
	if t == nil || !t.started {
		return nil
	}
	var errs []error
	if t.reg != nil && t.inf != nil {
		if err := t.inf.RemoveEventHandler(t.reg); err != nil {
			errs = append(errs, err)
		}
	}
	t.mu.Lock()
	t.closeProducerLocked()
	t.mu.Unlock()
	select {
	case <-t.done:
	case <-time.After(signalShutdownTimeout):
		errs = append(errs, errors.New("cache tap worker join timed out"))
	}
	return errs
}

func (t *signalCacheTap) stop() []error {
	return t.closeAndJoin()
}

type podEventFn func(pod *corev1.Pod, op string)

type simplePodEventHandler struct{ fn podEventFn }

func simplePodHandler(fn podEventFn) *simplePodEventHandler { return &simplePodEventHandler{fn: fn} }

func (h *simplePodEventHandler) OnAdd(obj interface{}, _ bool) {
	if pod, ok := obj.(*corev1.Pod); ok {
		h.fn(pod, "add")
	}
}
func (h *simplePodEventHandler) OnUpdate(_, newObj interface{}) {
	if pod, ok := newObj.(*corev1.Pod); ok {
		h.fn(pod, "update")
	}
}
func (h *simplePodEventHandler) OnDelete(obj interface{}) {
	if pod, ok := obj.(*corev1.Pod); ok {
		h.fn(pod, "delete")
	}
}

type signalSummary struct {
	HarnessSuccess         bool           `json:"harnessSuccess"`
	ObservationDurationSec int            `json:"observationDurationSec"`
	Baseline               map[string]any `json:"baseline"`
	Candidate              map[string]any `json:"candidate"`
	ShutdownErrors         []string       `json:"shutdownErrors"`
	ProcessLocalLowerBound  bool           `json:"process_local_observed_lower_bound"`
	NoDurableLedger        bool           `json:"noDurableLedger"`
	ScenarioPassIsExternal bool           `json:"scenarioPassIsExternalReview"`
	Limitation             string         `json:"limitation"`
	MeasurementCompletedAt string         `json:"measurementCompletedAt"`
	RunError               string         `json:"runError,omitempty"`
}

func writeSignalSummary(path string, summary signalSummary) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		return err
	}
	return f.Sync()
}

func controllerOwnedBy(obj client.Object, objKind string, objAPIs []string, owner client.Object, ownerKind string, ownerAPIs []string) bool {
	_ = objKind
	_ = objAPIs
	ref := controllerRef(obj)
	if validateOwnerRef(ref, ownerKind, ownerAPIs) != nil {
		return false
	}
	return ref.UID == owner.GetUID() && ref.Name == owner.GetName()
}

func controllerRef(obj client.Object) *metav1.OwnerReference {
	owners := obj.GetOwnerReferences()
	for i := range owners {
		ref := owners[i]
		if ref.Controller != nil && *ref.Controller {
			cp := ref
			return &cp
		}
	}
	return nil
}

func ownerRefSnapshots(refs []metav1.OwnerReference) []map[string]any {
	out := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		item := map[string]any{
			"kind":       ref.Kind,
			"apiVersion": ref.APIVersion,
			"name":       ref.Name,
			"uid":        string(ref.UID),
		}
		if ref.Controller != nil {
			item["controller"] = *ref.Controller
		}
		out = append(out, item)
	}
	return out
}

func ownerRefsEqual(a, b []metav1.OwnerReference) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		aCtrl := a[i].Controller != nil && *a[i].Controller
		bCtrl := b[i].Controller != nil && *b[i].Controller
		if a[i].UID != b[i].UID || a[i].Name != b[i].Name || a[i].Kind != b[i].Kind ||
			a[i].APIVersion != b[i].APIVersion || aCtrl != bCtrl {
			return false
		}
	}
	return true
}

func labelsEqualSelected(a, b map[string]string) bool {
	keys := []string{
		leaderworkersetv1.SetNameLabelKey,
		leaderworkersetv1.GroupIndexLabelKey,
		leaderworkersetv1.WorkerIndexLabelKey,
		leaderworkersetv1.RevisionKey,
	}
	for _, key := range keys {
		if a[key] != b[key] {
			return false
		}
	}
	return true
}

func deletionEqual(a, b *corev1.Pod) bool {
	return (a.DeletionTimestamp == nil) == (b.DeletionTimestamp == nil)
}

func currentExitEqual(a, b *corev1.Pod) bool {
	ta, tb := currentTerminatedNamed(a, "main"), currentTerminatedNamed(b, "main")
	if ta == nil || tb == nil {
		return ta == nil && tb == nil
	}
	return ta.ExitCode == tb.ExitCode && ta.FinishedAt.Equal(&tb.FinishedAt)
}

func currentTerminatedNamed(pod *corev1.Pod, name string) *corev1.ContainerStateTerminated {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name != name {
			continue
		}
		return pod.Status.ContainerStatuses[i].State.Terminated
	}
	return nil
}

func restartCountOf(pod *corev1.Pod) int32 {
	var total int32
	for i := range pod.Status.ContainerStatuses {
		total += pod.Status.ContainerStatuses[i].RestartCount
	}
	return total
}

func podReadyFalse(pod *corev1.Pod) bool {
	status, _, ok := podReadyObservation(pod)
	return ok && status == string(corev1.ConditionFalse)
}

func podReadyStatus(pod *corev1.Pod) corev1.ConditionStatus {
	status, _, ok := podReadyObservation(pod)
	if !ok {
		return ""
	}
	return corev1.ConditionStatus(status)
}

func podReadyObservation(pod *corev1.Pod) (status string, lastTransition string, ok bool) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type != corev1.PodReady {
			continue
		}
		status = string(pod.Status.Conditions[i].Status)
		if pod.Status.Conditions[i].LastTransitionTime.IsZero() {
			lastTransition = "Unknown"
		} else {
			lastTransition = formatMetaTime(pod.Status.Conditions[i].LastTransitionTime)
		}
		return status, lastTransition, true
	}
	return "Unknown", "Unknown", false
}

func containerStateSnapshot(pod *corev1.Pod) (waiting, running, terminated map[string]any) {
	for i := range pod.Status.ContainerStatuses {
		st := pod.Status.ContainerStatuses[i]
		if st.Name != "main" {
			continue
		}
		if st.State.Waiting != nil {
			waiting = map[string]any{"container": st.Name, "reason": st.State.Waiting.Reason}
		}
		if st.State.Running != nil {
			started := "Unknown"
			if !st.State.Running.StartedAt.IsZero() {
				started = formatMetaTime(st.State.Running.StartedAt)
			}
			running = map[string]any{"container": st.Name, "startedAt": started}
		}
		if st.State.Terminated != nil {
			finished := "Unknown"
			if !st.State.Terminated.FinishedAt.IsZero() {
				finished = formatMetaTime(st.State.Terminated.FinishedAt)
			}
			terminated = map[string]any{
				"container":  st.Name,
				"exitCode":   st.State.Terminated.ExitCode,
				"reason":     st.State.Terminated.Reason,
				"finishedAt": finished,
			}
		}
	}
	return waiting, running, terminated
}

func formatMetaTime(t metav1.Time) string {
	if t.IsZero() {
		return "Unknown"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func formatMetaTimePtr(t *metav1.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func errorStrings(errs []error) []string {
	out := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			out = append(out, err.Error())
		}
	}
	return out
}
