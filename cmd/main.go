/*
Copyright 2026 SRE Fleet Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"os"
	"strings"
	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	webhookserver "sigs.k8s.io/controller-runtime/pkg/webhook"

	upgradev1alpha1 "github.com/duritong/hypershift-upgrade-coordinator/api/v1alpha1"
	"github.com/duritong/hypershift-upgrade-coordinator/internal/controller"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(upgradev1alpha1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var hostedClusterNamespace string
	var operatorNamespace, guestEndpointMode, guestObserverNamespace string
	flag.StringVar(&operatorNamespace, "operator-namespace", os.Getenv("OPERATOR_NAMESPACE"), "Deployment namespace for observer credential Secrets (required).")
	flag.StringVar(&guestEndpointMode, "guest-endpoint-mode", controller.GuestEndpointInternal, "Guest API endpoint: internal or published; no automatic fallback.")
	flag.StringVar(&guestObserverNamespace, "guest-observer-namespace", controller.GuestObserverNamespace, "Dedicated HUC-owned namespace for the observer ServiceAccount in each hosted cluster.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	defaultHCNamespace := "clusters"
	if envVal, ok := os.LookupEnv("HOSTED_CLUSTER_NAMESPACE"); ok {
		defaultHCNamespace = envVal
	}
	flag.StringVar(&hostedClusterNamespace, "hosted-cluster-namespace", defaultHCNamespace, "The namespace of HostedCluster resources managed by this operator.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	hostedClusterNamespace = strings.Trim(strings.TrimSpace(hostedClusterNamespace), "\"'")

	logger := zap.New(zap.UseFlagOptions(&opts))
	ctrl.SetLogger(logger)
	klog.SetLogger(logger)
	if err := controller.ValidateGuestObserverNamespace(guestObserverNamespace); err != nil {
		setupLog.Error(err, "invalid guest-observer-namespace")
		os.Exit(1)
	}
	if operatorNamespace == "" || (guestEndpointMode != controller.GuestEndpointInternal && guestEndpointMode != controller.GuestEndpointPublished) {
		setupLog.Error(nil, "operator-namespace is required and guest-endpoint-mode must be internal or published")
		os.Exit(1)
	}
	secretCache, err := controller.GuestAccessCacheOptions(operatorNamespace)
	if err != nil {
		setupLog.Error(err, "unable to configure delivery Secret cache")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Client: client.Options{Cache: &client.CacheOptions{Unstructured: true}},
		// Only delivery Secrets are cached. Admin/pull Secrets remain APIReader get-only.
		Cache: secretCache,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		WebhookServer: webhookserver.NewServer(webhookserver.Options{
			Port: 9443,
		}),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "49fa9c13.0xshift.dev",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to create kubernetes clientset")
		os.Exit(1)
	}
	if err = (&controller.GuestAccessReconciler{
		Client:              mgr.GetClient(),
		GuestAccessProvider: controller.GuestAccessProvider{APIReader: mgr.GetAPIReader(), OperatorNamespace: operatorNamespace, HostedClusterNamespace: hostedClusterNamespace, EndpointMode: guestEndpointMode, ObserverNamespace: guestObserverNamespace},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "guest-access")
		os.Exit(1)
	}

	if err = (&controller.HostedClusterUpgradeReconciler{
		Client:                 mgr.GetClient(),
		APIReader:              mgr.GetAPIReader(),
		ReleaseResolver:        controller.NewRegistryReleaseMetadataResolver(),
		Scheme:                 mgr.GetScheme(),
		HostedClusterNamespace: hostedClusterNamespace,
		LogScraper:             &controller.KubeLogScraper{Client: clientset},
		GuestAccess:            &controller.GuestAccessProvider{APIReader: mgr.GetAPIReader(), OperatorNamespace: operatorNamespace, HostedClusterNamespace: hostedClusterNamespace, EndpointMode: guestEndpointMode, ObserverNamespace: guestObserverNamespace},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "HostedClusterUpgrade")
		os.Exit(1)
	}
	if err = (&upgradev1alpha1.HostedClusterUpgrade{}).SetupWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "HostedClusterUpgrade")
		os.Exit(1)
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
