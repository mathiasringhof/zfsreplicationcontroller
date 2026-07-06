package main

import (
	"flag"
	"os"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var metricsAddr, probeAddr, image, watchNamespace string
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "metrics bind address")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "probe bind address")
	flag.StringVar(&image, "datamover-image", os.Getenv("DATA_MOVER_IMAGE"), "datamover image")
	flag.StringVar(&watchNamespace, "watch-namespace", os.Getenv("WATCH_NAMESPACE"), "namespace to watch; empty watches all namespaces")
	flag.Parse()

	ctrl.SetLogger(logzap.New())

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(coordinationv1.AddToScheme(scheme))
	utilruntime.Must(zfsv1.AddToScheme(scheme))

	config := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(config, managerOptions(scheme, metricsAddr, probeAddr, watchNamespace))
	if err != nil {
		panic(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	runReconciler := &controller.ZFSReplicationRunReconciler{
		Client:         mgr.GetClient(),
		APIReader:      mgr.GetAPIReader(),
		Scheme:         scheme,
		DataMoverImage: image,
		PodLogs:        controller.KubernetesPodLogReader{Client: clientset},
	}
	if err := runReconciler.SetupWithManager(mgr); err != nil {
		panic(err)
	}
	scheduleReconciler := &controller.ZFSReplicationScheduleReconciler{
		Client: mgr.GetClient(),
		Scheme: scheme,
	}
	if err := scheduleReconciler.SetupWithManager(mgr); err != nil {
		panic(err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		panic(err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		panic(err)
	}
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		panic(err)
	}
}

func managerOptions(scheme *runtime.Scheme, metricsAddr, probeAddr, watchNamespace string) ctrl.Options {
	opts := ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: probeAddr,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
	}
	watchNamespace = strings.TrimSpace(watchNamespace)
	if watchNamespace != "" {
		opts.Cache.DefaultNamespaces = map[string]cache.Config{
			watchNamespace: {},
		}
	}
	return opts
}
