package main

import (
	"flag"
	"os"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var metricsAddr, probeAddr, image string
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "metrics bind address")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "probe bind address")
	flag.StringVar(&image, "datamover-image", os.Getenv("DATA_MOVER_IMAGE"), "sender/receiver image")
	flag.Parse()

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(coordinationv1.AddToScheme(scheme))
	utilruntime.Must(zfsv1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: probeAddr,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
	})
	if err != nil {
		panic(err)
	}
	reconciler := &controller.ZFSReplicationReconciler{Client: mgr.GetClient(), Scheme: scheme, DataMoverImage: image}
	if err := reconciler.SetupWithManager(mgr); err != nil {
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
