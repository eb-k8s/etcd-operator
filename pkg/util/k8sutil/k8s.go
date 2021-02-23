package k8sutil

import (
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Services interface {
	Cluster
}

type services struct {
	Cluster
}

// New returns a new Kubernetes client set.
func New(kubecli client.Client, logger logr.Logger) Services {
	return &services{
		Cluster:             NewCluster(kubecli, logger),
	}
}