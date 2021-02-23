package k8sutil

import (
	"context"
	api "github.com/Facecircccccle/etcd-operator/pkg/apis/cache/v1alpha1"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Cluster interface {
	UpdateCluster(namespace string, cluster *api.Cluster) error
}

type ClusterOption struct {
	client client.Client
	logger logr.Logger
}

func NewCluster(kubeClient client.Client, logger logr.Logger) Cluster {
	logger = logger.WithValues("service", "crd.Cluster")
	return &ClusterOption{
		client: kubeClient,
		logger: logger,
	}
}

func (c *ClusterOption) UpdateCluster(namespace string, cluster *api.Cluster) error {
	err := c.client.Update(context.TODO(), cluster)
	if err != nil {
		c.logger.WithValues("namespace", namespace, "cluster", cluster.Name, "conditions", cluster.Status.Conditions).
			Error(err, "ClusterStatus")
		return err
	}
	c.logger.WithValues("namespace", namespace, "cluster", cluster.Name, "conditions", cluster.Status.Conditions).
		V(3).Info("ClusterStatus updated")
	return nil
}