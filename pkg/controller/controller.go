package controller

import (
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// AddToManagerFuncs is a list of functions to add all Controllers to the Manager
var AddToManagerFuncs []func(manager.Manager) error

// AddToManager adds all Controllers to the Manager
func AddToManager(m manager.Manager) error {
	for _, f := range AddToManagerFuncs {
		if err := f(m); err != nil {
			return err
		}
	}
	return nil
}


//type Controller struct {
//	logger *logrus.Entry
//	Config
//	clusters map[string]*handler.Cluster
//}
//
//type Config struct {
//	Namespace      string
//	ClusterWide    bool
//	ServiceAccount string
//	KubeCli        kubernetes.Interface
//	KubeExtCli     apiextensionsclient.Interface
//	EtcdCRCli      etcdutil.Interface
//	CreateCRD      bool
//}
//
//func New(config Config) *Controller {
//	return &Controller{
//		logger: logrus.WithField("pkg", "controller"),
//		Config: config,
//		clusters: make(map[string]*handler.Cluster),
//	}
//}