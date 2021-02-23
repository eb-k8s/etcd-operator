package clustercache

import (
	api "github.com/Facecircccccle/etcd-operator/pkg/apis/cache/v1alpha1"
	"github.com/Facecircccccle/etcd-operator/pkg/util/k8sutil"
	"github.com/go-logr/logr"
)

type EtcdClusterHandler struct {
	K8sServices 	k8sutil.Services
	MetaCache      	*MetaMap
	Logger			logr.Logger
}

func (h *EtcdClusterHandler) Do(cl *api.Cluster) error{
	h.Logger.WithValues("namespace", cl.Namespace, "name", cl.Name).Info("handler doing..")

	meta, ok := h.MetaCache.Cache(cl)
	if !ok{
		if err := meta.Setup(h);
		err != nil {
				if meta.Status.Phase != api.ClusterPhaseFailed{
					meta.Status.SetReason(err.Error())
					meta.Status.SetPhase(api.ClusterPhaseFailed)
					if err := meta.UpdateCRStatus(h);
					err != nil {
						meta.Logger.Errorf("failed to update cluster phase (%v): %v", api.ClusterPhaseFailed, err)}
					}

				if err := meta.setupServices(); err != nil {
					meta.Logger.Errorf("fail to setup etcd services: %v", err)
				}
				meta.Status.ServiceName = k8sutil.ClientServiceName(meta.Cluster.Name)
				meta.Status.ClientPort = k8sutil.EtcdClientPort

				meta.Status.SetPhase(api.ClusterPhaseRunning)
				if err := meta.UpdateCRStatus(h); err != nil {
					meta.Logger.Warningf("update initial CR status failed: %v", err)
				}
				meta.Logger.Infof("start running...")
			}
	}
	if err := h.Check(meta); err !=nil{
		meta.Logger.Errorf("failed to check cluster", err)
	}
	return nil
}


func (h *EtcdClusterHandler) Check(cl *MetaCluster) error{

	if cl.Cluster.Spec.Paused {
		cl.Status.PauseControl()
		cl.Logger.Infof("control is paused, skipping reconciliation")
		return nil
	} else {
		cl.Status.Control()
	}

	running, pending, err := cl.PollPods()
	if err != nil {
		cl.Logger.Errorf("fail to poll pods: %v", err)
		return nil
	}

	if len(pending) > 0 {
		cl.Logger.Infof("skip reconciliation: running (%v), pending (%v)",
			k8sutil.GetPodNames(running), k8sutil.GetPodNames(pending))
		return nil
	}

	var rerr error
	if rerr != nil || cl.Members == nil {
		rerr = cl.UpdateMembers(PodsToMemberSet(running, cl.IsSecureClient()))
		if rerr != nil {
			cl.Logger.Errorf("failed to update members: %v", rerr)
			return nil
		}
	}

	rerr = cl.reconcile(running)

	if rerr != nil {
		cl.Logger.Errorf("failed to reconcile: %v", rerr)
		return nil
	}
	cl.updateMemberStatus(running)
	if err := cl.UpdateCRStatus(h); err != nil {
		cl.Logger.Warningf("periodic update CR status failed: %v", err)
	}

	return nil
}

