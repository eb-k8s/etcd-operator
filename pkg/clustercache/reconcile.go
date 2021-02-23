package clustercache


import (
	"context"
	"errors"
	"fmt"
	api "github.com/Facecircccccle/etcd-operator/pkg/apis/cache/v1alpha1"
	"github.com/Facecircccccle/etcd-operator/pkg/constants"
	"github.com/Facecircccccle/etcd-operator/pkg/util/etcdutil"
	"github.com/Facecircccccle/etcd-operator/pkg/util/k8sutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *MetaCluster) reconcile(pods []*v1.Pod) error {
	c.Logger.Infoln("Start reconciling")
	defer c.Logger.Infoln("Finish reconciling")

	defer func() {
		c.Status.Size = c.Members.Size()
	}()

	sp := c.Cluster.Spec
	running := PodsToMemberSet(pods, c.IsSecureClient())
	if !running.IsEqual(c.Members) || c.Members.Size() != sp.Size {
		return c.reconcileMembers(running)
	}
	c.Status.ClearCondition(api.ClusterConditionScaling)

	if needUpgrade(pods, sp) {
		c.Status.UpgradeVersionTo(sp.Version)

		m := pickOneOldMember(pods, sp.Version)
		return c.upgradeOneMember(m.Name)
	}
	c.Status.ClearCondition(api.ClusterConditionUpgrading)

	c.Status.SetVersion(sp.Version)
	c.Status.SetReadyCondition()

	return nil
}

var ErrLostQuorum = errors.New("lost quorum")
func (c *MetaCluster) reconcileMembers(running etcdutil.MemberSet) error {
	c.Logger.Infof("running members: %s", running)
	c.Logger.Infof("cluster membership: %s", c.Members)

	unknownMembers := running.Diff(c.Members)
	if unknownMembers.Size() > 0 {
		c.Logger.Infof("removing unexpected pods: %v", unknownMembers)
		for _, m := range unknownMembers {
			if err := c.removePod(m.Name); err != nil {
				return err
			}
		}
	}
	L := running.Diff(unknownMembers)

	if L.Size() == c.Members.Size() {
		return c.resize()
	}

	if L.Size() < c.Members.Size()/2+1 {
		return ErrLostQuorum
	}

	c.Logger.Infof("removing one dead member")
	// remove dead members that doesn't have any running pods before doing resizing.
	return c.removeDeadMember(c.Members.Diff(L).PickOne())
}

func (c *MetaCluster) resize() error {
	if c.Members.Size() == c.Cluster.Spec.Size {
		return nil
	}

	if c.Members.Size() < c.Cluster.Spec.Size {
		return c.addOneMember()
	}

	return c.removeOneMember()
}

func (c *MetaCluster) addOneMember() error {
	c.Status.SetScalingUpCondition(c.Members.Size(), c.Cluster.Spec.Size)

	cfg := clientv3.Config{
		Endpoints:   c.Members.ClientURLs(),
		DialTimeout: constants.DefaultDialTimeout,
		TLS:         c.tlsConfig,
	}
	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		return fmt.Errorf("add one member failed: creating etcd client failed %v", err)
	}
	defer etcdcli.Close()

	newMember := c.newMember()
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultRequestTimeout)
	resp, err := etcdcli.MemberAdd(ctx, []string{newMember.PeerURL()})
	cancel()
	if err != nil {
		return fmt.Errorf("fail to add new member (%s): %v", newMember.Name, err)
	}
	newMember.ID = resp.Member.ID
	c.Members.Add(newMember)

	if err := c.createPod(c.Members, newMember, "existing"); err != nil {
		return fmt.Errorf("fail to create member's pod (%s): %v", newMember.Name, err)
	}
	c.Logger.Infof("added member (%s)", newMember.Name)
	_, err = c.eventsCli.Create(context.TODO(), k8sutil.NewMemberAddEvent(newMember.Name, c.Cluster), metav1.CreateOptions{})
	if err != nil {
		c.Logger.Errorf("failed to create new member add event: %v", err)
	}
	return nil
}

func (c *MetaCluster) removeOneMember() error {
	c.Status.SetScalingDownCondition(c.Members.Size(), c.Cluster.Spec.Size)

	return c.removeMember(c.Members.PickOne())
}

func (c *MetaCluster) removeDeadMember(toRemove *etcdutil.Member) error {
	c.Logger.Infof("removing dead member %q", toRemove.Name)
	_, err := c.eventsCli.Create(context.TODO(), k8sutil.ReplacingDeadMemberEvent(toRemove.Name, c.Cluster), metav1.CreateOptions{})
	if err != nil {
		c.Logger.Errorf("failed to create replacing dead member event: %v", err)
	}

	return c.removeMember(toRemove)
}

func (c *MetaCluster) removeMember(toRemove *etcdutil.Member) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("remove member (%s) failed: %v", toRemove.Name, err)
		}
	}()

	err = etcdutil.RemoveMember(c.Members.ClientURLs(), c.tlsConfig, toRemove.ID)
	if err != nil {
		switch err {
		case rpctypes.ErrMemberNotFound:
			c.Logger.Infof("etcd member (%v) has been removed", toRemove.Name)
		default:
			return err
		}
	}
	c.Members.Remove(toRemove.Name)
	_, err = c.eventsCli.Create(context.TODO(), k8sutil.MemberRemoveEvent(toRemove.Name, c.Cluster), metav1.CreateOptions{})
	if err != nil {
		c.Logger.Errorf("failed to create remove member event: %v", err)
	}
	if err := c.removePod(toRemove.Name); err != nil {
		return err
	}
	if c.isPodPVEnabled() {
		err = c.removePVC(k8sutil.PVCNameFromMember(toRemove.Name))
		if err != nil {
			return err
		}
	}
	c.Logger.Infof("removed member (%v) with ID (%d)", toRemove.Name, toRemove.ID)
	return nil
}

func (c *MetaCluster) removePVC(pvcName string) error {
	err := c.config.KubeCli.CoreV1().PersistentVolumeClaims(c.Cluster.Namespace).Delete(context.TODO(), pvcName, metav1.DeleteOptions{})
	if err != nil && !k8sutil.IsKubernetesResourceNotFoundError(err) {
		return fmt.Errorf("remove pvc (%s) failed: %v", pvcName, err)
	}
	return nil
}

func needUpgrade(pods []*v1.Pod, cs api.ClusterSpec) bool {
	return len(pods) == cs.Size && pickOneOldMember(pods, cs.Version) != nil
}

func pickOneOldMember(pods []*v1.Pod, newVersion string) *etcdutil.Member {
	for _, pod := range pods {
		if k8sutil.GetEtcdVersion(pod) == newVersion {
			continue
		}
		return &etcdutil.Member{Name: pod.Name, Namespace: pod.Namespace}
	}
	return nil
}