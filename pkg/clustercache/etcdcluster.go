package clustercache

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	api "github.com/Facecircccccle/etcd-operator/pkg/apis/cache/v1alpha1"
	"github.com/Facecircccccle/etcd-operator/pkg/util/etcdutil"
	"github.com/Facecircccccle/etcd-operator/pkg/util/k8sutil"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"reflect"
	"strings"
	"sync"
)

type MetaCluster struct {
	Logger  *logrus.Entry
	Name    string
	Cluster *api.Cluster
	Status api.ClusterStatus
	Members etcdutil.MemberSet
	config  Config

	tlsConfig *tls.Config
	eventsCli corev1.EventInterface
	Config map[string]string
}

type Config struct {
	KubeCli   kubernetes.Interface
}

type clusterEventType string
type clusterEvent struct {
	typ     clusterEventType
	cluster *api.Cluster
}

type MetaMap struct{
	sync.Map
}

func (c *MetaMap) Cache(cluster *api.Cluster) (*MetaCluster, bool) {
	meta, ok := c.Load(getNamespaceName(cluster.GetNamespace(), cluster.GetName()))
	if !ok{
		c.Add(cluster)
		return c.Get(cluster), false
	} else{
		c.Update(meta.(*MetaCluster), cluster)
		return c.Get(cluster), true
	}
}

func (c *MetaMap) Get(cluster *api.Cluster) *MetaCluster {
	meta,_ := c.Load(getNamespaceName(cluster.GetNamespace(), cluster.GetName()))
	return meta.(*MetaCluster)
}

func (c *MetaMap) Add(cluster *api.Cluster) {
	c.Store(getNamespaceName(cluster.GetNamespace(), cluster.GetName()), newCluster(cluster, clusterConfig()))
}

func (c *MetaMap) Update(meta *MetaCluster, cluster *api.Cluster) {
	err := meta.handleUpdateEvent(cluster)
	if err != nil {
		meta.Logger.Errorf("handle update event failed: %v", err)
		meta.Status.SetReason(err.Error())
		return
	}

}

func getNamespaceName(namespace, name string) string{
	return fmt.Sprintf("%s%c%s", namespace, '/', name)
}

func clusterConfig() Config {
	return Config{
		KubeCli: k8sutil.MustNewKubeClient(),
	}
}

func newCluster(cl *api.Cluster, config Config) *MetaCluster {
	lg := logrus.WithField("pkg", "cluster").WithField("cluster-name", cl.Name).WithField("cluster-namespace", cl.Namespace)
	if len(cl.Name) > k8sutil.MaxNameLength || len(cl.ClusterName) > k8sutil.MaxNameLength {
		return nil
	}

	c := &MetaCluster{
		Logger:    lg,
		Cluster:   cl,
		Status:    *(cl.Status.DeepCopy()),
		config:    config,
		eventsCli: config.KubeCli.CoreV1().Events(cl.Namespace),
	}
	return c
}

func (c *MetaCluster) Setup(h *EtcdClusterHandler) error {
	var shouldCreateCluster bool
	switch c.Status.Phase {
	case api.ClusterPhaseNone:
		shouldCreateCluster = true
	case api.ClusterPhaseCreating:
		return errCreatedCluster
	case api.ClusterPhaseRunning:
		shouldCreateCluster = false

	default:
		return fmt.Errorf("unexpected cluster phase: %s", c.Status.Phase)
	}

	if c.IsSecureClient() {
		d, err := k8sutil.GetTLSDataFromSecret(c.config.KubeCli, c.Cluster.Namespace, c.Cluster.Spec.TLS.Static.OperatorSecret)
		if err != nil {
			return err
		}
		c.tlsConfig, err = etcdutil.NewTLSConfig(d.CertData, d.KeyData, d.CAData)
		if err != nil {
			return err
		}
	}

	if shouldCreateCluster {
		return c.create(h)
	}
	return nil
}

func (c *MetaCluster) setupServices() error {
	err := k8sutil.CreateClientService(c.config.KubeCli, c.Cluster.Name, c.Cluster.Namespace, c.Cluster.AsOwner())
	if err != nil {
		return err
	}

	return k8sutil.CreatePeerService(c.config.KubeCli, c.Cluster.Name, c.Cluster.Namespace, c.Cluster.AsOwner())
}

func (c *MetaCluster) create(h *EtcdClusterHandler) error {
	c.Status.SetPhase(api.ClusterPhaseCreating)

	c.Cluster.SetDefaults()

	if err := c.UpdateCRStatus(h); err != nil {
		return fmt.Errorf("cluster create: failed to update cluster phase (%v): %v", api.ClusterPhaseCreating, err)
	}
	newCluster := c.Cluster
	newCluster.Status = c.Status
	c.Cluster = newCluster
	c.logClusterCreation()

	return c.prepareSeedMember()
}

func (c *MetaCluster) UpdateCRStatus(h *EtcdClusterHandler) error {
	if reflect.DeepEqual(c.Cluster.Status, c.Status) {
		return nil
	}

	newCluster := c.Cluster
	newCluster.Status = c.Status

	_ = h.K8sServices.UpdateCluster(c.Cluster.Namespace, c.Cluster)

	c.Cluster = newCluster

	return nil
}

func (c *MetaCluster) updateMemberStatus(running []*v1.Pod) {
	var unready []string
	var ready []string
	for _, pod := range running {
		if k8sutil.IsPodReady(pod) {
			ready = append(ready, pod.Name)
			continue
		}
		unready = append(unready, pod.Name)
	}

	c.Status.Members.Ready = ready
	c.Status.Members.Unready = unready
}

func (c *MetaCluster) logClusterCreation() {
	specBytes, err := json.MarshalIndent(c.Cluster.Spec, "", "    ")
	if err != nil {
		c.Logger.Errorf("failed to marshal cluster spec: %v", err)
	}

	c.Logger.Info("creating cluster with Spec:")
	for _, m := range strings.Split(string(specBytes), "\n") {
		c.Logger.Info(m)
	}
}

func (c *MetaCluster) prepareSeedMember() error {
	c.Status.SetScalingUpCondition(0, c.Cluster.Spec.Size)

	err := c.bootstrap()
	if err != nil {
		return err
	}

	c.Status.Size = 1
	return nil
}

// bootstrap creates the seed etcd member for a new Cluster.
func (c *MetaCluster) bootstrap() error {
	return c.startSeedMember()
}

func (c *MetaCluster) startSeedMember() error {
	m := &etcdutil.Member{
		Name:         k8sutil.UniqueMemberName(c.Cluster.Name),
		Namespace:    c.Cluster.Namespace,
		SecurePeer:   c.isSecurePeer(),
		SecureClient: c.IsSecureClient(),
	}
	if c.Cluster.Spec.Pod != nil {
		m.ClusterDomain = c.Cluster.Spec.Pod.ClusterDomain
	}
	ms := etcdutil.NewMemberSet(m)
	if err := c.createPod(ms, m, "new"); err != nil {
		return fmt.Errorf("failed to create seed member (%s): %v", m.Name, err)
	}
	c.Members = ms
	c.Logger.Infof("cluster created with seed member (%s)", m.Name)
	_, err := c.eventsCli.Create(context.TODO(), k8sutil.NewMemberAddEvent(m.Name, c.Cluster), metav1.CreateOptions{})
	if err != nil {
		c.Logger.Errorf("failed to create new member add event: %v", err)
	}

	return nil
}

func (c *MetaCluster) upgradeOneMember(memberName string) error {
	c.Status.SetUpgradingCondition(c.Cluster.Spec.Version)

	ns := c.Cluster.Namespace

	pod, err := c.config.KubeCli.CoreV1().Pods(ns).Get(context.TODO(), memberName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("fail to get pod (%s): %v", memberName, err)
	}
	oldpod := pod.DeepCopy()

	c.Logger.Infof("upgrading the etcd member %v from %s to %s", memberName, k8sutil.GetEtcdVersion(pod), c.Cluster.Spec.Version)
	pod.Spec.Containers[0].Image = k8sutil.ImageName(c.Cluster.Spec.Repository, c.Cluster.Spec.Version)
	k8sutil.SetEtcdVersion(pod, c.Cluster.Spec.Version)

	patchdata, err := k8sutil.CreatePatch(oldpod, pod, v1.Pod{})
	if err != nil {
		return fmt.Errorf("error creating patch: %v", err)
	}

	_, err = c.config.KubeCli.CoreV1().Pods(ns).Patch(context.TODO(), pod.GetName(), types.StrategicMergePatchType, patchdata, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("fail to update the etcd member (%s): %v", memberName, err)
	}
	c.Logger.Infof("finished upgrading the etcd member %v", memberName)
	_, err = c.eventsCli.Create(context.TODO(), k8sutil.MemberUpgradedEvent(memberName, k8sutil.GetEtcdVersion(oldpod), c.Cluster.Spec.Version, c.Cluster), metav1.CreateOptions{})
	if err != nil {
		c.Logger.Errorf("failed to create member upgraded event: %v", err)
	}

	return nil
}

func (c *MetaCluster) createPod(members etcdutil.MemberSet, m *etcdutil.Member, state string) error {
	pod := k8sutil.NewEtcdPod(m, members.PeerURLPairs(), c.Cluster.Name, state, uuid.New(), c.Cluster.Spec, c.Cluster.AsOwner())
	if c.isPodPVEnabled() {
		pvc := k8sutil.NewEtcdPodPVC(m, *c.Cluster.Spec.Pod.PersistentVolumeClaimSpec, c.Cluster.Name, c.Cluster.Namespace, c.Cluster.AsOwner())
		_, err := c.config.KubeCli.CoreV1().PersistentVolumeClaims(c.Cluster.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create PVC for member (%s): %v", m.Name, err)
		}
		k8sutil.AddEtcdVolumeToPod(pod, pvc)
	} else {
		k8sutil.AddEtcdVolumeToPod(pod, nil)
	}
	_, err := c.config.KubeCli.CoreV1().Pods(c.Cluster.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	return err
}
func (c *MetaCluster) removePod(name string) error {
	ns := c.Cluster.Namespace
	err := c.config.KubeCli.CoreV1().Pods(ns).Delete(context.TODO(), name, metav1.DeleteOptions{})//metav1.NewDeleteOptions(podTerminationGracePeriod))
	if err != nil {
		if !k8sutil.IsKubernetesResourceNotFoundError(err) {
			return err
		}
	}
	return nil
}

func (c *MetaCluster) isPodPVEnabled() bool {
	if podPolicy := c.Cluster.Spec.Pod; podPolicy != nil {
		return podPolicy.PersistentVolumeClaimSpec != nil
	}
	return false
}

func (c *MetaCluster) isSecurePeer() bool {
	return c.Cluster.Spec.TLS.IsSecurePeer()
}

func (c *MetaCluster) IsSecureClient() bool {
	return c.Cluster.Spec.TLS.IsSecureClient()
}


func (c *MetaCluster) handleUpdateEvent(cl *api.Cluster) error {
	oldSpec := c.Cluster.Spec.DeepCopy()
	c.Cluster = cl

	if isSpecEqual(cl.Spec, *oldSpec) {
		if !reflect.DeepEqual(cl.Spec, *oldSpec) {
			c.Logger.Infof("ignoring update event: %#v", cl.Spec)
		}
		return nil
	}
	// TODO: we can't handle another upgrade while an upgrade is in progress

	c.logSpecUpdate(*oldSpec, cl.Spec)
	return nil
}


func isSpecEqual(s1, s2 api.ClusterSpec) bool {
	if s1.Size != s2.Size || s1.Paused != s2.Paused || s1.Version != s2.Version {
		return false
	}
	return true
}
func (c *MetaCluster) logSpecUpdate(oldSpec, newSpec api.ClusterSpec) {
	oldSpecBytes, err := json.MarshalIndent(oldSpec, "", "    ")
	if err != nil {
		c.Logger.Errorf("failed to marshal cluster spec: %v", err)
	}
	newSpecBytes, err := json.MarshalIndent(newSpec, "", "    ")
	if err != nil {
		c.Logger.Errorf("failed to marshal cluster spec: %v", err)
	}

	c.Logger.Infof("spec update: Old Spec:")
	for _, m := range strings.Split(string(oldSpecBytes), "\n") {
		c.Logger.Info(m)
	}

	c.Logger.Infof("New Spec:")
	for _, m := range strings.Split(string(newSpecBytes), "\n") {
		c.Logger.Info(m)
	}

}

func (c *MetaCluster) PollPods() (running, pending []*v1.Pod, err error) {
	podList, err := c.config.KubeCli.CoreV1().Pods(c.Cluster.Namespace).List(context.TODO(), k8sutil.ClusterListOpt(c.Cluster.Name))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list running pods: %v", err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		// Avoid polling deleted pods. k8s issue where deleted pods would sometimes show the status Pending
		// See https://github.com/coreos/etcd-operator/issues/1693
		if pod.DeletionTimestamp != nil {
			continue
		}
		if len(pod.OwnerReferences) < 1 {
			c.Logger.Warningf("pollPods: ignore pod %v: no owner", pod.Name)
			continue
		}
		if pod.OwnerReferences[0].UID != c.Cluster.UID {
			c.Logger.Warningf("pollPods: ignore pod %v: owner (%v) is not %v",
				pod.Name, pod.OwnerReferences[0].UID, c.Cluster.UID)
			continue
		}
		switch pod.Status.Phase {
		case v1.PodRunning:
			running = append(running, pod)
		case v1.PodPending:
			pending = append(pending, pod)
		}
	}

	return running, pending, nil
}