package clustercache



import (
	"fmt"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"

	"github.com/Facecircccccle/etcd-operator/pkg/util/etcdutil"
	"github.com/Facecircccccle/etcd-operator/pkg/util/k8sutil"
	"github.com/pkg/errors"

	"k8s.io/api/core/v1"
)

func (c *MetaCluster) UpdateMembers(known etcdutil.MemberSet) error {
	resp, err := etcdutil.ListMembers(known.ClientURLs(), c.tlsConfig)
	if err != nil {
		return err
	}
	members := etcdutil.MemberSet{}
	for _, m := range resp.Members {
		name, err := getMemberName(m, c.Cluster.GetName())
		if err != nil {
			return errors.Wrap(err, "get member name failed")
		}

		members[name] = &etcdutil.Member{
			Name:         name,
			Namespace:    c.Cluster.Namespace,
			ID:           m.ID,
			SecurePeer:   c.isSecurePeer(),
			SecureClient: c.IsSecureClient(),
		}
	}
	c.Members = members
	return nil
}

func (c *MetaCluster) newMember() *etcdutil.Member {
	name := k8sutil.UniqueMemberName(c.Cluster.Name)
	m := &etcdutil.Member{
		Name:         name,
		Namespace:    c.Cluster.Namespace,
		SecurePeer:   c.isSecurePeer(),
		SecureClient: c.IsSecureClient(),
	}

	if c.Cluster.Spec.Pod != nil {
		m.ClusterDomain = c.Cluster.Spec.Pod.ClusterDomain
	}
	return m
}

func PodsToMemberSet(pods []*v1.Pod, sc bool) etcdutil.MemberSet {
	members := etcdutil.MemberSet{}
	for _, pod := range pods {
		m := &etcdutil.Member{Name: pod.Name, Namespace: pod.Namespace, SecureClient: sc}
		members.Add(m)
	}
	return members
}

func getMemberName(m *etcdserverpb.Member, clusterName string) (string, error) {
	name, err := etcdutil.MemberNameFromPeerURL(m.PeerURLs[0])
	if err != nil {
		return "", newFatalError(fmt.Sprintf("invalid member peerURL (%s): %v", m.PeerURLs[0], err))
	}
	return name, nil
}
