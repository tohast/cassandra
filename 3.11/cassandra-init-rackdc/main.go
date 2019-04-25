package main

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"strings"
)

func main() {

	nodename := os.Getenv("NODE_NAME")
	if len(nodename) == 0 {
		panic("Environment variable NODE_NAME is empty! NODE_NAME should be set to Kubernetes node name.")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	nodes, err := clientset.CoreV1().Nodes().List(
		metav1.ListOptions{
			FieldSelector: "metadata.name=" + nodename,
		})
	if err != nil {
		panic(err.Error())
	}
	if len(nodes.Items) > 1 {
		panic("More than 1 Kubernetes Node with name " + nodename + " has been returned! Should be exactly one.")
	}
	if len(nodes.Items) == 0 {
		panic("No Kubernetes Node with name " + nodename + " has been found!")
	}
	var region string
	var zone string
	for _, n := range nodes.Items {
		var exist bool
		region, exist = n.ObjectMeta.Labels["failure-domain.beta.kubernetes.io/region"]
		if !exist {
			panic("No region label ('failure-domain.beta.kubernetes.io/region') has been found for Node[name=" + nodename+ "]. Found labels: " + concatLabels(n.ObjectMeta.Labels))
		}
		zone, exist = n.ObjectMeta.Labels["failure-domain.beta.kubernetes.io/zone"]
		if !exist {
			panic("No zone label ('failure-domain.beta.kubernetes.io/zone') has been found for Node[name=" + nodename+ "]. Found labels: " + concatLabels(n.ObjectMeta.Labels))
		}
	}
	f, err := os.Create("/config/set-env-vars.sh")
	defer f.Close()
	if err != nil {
		panic(err.Error())
	}
	f.WriteString("export CASSANDRA_DC=" + region + "\n")
	f.WriteString("export CASSANDRA_RACK=" + zone + "\n")
	f.Sync()
}

func concatLabels(l map[string]string) string {
	kvpairs := make([]string, 0)
	var kv [2]string
	kvs := kv[:]
	for k, v := range l {
		kv[0] = k
		kv[1] = v
		kvpairs = append(kvpairs, strings.Join(kvs, ":"))
	}
	return "{" + strings.Join(kvpairs, ",") + "}"
}
