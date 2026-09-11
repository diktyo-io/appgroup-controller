/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"
	"k8s.io/klog/v2"
	"os"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apiserver/pkg/server"
	coreinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	clientset "github.com/diktyo-io/appgroup-api/pkg/generated/clientset/versioned"
	informers "github.com/diktyo-io/appgroup-api/pkg/generated/informers/externalversions"
	controller "github.com/diktyo-io/appgroup-controller/pkg/controller"
)

func newConfig(kubeconfig, master string, inCluster bool) (*restclient.Config, error) {
	var (
		config *rest.Config
		err    error
	)
	if inCluster {
		config, err = rest.InClusterConfig()
	} else {
		config, err = clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	if err != nil {
		return nil, err
	}
	return config, nil
}

func Run(s *ServerRunOptions) error {
	ctx := context.Background()
	config, err := newConfig(s.KubeConfig, s.MasterUrl, s.InCluster)
	if err != nil {
		klog.ErrorS(err, "Failed to parse config")
		os.Exit(1)
	}
	config.QPS = float32(s.ApiServerQPS)
	config.Burst = s.ApiServerBurst
	stopCh := server.SetupSignalHandler()
	client := clientset.NewForConfigOrDie(config)
	kubeClient := kubernetes.NewForConfigOrDie(config)

	agInformerFactory := informers.NewSharedInformerFactory(client, 0)
	agInformer := agInformerFactory.Appgroup().V1alpha1().AppGroups()

	coreInformerFactory := coreinformers.NewSharedInformerFactory(kubeClient, 0)
	podInformer := coreInformerFactory.Core().V1().Pods()
	agCtrl := controller.NewAppGroupController(kubeClient, agInformer, podInformer, client)

	run := func(ctx context.Context) {
		go agCtrl.Run(s.Workers, ctx.Done())
		select {}
	}
	agInformerFactory.Start(stopCh)
	coreInformerFactory.Start(stopCh)
	if !s.EnableLeaderElection {
		run(ctx)
	} else {
		id, err := os.Hostname()
		if err != nil {
			return err
		}
		// add a uniquifier so that two processes on the same host don't accidentally both become active
		id = id + "_" + string(uuid.NewUUID())

		rl, err := resourcelock.New("endpoints",
			"kube-system",
			"appgroup-controller",
			kubeClient.CoreV1(),
			kubeClient.CoordinationV1(),
			resourcelock.ResourceLockConfig{
				Identity: id,
			})
		if err != nil {
			klog.ErrorS(err, "Resource lock creation failed")
			os.Exit(1)
		}

		leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
			Lock: rl,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: run,
				OnStoppedLeading: func() {
					klog.ErrorS(err, "Leaderelection lost")
					os.Exit(1)
				},
			},
			Name: "app-group controller",
		})
	}

	<-stopCh
	return nil
}
