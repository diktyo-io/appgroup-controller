/*
Copyright 2017 The Kubernetes Authors.

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

package appgroup

import (
	"context"
	"fmt"
	"github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
	"github.com/diktyo-io/appgroup-controller/util"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	klog "k8s.io/klog/v2"
	"reflect"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformer "k8s.io/client-go/informers/core/v1"

	clientset "github.com/diktyo-io/appgroup-api/pkg/generated/clientset/versioned"
	appgroupinformers "github.com/diktyo-io/appgroup-api/pkg/generated/informers/externalversions/appgroup/v1alpha1"
	appgrouplisters "github.com/diktyo-io/appgroup-api/pkg/generated/listers/appgroup/v1alpha1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const (
	timeLimitation = 48 * time.Hour

	// podEventDebounce is the coalescing window applied to pod Add/Update/Delete
	// events before reconciling an AppGroup.  When N pods bind in a short burst
	// (the common case for a batch workload) every pod fires an event that would
	// otherwise trigger an immediate AppGroup status PATCH.  By using
	// agQueue.AddAfter instead of agQueue.Add, events that arrive within this
	// window for the same AppGroup key are deduplicated by the workqueue, so the
	// controller issues O(1) PATCHes per burst window instead of O(N) — directly
	// reducing the AppGroup-update event volume seen by the scheduler plugins.
	podEventDebounce = 1 * time.Second
)

// AppGroupController : a controller that processes App groups using provided Handler interface
type AppGroupController struct {
	eventRecorder   record.EventRecorder
	agQueue         workqueue.RateLimitingInterface
	agLister        appgrouplisters.AppGroupLister
	podLister       corelister.PodLister
	agListerSynced  cache.InformerSynced
	podListerSynced cache.InformerSynced
	agClient        clientset.Interface
}

// NewAppGroupController : returns a new *AppGroupController
func NewAppGroupController(client kubernetes.Interface,
	agInformer appgroupinformers.AppGroupInformer,
	podInformer coreinformer.PodInformer,
	agClient clientset.Interface) *AppGroupController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})

	ctrl := &AppGroupController{
		eventRecorder: broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "AppGroupController"}),
		agQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "AppGroup"),
	}

	klog.V(5).InfoS("Setting up AppGroup event handlers")
	agInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.agAdded,
		UpdateFunc: ctrl.agUpdated,
		DeleteFunc: ctrl.agDeleted,
	})

	klog.V(5).InfoS("Setting up Pod event handlers")
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.podAdded,
		UpdateFunc: ctrl.podUpdated,
		DeleteFunc: ctrl.podDeleted,
	})

	ctrl.agLister = agInformer.Lister()
	ctrl.podLister = podInformer.Lister()
	ctrl.agListerSynced = agInformer.Informer().HasSynced
	ctrl.podListerSynced = podInformer.Informer().HasSynced
	ctrl.agClient = agClient
	return ctrl
}

// Run : starts listening on channel events
func (ctrl *AppGroupController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.agQueue.ShutDown()

	klog.InfoS("[Init] Starting App Group controller")
	defer klog.InfoS("Shutting App Group controller")

	if !cache.WaitForCacheSync(stopCh, ctrl.agListerSynced, ctrl.podListerSynced) {
		klog.Error("Cannot sync caches")
		return
	}
	klog.InfoS("[Init] App Group sync finished")
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.worker, time.Second, stopCh)
	}

	<-stopCh
}

// agAdded : reacts to a AppGroup creation
func (ctrl *AppGroupController) agAdded(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	ag := obj.(*v1alpha1.AppGroup)

	// If startScheduleTime - createTime > 2days, do not enqueue again because pod may have been GCed
	if ag.Status.RunningWorkloads == 0 &&
		ag.Status.ScheduleStartTime.Sub(ag.CreationTimestamp.Time) > timeLimitation {
		return
	}

	klog.V(5).InfoS("Enqueue AppGroup ", "app group", key)
	ctrl.agQueue.Add(key)
}

// agUpdated : reacts to a AppGroup update
func (ctrl *AppGroupController) agUpdated(old, new interface{}) {
	ctrl.agAdded(new)
}

// agDeleted : reacts to a AppGroup deletion
func (ctrl *AppGroupController) agDeleted(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.V(5).InfoS("Enqueue deleted app group key", "appGroup", key)
	ctrl.agQueue.AddRateLimited(key)
}

// podAdded : reacts to a pod creation
func (ctrl *AppGroupController) podAdded(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}
	ctrl.enqueuePodAppGroup(pod)
}

// podDeleted : reacts to a pod deletion
func (ctrl *AppGroupController) podDeleted(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		// The watch cache may deliver a tombstone when the controller missed
		// the original delete event.  Extract the Pod from it before proceeding.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			runtime.HandleError(fmt.Errorf("tombstone contained object that is not a Pod %#v", obj))
			return
		}
	}
	ctrl.enqueuePodAppGroup(pod)
}

// podUpdated : reacts to a pod update
func (ctrl *AppGroupController) podUpdated(old, new interface{}) {
	ctrl.podAdded(new)
}

// enqueuePodAppGroup resolves the AppGroup that owns pod and schedules a
// debounced reconciliation via agQueue.AddAfter.  Using AddAfter instead of
// Add means that N pod-bind events arriving within podEventDebounce for the
// same AppGroup key are collapsed by the workqueue's own deduplication into a
// single reconciliation — and therefore a single status PATCH — rather than
// one PATCH per pod.  AppGroup-level events (agAdded/agUpdated) still use the
// immediate agQueue.Add path so that structural changes (topology, spec) are
// never delayed.
func (ctrl *AppGroupController) enqueuePodAppGroup(pod *v1.Pod) {
	agName := util.GetPodAppGroupLabel(pod)
	if len(agName) == 0 {
		return
	}
	ag, err := ctrl.agLister.AppGroups(pod.Namespace).Get(agName)
	if err != nil {
		klog.ErrorS(err, "Error while looking up AppGroup for pod", "pod", klog.KObj(pod))
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(ag)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.V(5).InfoS("Enqueue AppGroup for pod event (debounced)",
		"AppGroup", klog.KObj(ag), "pod", klog.KObj(pod), "delay", podEventDebounce)
	ctrl.agQueue.AddAfter(key, podEventDebounce)
}

func (ctrl *AppGroupController) worker() {
	for ctrl.processNextWorkItem() {
	}
}

// processNextWorkItem : deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *AppGroupController) processNextWorkItem() bool {
	keyObj, quit := ctrl.agQueue.Get()
	if quit {
		return false
	}
	defer ctrl.agQueue.Done(keyObj)

	key, ok := keyObj.(string)
	if !ok {
		ctrl.agQueue.Forget(keyObj)
		runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", keyObj))
		return true
	}
	if err := ctrl.syncHandler(key); err != nil {
		runtime.HandleError(err)
		klog.ErrorS(err, "Error syncing app group", "appGroup", key)
		return true
	}

	return true
}

// syncHandler : syncs AppGroup and converts the status
func (ctrl *AppGroupController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	defer func() {
		if err != nil {
			ctrl.agQueue.AddRateLimited(key)
			return
		}
	}()
	ag, err := ctrl.agLister.AppGroups(namespace).Get(name)
	if apierrs.IsNotFound(err) {
		klog.V(5).InfoS("App group has been deleted", "appGroup", key)
		return nil
	}
	if err != nil {
		klog.V(3).ErrorS(err, "Unable to retrieve app group from store", "appGroup", key)
		return err
	}

	agCopy := ag.DeepCopy()
	selector := labels.Set(map[string]string{v1alpha1.AppGroupLabel: agCopy.Name}).AsSelector()

	pods, err := ctrl.podLister.List(selector)
	if err != nil {
		klog.ErrorS(err, "List pods for App group failed", "AppGroup", klog.KObj(agCopy))
		return err
	}

	// Update Status of AppGroup CRD
	// Running Workloads
	var numWorkloadsRunning int32 = 0
	if len(pods) != 0 {
		for _, pod := range pods {
			switch pod.Status.Phase {
			case v1.PodRunning:
				numWorkloadsRunning++
			}
		}
	}

	agCopy.Status.RunningWorkloads = numWorkloadsRunning
	klog.Info("[Sync] RunningWorkloads: ", numWorkloadsRunning)

	if agCopy.Status.TopologyCalculationTime.IsZero() {
		klog.InfoS("[Sync] Initial Calculation of Topology order...")
		agCopy.Status.TopologyOrder, err = calculateTopologyOrder(agCopy, agCopy.Spec.TopologySortingAlgorithm, agCopy.Spec.Workloads, err)
		if err != nil {
			klog.InfoS("[Sync] Error Calculating Topology order, application does not reflect a DAG...", "appGroup", key)
			agCopy.Status.TopologyOrder = defaultTopologyOrder(agCopy.Spec.Workloads)
		}
		agCopy.Status.TopologyCalculationTime = metav1.Time{Time: time.Now()}

		klog.InfoS("[Sync] Calculation of AppGroup Statistics...")
		agCopy.Status.AppGroupStatistics = calculateAppGroupStatistics(agCopy.Status.TopologyOrder)

	} else if time.Now().Sub(ag.Status.TopologyCalculationTime.Time) > 24*time.Hour {
		klog.InfoS("[Sync] Recalculation of Topology Order... Every 24 hours...")
		agCopy.Status.TopologyOrder, err = calculateTopologyOrder(agCopy, agCopy.Spec.TopologySortingAlgorithm, agCopy.Spec.Workloads, err)
		if err != nil {
			klog.InfoS("[Sync] Error Calculating Topology order, application does not reflect a DAG...", "appGroup", key)
			agCopy.Status.TopologyOrder = defaultTopologyOrder(agCopy.Spec.Workloads)
		}
		agCopy.Status.TopologyCalculationTime = metav1.Time{Time: time.Now()}

		klog.InfoS("[Sync] Calculation of AppGroup Statistics...")
		agCopy.Status.AppGroupStatistics = calculateAppGroupStatistics(agCopy.Status.TopologyOrder)
	}
	klog.V(5).InfoS("[Sync] ag to patch", "ag", agCopy)

	err = ctrl.patchAppGroup(ag, agCopy)
	if err == nil {
		ctrl.agQueue.Forget(ag)
	}
	return err
}

// patchAppGroup : patches the new info to the AppGroup
func (ctrl *AppGroupController) patchAppGroup(old, new *v1alpha1.AppGroup) error {
	if reflect.DeepEqual(old, new) {
		return nil
	}

	patch, err := util.CreateMergePatch(old, new)
	if err != nil {
		return err
	}

	_, err = ctrl.agClient.AppgroupV1alpha1().AppGroups(old.Namespace).Patch(context.TODO(), old.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// calculateTopologyOrder : calculates the correct sequence order for workload deployment based on the selected sorting algorithm
func calculateTopologyOrder(agCopy *v1alpha1.AppGroup, algorithm string, workloadList v1alpha1.AppGroupWorkloadList, err error) (v1alpha1.AppGroupTopologyList, error) {

	var order []string
	var topologyList v1alpha1.AppGroupTopologyList
	tree := map[string][]string{}
	minCostTree := map[string]int64{}
	avgCostTree := map[string]int64{}
	maxCostTree := map[string]int64{}

	minBandwidthTree := map[string]resource.Quantity{}
	avgBandwidthTree := map[string]resource.Quantity{}
	maxBandwidthTree := map[string]resource.Quantity{}

	for _, w := range workloadList {
		if w.Dependencies == nil { // no dependencies
			minCostTree[w.Workload.Name] = 0
			avgCostTree[w.Workload.Name] = 0
			maxCostTree[w.Workload.Name] = 0

			minBandwidthTree[w.Workload.Name] = *resource.NewQuantity(0, resource.DecimalSI)
			avgBandwidthTree[w.Workload.Name] = *resource.NewQuantity(0, resource.DecimalSI)
			maxBandwidthTree[w.Workload.Name] = *resource.NewQuantity(0, resource.DecimalSI)
		} else {
			var sum_c, max_c, min_c, num int64

			min_c = 10000
			sum_b := *resource.NewQuantity(0, resource.DecimalSI)
			max_b := *resource.NewQuantity(0, resource.DecimalSI)
			min_b := *resource.NewQuantity(1000000000, resource.DecimalSI)

			for _, dependency := range w.Dependencies {
				tree[w.Workload.Name] = append(tree[w.Workload.Name], dependency.Workload.Name)

				num += 1
				// update maximum
				if max_c < dependency.MaxNetworkCost {
					max_c = dependency.MaxNetworkCost
				}
				// update minimum
				if min_c > dependency.MaxNetworkCost {
					min_c = dependency.MaxNetworkCost
				}
				sum_c += dependency.MaxNetworkCost

				// update maximum
				if max_b.Cmp(dependency.MinBandwidth) == -1 {
					max_b = dependency.MinBandwidth
				}
				// update minimum
				if min_b.Cmp(dependency.MinBandwidth) == 1 {
					min_b = dependency.MinBandwidth
				}
				sum_b.Add(dependency.MinBandwidth)
			}

			minCostTree[w.Workload.Name] = min_c
			avgCostTree[w.Workload.Name] = sum_c / num
			maxCostTree[w.Workload.Name] = max_c

			minBandwidthTree[w.Workload.Name] = min_b
			avgBandwidthTree[w.Workload.Name] = *resource.NewQuantity(sum_b.Value()/num, resource.DecimalSI)
			maxBandwidthTree[w.Workload.Name] = max_b
		}
	}

	klog.Info("[calculateTopologyOrder] Service Dependency Tree: ", tree)

	klog.Info("[calculateTopologyOrder] Min Cost Dependency Tree: ", minCostTree)
	klog.Info("[calculateTopologyOrder] Avg Cost Dependency Tree: ", avgCostTree)
	klog.Info("[calculateTopologyOrder] Max Cost Dependency Tree: ", maxCostTree)

	klog.Info("[calculateTopologyOrder] Min Bandwidth Dependency Tree: ", minBandwidthTree)
	klog.Info("[calculateTopologyOrder] Avg Bandwidth Dependency Tree: ", avgBandwidthTree)
	klog.Info("[calculateTopologyOrder] Max Bandwidth Dependency Tree: ", maxBandwidthTree)

	// Calculate order based on the specified algorithm
	type sortImpl func(tree map[string][]string) ([]string, error)
	sortingAlgorithms := map[string]sortImpl{
		v1alpha1.AppGroupKahnSort:        util.KahnSort,
		v1alpha1.AppGroupTarjanSort:      util.TarjanSort,
		v1alpha1.AppGroupReverseKahn:     util.ReverseKahn,
		v1alpha1.AppGroupReverseTarjan:   util.ReverseTarjan,
		v1alpha1.AppGroupAlternateKahn:   util.AlternateKahn,
		v1alpha1.AppGroupAlternateTarjan: util.AlternateTarjan,
	}

	sortAlg, exists := sortingAlgorithms[algorithm]
	if !exists {
		klog.Infof("[calculateTopologyOrder] Sorting algorithm not identified: %v. Defaulting to %v", agCopy.Spec.TopologySortingAlgorithm, v1alpha1.AppGroupKahnSort)
		// Default: consider KahnSort algorithm
		sortAlg = util.KahnSort
	} else {
		klog.InfoS("[calculateTopologyOrder] Sorting algorithm identified", "algorithm", algorithm)
	}
	// Get TopologyList
	order, err = sortAlg(tree)
	if err != nil {
		klog.ErrorS(err, "[calculateTopologyOrder] Sorting failed", "algorithm", sortAlg, "AppGroup", klog.KObj(agCopy))
		return topologyList, err
	}

	// Sort workload data by Selector
	sort.Sort(util.BySelector(agCopy.Spec.Workloads))

	klog.InfoS("[calculateTopologyOrder] Sorting workloads", "order", order)

	for id, workloadName := range order {
		index := int32(id + 1)
		// Find workload data by name (Binary Search)
		w := util.FindWorkloadBySelector(agCopy.Spec.Workloads, workloadName)
		if w == nil {
			err := fmt.Errorf("[calculateTopologyOrder] Unable to find workload by name... ")
			klog.Error(err, "[calculateTopologyOrder] Binary search failed")
			return topologyList, err
		}

		topologyList = append(topologyList, v1alpha1.AppGroupTopologyInfo{
			Workload: *w,
			Index:    index,
			WorkloadStatistics: v1alpha1.AppGroupStatistics{
				MinBandwidth: minBandwidthTree[w.Name],
				AvgBandwidth: avgBandwidthTree[w.Name],
				MaxBandwidth: maxBandwidthTree[w.Name],
				MinCost:      minCostTree[w.Name],
				AvgCost:      avgCostTree[w.Name],
				MaxCost:      maxCostTree[w.Name],
			},
		})
	}

	// Sort TopologyList by Selector
	klog.Infof("[calculateTopologyOrder] Sort Topology List by Workload.Selector... ")
	sort.Sort(util.ByWorkloadSelector(topologyList))

	klog.InfoS("[calculateTopologyOrder] Sorted list", "topologyList", topologyList)
	return topologyList, nil
}

// defaultTopologyOrder : retrieves a default sequence order for workload deployment
func defaultTopologyOrder(workloadList v1alpha1.AppGroupWorkloadList) v1alpha1.AppGroupTopologyList {
	var topologyList v1alpha1.AppGroupTopologyList

	minCostTree := map[string]int64{}
	avgCostTree := map[string]int64{}
	maxCostTree := map[string]int64{}

	minBandwidthTree := map[string]resource.Quantity{}
	avgBandwidthTree := map[string]resource.Quantity{}
	maxBandwidthTree := map[string]resource.Quantity{}

	for _, w := range workloadList {
		if w.Dependencies == nil { // no dependencies

			minCostTree[w.Workload.Name] = 0
			avgCostTree[w.Workload.Name] = 0
			maxCostTree[w.Workload.Name] = 0

			minBandwidthTree[w.Workload.Name] = *resource.NewQuantity(0, resource.DecimalSI)
			avgBandwidthTree[w.Workload.Name] = *resource.NewQuantity(0, resource.DecimalSI)
			maxBandwidthTree[w.Workload.Name] = *resource.NewQuantity(0, resource.DecimalSI)

		} else {
			var sum_c, max_c, min_c, num int64

			min_c = 10000
			sum_b := *resource.NewQuantity(0, resource.DecimalSI)
			max_b := *resource.NewQuantity(0, resource.DecimalSI)
			min_b := *resource.NewQuantity(1000000000, resource.DecimalSI)

			for _, dependency := range w.Dependencies {
				num += 1

				// update maximum
				if max_c < dependency.MaxNetworkCost {
					max_c = dependency.MaxNetworkCost
				}
				// update minimum
				if min_c > dependency.MaxNetworkCost {
					min_c = dependency.MaxNetworkCost
				}
				sum_c += dependency.MaxNetworkCost

				// update maximum
				if max_b.Cmp(dependency.MinBandwidth) == -1 {
					max_b = dependency.MinBandwidth
				}
				// update minimum
				if min_b.Cmp(dependency.MinBandwidth) == 1 {
					min_b = dependency.MinBandwidth
				}
				sum_b.Add(dependency.MinBandwidth)
			}

			minCostTree[w.Workload.Name] = min_c
			avgCostTree[w.Workload.Name] = sum_c / num
			maxCostTree[w.Workload.Name] = max_c

			minBandwidthTree[w.Workload.Name] = min_b
			avgBandwidthTree[w.Workload.Name] = *resource.NewQuantity(sum_b.Value()/num, resource.DecimalSI)
			maxBandwidthTree[w.Workload.Name] = max_b
		}
	}

	klog.Info("[calculateTopologyOrder] Min Cost Dependency Tree: ", minCostTree)
	klog.Info("[calculateTopologyOrder] Avg Cost Dependency Tree: ", avgCostTree)
	klog.Info("[calculateTopologyOrder] Max Cost Dependency Tree: ", maxCostTree)

	klog.Info("[calculateTopologyOrder] Min Bandwidth Dependency Tree: ", minBandwidthTree)
	klog.Info("[calculateTopologyOrder] Avg Bandwidth Dependency Tree: ", avgBandwidthTree)
	klog.Info("[calculateTopologyOrder] Max Bandwidth Dependency Tree: ", maxBandwidthTree)

	for i, w := range workloadList {
		topologyList = append(topologyList, v1alpha1.AppGroupTopologyInfo{
			Workload: w.Workload,
			Index:    int32(i + 1),
			WorkloadStatistics: v1alpha1.AppGroupStatistics{
				MinBandwidth: minBandwidthTree[w.Workload.Name],
				AvgBandwidth: avgBandwidthTree[w.Workload.Name],
				MaxBandwidth: maxBandwidthTree[w.Workload.Name],
				MinCost:      minCostTree[w.Workload.Name],
				AvgCost:      avgCostTree[w.Workload.Name],
				MaxCost:      maxCostTree[w.Workload.Name],
			},
		})
	}

	// Sort TopologyList by Selector
	klog.Infof("[defaultOrder] Sort Topology List by Workload.Selector... ")
	sort.Sort(util.ByWorkloadSelector(topologyList))

	klog.InfoS("[defaultOrder] Sorted list", "topologyList", topologyList)
	return topologyList
}

// calculateAppGroupStatistics : calculates the AppGroup Statistics
func calculateAppGroupStatistics(topOrder v1alpha1.AppGroupTopologyList) v1alpha1.AppGroupStatistics {
	var appGroupStats v1alpha1.AppGroupStatistics

	if len(topOrder) == 0 {
		return appGroupStats
	}

	var sum_c, max_c, min_c, num int64

	min_c = 10000
	sum_b := *resource.NewQuantity(0, resource.DecimalSI)
	max_b := *resource.NewQuantity(0, resource.DecimalSI)
	min_b := *resource.NewQuantity(1000000000, resource.DecimalSI)

	for _, w := range topOrder {
		klog.InfoS("[calculateAppGroupStatistics] WorkloadStatistics", "w", w.WorkloadStatistics)

		num += 1
		// update maximum
		if max_c < w.WorkloadStatistics.MaxCost {
			max_c = w.WorkloadStatistics.MaxCost
		}
		// update minimum
		if min_c > w.WorkloadStatistics.MinCost {
			min_c = w.WorkloadStatistics.MinCost
		}
		sum_c += w.WorkloadStatistics.AvgCost

		// update maximum
		if max_b.Cmp(w.WorkloadStatistics.MaxBandwidth) == -1 {
			max_b = w.WorkloadStatistics.MaxBandwidth
		}
		// update minimum
		if min_b.Cmp(w.WorkloadStatistics.MinBandwidth) == 1 {
			min_b = w.WorkloadStatistics.MinBandwidth
		}
		sum_b.Add(w.WorkloadStatistics.AvgBandwidth)
	}

	appGroupStats = v1alpha1.AppGroupStatistics{
		MinBandwidth: min_b,
		AvgBandwidth: *resource.NewQuantity(sum_b.Value()/num, resource.DecimalSI),
		MaxBandwidth: max_b,
		MinCost:      min_c,
		AvgCost:      sum_c / num,
		MaxCost:      max_c,
	}
	klog.InfoS("[calculateAppGroupStatistics] ", "AppGroupStats", appGroupStats)

	return appGroupStats
}
