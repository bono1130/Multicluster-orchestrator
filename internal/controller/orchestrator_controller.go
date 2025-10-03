package controller

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	uobj "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	orchestrationv1alpha1 "github.com/wnguddn777/multicluster-orchestrator/api/v1alpha1"
)

const (
	fieldManager          = "orchestrator"
	orchestratorFinalizer = "orchestration.operator.io/finalizer"
)

// ──────────────────────────────────────────────────────────────
// Common label for all orchestrator-managed resources
func applyCommonLabel(u *uobj.Unstructured, event string) {
	if u == nil {
		return
	}
	obj := u.Object
	md, _ := obj["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		obj["metadata"] = md
	}
	labels, _ := md["labels"].(map[string]any)
	if labels == nil {
		labels = map[string]any{}
		md["labels"] = labels
	}
	labels["orchestrator.operator.io/event"] = event
}

// ──────────────────────────────────────────────────────────────
// Reconciler
// ──────────────────────────────────────────────────────────────

type OrchestratorReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	PromURL string // (미사용; PD 컨트롤러에서 사용)
}

// +kubebuilder:rbac:groups=orchestration.operator.io,resources=orchestrators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=orchestrators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestration.operator.io,resources=orchestrators/finalizers,verbs=update
// +kubebuilder:rbac:groups=argoproj.io,resources=eventsources;sensors;workflowtemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=serving.knative.dev,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.karmada.io,resources=propagationpolicies;overridepolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.karmada.io,resources=clusterpropagationpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;services;endpoints,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=argoproj.io,resources=eventbus;eventbuses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=work.karmada.io,resources=resourcebindings;clusterresourcebindings;works,verbs=get;list;watch;delete

// ──────────────────────────────────────────────────────────────
// cluster → endpoint 매핑(ConfigMap: orchestrator-domain-map in spec.namespace)
//   <cluster>.ip:       "192.168.10.x"
//   <cluster>.kourier:  "31863" (NodePort)
// ──────────────────────────────────────────────────────────────

type clusterEndpoint struct {
	IP   string
	Port int32
}

func (r *OrchestratorReconciler) loadDomainMap(ctx context.Context, ns string) (map[string]clusterEndpoint, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: "orchestrator-domain-map"}, cm); err != nil {
		return nil, err
	}

	tmp := map[string]struct {
		ip   string
		port *int32
	}{}

	for k, v := range cm.Data {
		parts := strings.SplitN(k, ".", 2)
		if len(parts) != 2 {
			continue
		}
		name, suffix := parts[0], parts[1]
		ent := tmp[name]
		switch suffix {
		case "ip":
			ent.ip = v
		case "kourier":
			if p, err := strconv.Atoi(v); err == nil {
				pp := int32(p)
				ent.port = &pp
			}
		}
		tmp[name] = ent
	}

	out := map[string]clusterEndpoint{}
	for name, ent := range tmp {
		port := int32(31370) // fallback
		if ent.port != nil {
			port = *ent.port
		}
		out[name] = clusterEndpoint{IP: ent.ip, Port: port}
	}
	return out, nil
}

// ──────────────────────────────────────────────────────────────
// Helpers: spec defaults / tiny utils
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) defaultSpec(o *orchestrationv1alpha1.Orchestrator) {
	if o.Spec.Namespace == "" {
		o.Spec.Namespace = o.Namespace
	}
	if o.Spec.EventName == "" {
		o.Spec.EventName = o.Name
	}

	// Back-compat: 단일 EventSource → 복수 EventSources 승격
	if len(o.Spec.EventSources) == 0 && o.Spec.EventSource != nil && o.Spec.EventSource.Name != "" {
		o.Spec.EventSources = []orchestrationv1alpha1.EventSourceSpec{*o.Spec.EventSource}
	}

	// EventSources 기본값
	if len(o.Spec.EventSources) > 0 {
		ports := randomPorts(len(o.Spec.EventSources), 10000)
		for i := range o.Spec.EventSources {
			es := &o.Spec.EventSources[i]
			if es.Type == "" {
				es.Type = "webhook"
			}
			if es.Port == 0 {
				es.Port = ports[i]
			}
			if es.Params == nil {
				es.Params = map[string]string{}
			}
			if es.Params["endpoint"] == "" {
				name := es.Name
				if name == "" {
					name = o.Spec.EventName
				}
				es.Params["endpoint"] = "/" + name
			}
			if es.Params["method"] == "" {
				es.Params["method"] = "POST"
			}
		}
	} else {
		// 레거시 단일 EventSource 기본값
		if o.Spec.EventSource == nil {
			o.Spec.EventSource = &orchestrationv1alpha1.EventSourceSpec{}
		}
		if o.Spec.EventSource.Type == "" {
			o.Spec.EventSource.Type = "webhook"
		}
		if o.Spec.EventSource.Port == 0 {
			rand.Seed(time.Now().UnixNano())
			o.Spec.EventSource.Port = int32(10000 + rand.Intn(10000))
		}
		if o.Spec.EventSource.Params == nil {
			o.Spec.EventSource.Params = map[string]string{}
		}
		if o.Spec.EventSource.Params["endpoint"] == "" {
			o.Spec.EventSource.Params["endpoint"] = "/" + o.Spec.EventName
		}
		if o.Spec.EventSource.Params["method"] == "" {
			o.Spec.EventSource.Params["method"] = "POST"
		}
	}

	// Services: 리스트 우선. 단일 service 있으면 승격
	if len(o.Spec.Services) == 0 && o.Spec.Service.Image != "" {
		if o.Spec.Service.Name == "" {
			o.Spec.Service.Name = nameKS(o.Spec.EventName)
		}
		o.Spec.Services = []orchestrationv1alpha1.ServiceSpec{o.Spec.Service}
	}
	for i := range o.Spec.Services {
		if o.Spec.Services[i].ConcurrencyTarget != nil && *o.Spec.Services[i].ConcurrencyTarget < 1 {
			v := int32(1)
			o.Spec.Services[i].ConcurrencyTarget = &v
		}
	}

	if o.Spec.EventType == "" {
		o.Spec.EventType = "webhook"
	}
}

func randomPorts(n int, start int32) []int32 {
	rand.Seed(time.Now().UnixNano())
	used := map[int32]bool{}
	out := make([]int32, n)
	for i := 0; i < n; i++ {
		for {
			p := start + int32(rand.Intn(10000))
			if !used[p] {
				used[p] = true
				out[i] = p
				break
			}
		}
	}
	return out
}

func toEnvList(m map[string]string) []any {
	if len(m) == 0 {
		return []any{}
	}
	out := make([]any, 0, len(m))
	for k, v := range m {
		out = append(out, map[string]any{"name": k, "value": v})
	}
	return out
}

func extractClusters(sel []orchestrationv1alpha1.SelectedCluster) []string {
	out := make([]string, 0, len(sel))
	for _, s := range sel {
		out = append(out, s.Cluster)
	}
	return out
}

func toAnySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i := range ss {
		out[i] = ss[i]
	}
	return out
}

// delete all namespaced policies (PropagationPolicy/OverridePolicy) that belong to this event
func (r *OrchestratorReconciler) deletePoliciesByLabel(ctx context.Context, ns, event string) error {
	sel := client.MatchingLabels(map[string]string{"orchestrator.operator.io/event": event})

	// PropagationPolicy
	ppList := &uobj.UnstructuredList{}
	ppList.SetAPIVersion("policy.karmada.io/v1alpha1")
	ppList.SetKind("PropagationPolicyList")
	if err := r.List(ctx, ppList, client.InNamespace(ns), sel); err == nil {
		for i := range ppList.Items {
			item := ppList.Items[i]
			_ = r.Delete(ctx, &item)
		}
	}

	// OverridePolicy (in case any was created with the same label)
	opList := &uobj.UnstructuredList{}
	opList.SetAPIVersion("policy.karmada.io/v1alpha1")
	opList.SetKind("OverridePolicyList")
	if err := r.List(ctx, opList, client.InNamespace(ns), sel); err == nil {
		for i := range opList.Items {
			item := opList.Items[i]
			_ = r.Delete(ctx, &item)
		}
	}
	return nil
}

// Helper: delete all resources of a given kind/apiVersion/namespace/label.
func (r *OrchestratorReconciler) deleteByLabel(ctx context.Context, apiVersion, kind, ns string, sel client.MatchingLabels) error {
    // Build a List for the given Kind
    list := &uobj.UnstructuredList{}
    list.SetAPIVersion(apiVersion)
    list.SetKind(kind + "List")

    var opts []client.ListOption
    if ns != "" {
        opts = append(opts, client.InNamespace(ns))
    }
    if sel != nil {
        opts = append(opts, sel)
    }

    if err := r.List(ctx, list, opts...); err != nil {
        return err
    }
    for i := range list.Items {
        item := list.Items[i]
        _ = r.Delete(ctx, &item)
    }
    return nil
}

// Delete Karmada ResourceBinding/ClusterResourceBinding/Work created for this event label.
// We rely on label propagation (resourcetemplate.karmada.io/managed-labels includes orchestrator label).
func (r *OrchestratorReconciler) deleteBindingsAndWorks(ctx context.Context, ns, event string) {
    sel := client.MatchingLabels(map[string]string{"orchestrator.operator.io/event": event})

    // 1) ResourceBinding (namespaced in the original resource namespace)
    _ = r.deleteByLabel(ctx, "work.karmada.io/v1alpha2", "ResourceBinding", ns, sel)
    // try older version as best-effort
    _ = r.deleteByLabel(ctx, "work.karmada.io/v1alpha1", "ResourceBinding", ns, sel)

    // 2) ClusterResourceBinding (cluster-scoped)
    _ = r.deleteByLabel(ctx, "work.karmada.io/v1alpha2", "ClusterResourceBinding", "", sel)
    _ = r.deleteByLabel(ctx, "work.karmada.io/v1alpha1", "ClusterResourceBinding", "", sel)

    // 3) Work (namespaced in karmada-es-* or similar). List across all namespaces.
    _ = r.deleteByLabel(ctx, "work.karmada.io/v1alpha1", "Work", "", sel)
}

// shard EventSource/Sensor names across clusters (round-robin)
func shardNames(names []string, clusters []string) map[string][]string {
	res := make(map[string][]string)
	if len(clusters) == 0 || len(names) == 0 {
		return res
	}
	for i, n := range names {
		c := clusters[i%len(clusters)]
		res[c] = append(res[c], n)
	}
	return res
}

// ──────────────────────────────────────────────────────────────
// Reconcile
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var orch orchestrationv1alpha1.Orchestrator
	if err := r.Get(ctx, req.NamespacedName, &orch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log := crlog.FromContext(ctx)
	log.Info("Reconciling Orchestrator", "name", orch.Name, "namespace", orch.Namespace)

	// Optional pause for demos: set annotation orchestrator.operator.io/pause: "true"
	if orch.Annotations != nil && strings.EqualFold(orch.Annotations["orchestrator.operator.io/pause"], "true") {
		r.setPhase(&orch, orchestrationv1alpha1.PhasePending, "Paused by annotation")
		_ = r.Status().Update(ctx, &orch)
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	// Finalizer 보장 / 삭제 처리
	if orch.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&orch, orchestratorFinalizer) {
			controllerutil.AddFinalizer(&orch, orchestratorFinalizer)
			if err := r.Update(ctx, &orch); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// sensor / workflowtemplate
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameSN(orch.Spec.EventName), "argoproj.io/v1alpha1", "Sensor")
		_ = r.deleteChild(ctx, orch.Spec.Namespace, nameWT(orch.Spec.EventName), "argoproj.io/v1alpha1", "WorkflowTemplate")

		// delete all namespaced policies created for this Orchestrator by label
		_ = r.deletePoliciesByLabel(ctx, orch.Spec.Namespace, orch.Spec.EventName)

		// also cleanup ResourceBindings/ClusterResourceBindings/Works carrying this label
		r.deleteBindingsAndWorks(ctx, orch.Spec.Namespace, orch.Spec.EventName)

		// cluster-scoped CPP for Namespace propagation (cannot have namespaced ownerRef)
		_ = r.deleteChild(ctx, "", fmt.Sprintf("%s-cpp-ns-%s", orch.Spec.EventName, orch.Spec.Namespace), "policy.karmada.io/v1alpha1", "ClusterPropagationPolicy")

		controllerutil.RemoveFinalizer(&orch, orchestratorFinalizer)
		_ = r.Update(ctx, &orch)
		return ctrl.Result{}, nil
	}

	// 0) 기본값 주입
	r.defaultSpec(&orch)

	// 1) 베이스 리소스(KServices / EventSources) 생성
	if err := r.ensureKnativeServices(ctx, &orch); err != nil {
		return r.fail(&orch, "EnsureKsvcFailed", err)
	}
	if _, err := r.ensureEventSourceNodePort(ctx, &orch); err != nil {
		return r.fail(&orch, "EnsureEventSourceFailed", err)
	}

	// 2) PlacementDecision 확보(없으면 생성만) → 선택 기다림
	pd, err := r.ensurePlacement(ctx, &orch)
	if err != nil {
		return r.fail(&orch, "EnsurePlacementFailed", err)
	}
	if len(pd.Status.Selected) == 0 {
		now := metav1.Now()
		orch.Status.LastPlacementTime = &now

		// PD를 기다리는 동안에도 WT+Sensors는 만들어 둔다(이후 PP로 바인딩)
		baseObjs := r.renderBase(&orch, "default", map[string]string{})
		for _, o := range baseObjs {
			applyCommonLabel(o, orch.Spec.EventName)
			setOwner(&orch, o, r.Scheme)
		}
		_ = r.applyAll(ctx, baseObjs)

		r.setPhase(&orch, orchestrationv1alpha1.PhasePending, "Base resources created; waiting for PlacementDecision")
		_ = r.Status().Update(ctx, &orch)
		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	// 3) 대상 클러스터 계산 (PD 선택 결과 기반)
	selected := extractClusters(pd.Status.Selected)

	// 선택된(또는 도메인맵 기반 전체) 클러스터로 네임스페이스 전파
	selectedAll := r.targetClustersForShared(ctx, &orch, pd)
	cppNS := renderClusterPropagationPolicyNamespace(&orch, selectedAll)
	if err := r.applyAll(ctx, []*uobj.Unstructured{cppNS}); err != nil {
		return r.fail(&orch, "ApplyNamespaceCPPFailed", err)
	}

	// 4) 정책 생성
	//    - Sensor+WorkflowTemplate: 클러스터별 샤딩(co-location)
	//    - EventSource(+NodePort):  클러스터별 샤딩
	//    - Knative Service:         선택된 모든 클러스터
	svcTargets := selected
	domap, _ := r.loadDomainMap(ctx, orch.Spec.Namespace)
	policyObjs := r.renderPoliciesSplit(&orch, svcTargets, []string{}, domap)
	for _, o := range policyObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, policyObjs); err != nil {
		return r.fail(&orch, "ApplyPoliciesFailed", err)
	}

	// 5) 베이스(ksvc/wt/sensor) 생성/패치 - 고정 EventBus("default") 사용
	baseObjs := r.renderBase(&orch, "default", map[string]string{})
	for _, o := range baseObjs {
		setOwner(&orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, baseObjs); err != nil {
		return r.fail(&orch, "ApplyBaseFailed", err)
	}

	// 6) Status 최종 업데이트
	orch.Status.SelectedClusters = selected

	// 간단한 Phase 계산
	r.setPhase(&orch, orchestrationv1alpha1.PhaseReady, "All resources applied")

	if err := r.Status().Update(ctx, &orch); err != nil {
		return ctrl.Result{}, err
	}

	log.V(1).Info("reconciled orchestrator",
		"name", orch.Name, "ns", orch.Namespace,
		"selected", selected,
	)
	return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
}

// ──────────────────────────────────────────────────────────────
// ensure / apply / delete / status helpers
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) ensurePlacement(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator) (*orchestrationv1alpha1.PlacementDecision, error) {
	name := fmt.Sprintf("%s-pd", orch.Name)
	var pd orchestrationv1alpha1.PlacementDecision
	err := r.Get(ctx, client.ObjectKey{Namespace: orch.Namespace, Name: name}, &pd)
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}
	if err != nil {
		pd = orchestrationv1alpha1.PlacementDecision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: orch.Namespace,
			},
		}
		if err := controllerutil.SetControllerReference(orch, &pd, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, &pd); err != nil {
			return nil, err
		}
	}
	return &pd, nil
}

func setOwner(orch *orchestrationv1alpha1.Orchestrator, o *uobj.Unstructured, scheme *runtime.Scheme) {
	_ = controllerutil.SetControllerReference(orch, o, scheme)
}

func (r *OrchestratorReconciler) applyAll(ctx context.Context, objs []*uobj.Unstructured) error {
	for _, o := range objs {
		if o == nil {
			continue
		}
		o.SetManagedFields(nil)
		if err := r.Patch(ctx, o, client.Apply,
			client.FieldOwner(fieldManager),
			client.ForceOwnership,
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *OrchestratorReconciler) deleteChild(ctx context.Context, ns, name, api, kind string) error {
	u := &uobj.Unstructured{}
	u.SetAPIVersion(api)
	u.SetKind(kind)
	u.SetNamespace(ns)
	u.SetName(name)
	return r.Delete(ctx, u)
}

func (r *OrchestratorReconciler) setPhase(orch *orchestrationv1alpha1.Orchestrator, phase orchestrationv1alpha1.OrchestratorPhase, msg string) {
	orch.Status.Phase = phase
	r.setCond(orch, "Ready", condFromPhase(phase), string(phase), msg)
}

func (r *OrchestratorReconciler) targetClustersForShared(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator, pd *orchestrationv1alpha1.PlacementDecision) []string {
	// 1) 도메인맵이 있으면 키 전체 사용(전 클러스터 전파용)
	if domap, err := r.loadDomainMap(ctx, orch.Spec.Namespace); err == nil && len(domap) > 0 {
		out := make([]string, 0, len(domap))
		for name := range domap {
			out = append(out, name)
		}
		return out
	}
	// 2) 없으면 Selected만
	return extractClusters(pd.Status.Selected)
}

func condFromPhase(p orchestrationv1alpha1.OrchestratorPhase) metav1.ConditionStatus {
	switch p {
	case orchestrationv1alpha1.PhaseReady:
		return metav1.ConditionTrue
	case orchestrationv1alpha1.PhaseError:
		return metav1.ConditionFalse
	default:
		return metav1.ConditionUnknown
	}
}

func (r *OrchestratorReconciler) setCond(orch *orchestrationv1alpha1.Orchestrator, t string, status metav1.ConditionStatus, reason, msg string) {
	cond := metav1.Condition{
		Type:               t,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: orch.Generation,
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&orch.Status.Conditions, cond)
}

func (r *OrchestratorReconciler) fail(orch *orchestrationv1alpha1.Orchestrator, reason string, err error) (ctrl.Result, error) {
	r.setPhase(orch, orchestrationv1alpha1.PhaseError, err.Error())
	_ = r.Status().Update(context.Background(), orch)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, err
}

// ──────────────────────────────────────────────────────────────
// Renderers: KService / EventSource / WorkflowTemplate / Sensor
// ──────────────────────────────────────────────────────────────

func renderKServices(orch *orchestrationv1alpha1.Orchestrator) []*uobj.Unstructured {
	objs := []*uobj.Unstructured{}
	for _, svc := range orch.Spec.Services {
		container := map[string]any{
			"image": svc.Image,
		}
		if env := toEnvList(svc.Env); len(env) > 0 {
			container["env"] = env
		}
		spec := map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{container},
				},
			},
		}
		// ensure template metadata + annotations exist
		tmpl := spec["template"].(map[string]any)
		md, ok := tmpl["metadata"].(map[string]any)
		if !ok || md == nil {
			md = map[string]any{}
			tmpl["metadata"] = md
		}
		ann, ok := md["annotations"].(map[string]any)
		if !ok || ann == nil {
			ann = map[string]any{}
		}
		// Keep at least one pod warm for demos
		ann["autoscaling.knative.dev/minScale"] = "1"
		// Preserve previous behavior
		if svc.ConcurrencyTarget != nil {
			ann["autoscaling.knative.dev/target"] = fmt.Sprintf("%d", *svc.ConcurrencyTarget)
		}
		md["annotations"] = ann
		u := &uobj.Unstructured{}
		u.Object = map[string]any{
			"apiVersion": "serving.knative.dev/v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      svc.Name,
				"namespace": orch.Spec.Namespace,
			},
			"spec": spec,
		}
		objs = append(objs, u)
	}
	return objs
}

func (r *OrchestratorReconciler) ensureKnativeServices(ctx context.Context, orch *orchestrationv1alpha1.Orchestrator) error {
	kobjs := renderKServices(orch)
	for _, o := range kobjs {
		applyCommonLabel(o, orch.Spec.EventName)
		setOwner(orch, o, r.Scheme)
	}
	return r.applyAll(ctx, kobjs)
}

func renderEventSources(orch *orchestrationv1alpha1.Orchestrator) []*uobj.Unstructured {
	objs := []*uobj.Unstructured{}
	for _, es := range orch.Spec.EventSources {
		endpoint := es.Params["endpoint"]
		if endpoint == "" {
			endpoint = "/" + es.Name
		}
		method := es.Params["method"]
		if method == "" {
			method = "POST"
		}
		u := &uobj.Unstructured{}
		u.Object = map[string]any{
			"apiVersion": "argoproj.io/v1alpha1",
			"kind":       "EventSource",
			"metadata": map[string]any{
				"name":      es.Name,
				"namespace": orch.Spec.Namespace,
			},
			"spec": map[string]any{
				"service": map[string]any{
					"ports": []any{
						map[string]any{"port": es.Port, "targetPort": es.Port},
					},
				},
				"webhook": map[string]any{
					es.Name: map[string]any{
						"port":     fmt.Sprintf("%d", es.Port),
						"endpoint": endpoint,
						"method":   method,
						"jsonBody": true,
					},
				},
			},
		}
		objs = append(objs, u)

		// NodePort Service (nodePort 고정 원할 때만 지정)
		svcPort := map[string]any{
			"name":       "http",
			"port":       es.Port,
			"targetPort": es.Port,
		}
		if es.NodePort > 0 {
			svcPort["nodePort"] = es.NodePort
		}

		svc := &uobj.Unstructured{}
		svc.Object = map[string]any{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      es.Name + "-np",
				"namespace": orch.Spec.Namespace,
			},
			"spec": map[string]any{
				"type": "NodePort",
				"selector": map[string]any{
					"eventsource-name": es.Name,
				},
				"ports": []any{svcPort},
			},
		}
		objs = append(objs, svc)
	}
	return objs
}

func renderEventSourceNodePortService(orch *orchestrationv1alpha1.Orchestrator) *uobj.Unstructured {
	if orch.Spec.EventSource.Port == 0 {
		return nil
	}
	name := nameESNodePortSvc(orch.Spec.EventName)
	port := orch.Spec.EventSource.Port

	portMap := map[string]any{
		"name":       "http",
		"port":       port,
		"targetPort": port,
	}
	if orch.Spec.EventSource.NodePort > 0 {
		portMap["nodePort"] = orch.Spec.EventSource.NodePort
	}

	spec := map[string]any{
		"type": string(corev1.ServiceTypeNodePort),
		"selector": map[string]any{
			"eventsource-name": nameES(orch.Spec.EventName),
		},
		"ports": []any{portMap},
	}

	md := map[string]any{
		"name":      name,
		"namespace": orch.Spec.Namespace,
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   md,
		"spec":       spec,
	}
	return u
}

func serviceURLFor(svcName string, orch *orchestrationv1alpha1.Orchestrator, svcURL map[string]string) string {
	if svcURL != nil {
		if v, ok := svcURL[svcName]; ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	// cluster-local fallback
	return fmt.Sprintf("http://%s.%s.svc.cluster.local", svcName, orch.Spec.Namespace)
}

// Build external URL: http://<svc>.<ns>.<IP>.nip.io[:Port]/<svc>
func buildSvcURL(orch *orchestrationv1alpha1.Orchestrator, ep clusterEndpoint, svcName string) string {
	host := fmt.Sprintf("%s.%s.%s.nip.io", svcName, orch.Spec.Namespace, ep.IP)

	// Decide path per service:
	// - output: use "/output"
	// - update: explicitly "/update"
	// - default: "/<svcName>"
	path := "/" + svcName
	switch strings.ToLower(strings.TrimSpace(svcName)) {
	case "output":
		path = "/output"
	case "update":
		path = "/update"
	}

	if ep.Port > 0 {
		return fmt.Sprintf("http://%s:%d%s", host, ep.Port, path)
	}
	return fmt.Sprintf("http://%s%s", host, path)
}

// Create an OverridePolicy to patch WT per-cluster HTTP URLs (one rule per cluster)
func renderOPWTUrls(
	orch *orchestrationv1alpha1.Orchestrator,
	cluster string,
	ep clusterEndpoint,
) *uobj.Unstructured {
	// WT templates: [0]=main, 이후부터 서비스 템플릿
	patches := []any{}
	for idx, svc := range orch.Spec.Services {
		if strings.TrimSpace(svc.Name) == "" {
			continue
		}
		path := fmt.Sprintf("/spec/templates/%d/http/url", idx+1) // +1: skip "main"
		val := buildSvcURL(orch, ep, svc.Name)
		patches = append(patches, map[string]any{
			"operator": "replace",
			"path":     path,
			"value":    val,
		})
	}
	if len(patches) == 0 {
		return nil
	}

	name := fmt.Sprintf("%s-op-wt-url-%s", orch.Spec.EventName, cluster)
	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "OverridePolicy",
		"metadata": map[string]any{
			"name":      name,
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{
					"apiVersion": "argoproj.io/v1alpha1",
					"kind":       "WorkflowTemplate",
					"name":       nameWT(orch.Spec.EventName),
					"namespace":  orch.Spec.Namespace,
				},
			},
			"overrideRules": []any{
				map[string]any{
					"targetCluster": map[string]any{
						"clusterNames": []any{cluster},
					},
					"overriders": map[string]any{
						"plaintext": patches,
					},
				},
			},
		},
	}
	return u
}

func renderWorkflowTemplate(orch *orchestrationv1alpha1.Orchestrator, svcURL map[string]string) *uobj.Unstructured {
	name := nameWT(orch.Spec.EventName)

	// Step 1: main steps
	stepsNested := []any{}
	for _, svc := range orch.Spec.Services {
		step := map[string]any{
			"name":     svc.Name,
			"template": svc.Name,
			"arguments": map[string]any{
				"parameters": []any{
					map[string]any{"name": "sensorName", "value": "{{workflow.parameters.sensorName}}"},
					map[string]any{"name": "subscriptionID", "value": "{{workflow.parameters.subscriptionID}}"},
					map[string]any{"name": "timestamp", "value": "{{workflow.parameters.timestamp}}"},
				},
			},
		}
		stepsNested = append(stepsNested, []any{step})
	}

	// Step 2: templates array (main + per service http)
	templates := []any{
		map[string]any{
			"name":  "main",
			"steps": stepsNested,
		},
	}

	for _, svc := range orch.Spec.Services {
		baseURL := serviceURLFor(svc.Name, orch, svcURL)
		path := "/" + svc.Name
		if strings.EqualFold(svc.Name, "output") {
			path = "/output"
		}
		finalURL := strings.TrimRight(baseURL, "/") + path

		tmpl := map[string]any{
			"name": svc.Name,
			"inputs": map[string]any{
				"parameters": []any{
					map[string]any{"name": "sensorName"},
					map[string]any{"name": "subscriptionID"},
					map[string]any{"name": "timestamp"},
				},
			},
			"http": map[string]any{
				"url":    finalURL,
				"method": "POST",
				"headers": []any{
					map[string]any{"name": "Content-Type", "value": "application/json"},
				},
				"body": `{
  "subscriptionID": "{{inputs.parameters.subscriptionID}}",
  "sensorName": "{{inputs.parameters.sensorName}}",
  "timestamp": "{{inputs.parameters.timestamp}}"
}`,
			},
			"retryStrategy": map[string]any{
				"limit":       3,
				"retryPolicy": "Always",
				"backoff": map[string]any{
					"duration":    "2s",
					"factor":      2,
					"maxDuration": "30s",
				},
			},
		}
		templates = append(templates, tmpl)
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "WorkflowTemplate",
		"metadata": map[string]any{
			"name":      name,
			"namespace": orch.Spec.Namespace,
		},
		"spec": map[string]any{
			"entrypoint": "main",
			"arguments": map[string]any{
				"parameters": []any{
					map[string]any{"name": "sensorName"},
					map[string]any{"name": "subscriptionID"},
					map[string]any{"name": "timestamp"},
				},
			},
			"templates": templates,
		},
	}
	return u
}

func renderLegacySensor(orch *orchestrationv1alpha1.Orchestrator, eventBusName string) *uobj.Unstructured {
	sensorName := nameSN(orch.Spec.EventName)
	wtName := nameWT(orch.Spec.EventName)

	wfObj := map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Workflow",
		"metadata":   map[string]any{"generateName": fmt.Sprintf("%s-", orch.Spec.EventName)},
		"spec": map[string]any{
			"serviceAccountName":  "operate-workflow-sa",
			"entrypoint":          "main",
			"workflowTemplateRef": map[string]any{"name": wtName},
		},
	}

	dependencies := []any{
		map[string]any{
			"name":                 orch.Spec.EventName,
			"eventSourceName":      nameES(orch.Spec.EventName),
			"eventSourceNamespace": orch.Spec.Namespace,
			"eventName":            orch.Spec.EventName,
		},
	}

	trigger := map[string]any{
		"template": map[string]any{
			"name": sensorName + "-trigger",
			"argoWorkflow": map[string]any{
				"operation": "submit",
				"source":    map[string]any{"resource": wfObj},
			},
		},
	}

	spec := map[string]any{
		"template": map[string]any{
			"eventBusName":       eventBusName,
			"serviceAccountName": "operate-workflow-sa",
		},
		"dependencies": dependencies,
		"triggers":     []any{trigger},
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Sensor",
		"metadata": map[string]any{
			"name":      sensorName,
			"namespace": orch.Spec.Namespace,
		},
		"spec": spec,
	}
	return u
}

// ──────────────────────────────────────────────────────────────
// Namespace CPP
func renderClusterPropagationPolicyNamespace(
	orch *orchestrationv1alpha1.Orchestrator,
	clusterNames []string,
) *uobj.Unstructured {
	name := fmt.Sprintf("%s-cpp-ns-%s", orch.Spec.EventName, orch.Spec.Namespace)

	u := &uobj.Unstructured{}
	u.SetAPIVersion("policy.karmada.io/v1alpha1")
	u.SetKind("ClusterPropagationPolicy")
	u.SetName(name)
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "ClusterPropagationPolicy",
		"metadata": map[string]any{
			"name": name,
		},
		"spec": map[string]any{
			"resourceSelectors": []any{
				map[string]any{"apiVersion": "v1", "kind": "Namespace", "name": orch.Spec.Namespace},
			},
			"placement": map[string]any{
				"clusterAffinity": map[string]any{
					"clusterNames": toAnySlice(clusterNames),
				},
			},
		},
	}
	return u
}

// ──────────────────────────────────────────────────────────────
// Policies (split/sharded):
func (r *OrchestratorReconciler) renderPoliciesSplit(
	orch *orchestrationv1alpha1.Orchestrator,
	selected []string,
	_ []string, // unused: kept for signature compatibility
	domap map[string]clusterEndpoint,
) []*uobj.Unstructured {
	objs := []*uobj.Unstructured{}

	// ES 이름들 수집
	esNames := []string{}
	for _, es := range orch.Spec.EventSources {
		if strings.TrimSpace(es.Name) != "" {
			esNames = append(esNames, es.Name)
		}
	}

	// ES가 없으면 KService만 다중 배포
	if len(esNames) == 0 {
		ppSvc := renderPropagationPolicyForServices(orch, selected)
		if ppSvc != nil {
			applyCommonLabel(ppSvc, orch.Spec.EventName)
			objs = append(objs, ppSvc)
		}
		return objs
	}

	// ES를 선택된 클러스터에 라운드로빈 샤딩
	esShard := shardNames(esNames, selected)

	// 센서가 한 개 이상 있는 클러스터 목록
	clustersWithSensors := []string{}
	for _, c := range selected {
		if len(esShard[c]) > 0 {
			clustersWithSensors = append(clustersWithSensors, c)
		}
	}

	// 1) WorkflowTemplate: 센서가 있는 모든 클러스터에 1개의 PP로 전파
	ppWT := renderPPWorkflowTemplate(orch, clustersWithSensors)
	if ppWT != nil {
		applyCommonLabel(ppWT, orch.Spec.EventName)
		objs = append(objs, ppWT)
	}

	// 1-b) 각 클러스터별 nip.io URL로 WT http.url 오버라이드
	for _, c := range clustersWithSensors {
		if ep, ok := domap[c]; ok && strings.TrimSpace(ep.IP) != "" {
			op := renderOPWTUrls(orch, c, ep)
			if op != nil {
				applyCommonLabel(op, orch.Spec.EventName)
				objs = append(objs, op)
			}
		}
	}

	// 2) 클러스터별 Sensor들만(동일 클러스터 내 WT 중복 충돌 방지)
	for _, cluster := range selected {
		names := esShard[cluster]
		pp := renderPPClusterSensorsOnly(orch, cluster, names)
		if pp != nil {
			applyCommonLabel(pp, orch.Spec.EventName)
			objs = append(objs, pp)
		}
	}

	// 3) 클러스터별 EventSource + NodePort 서비스
	for _, cluster := range selected {
		names := esShard[cluster]
		pp := renderPPClusterEventSources(orch, cluster, names)
		if pp != nil {
			applyCommonLabel(pp, orch.Spec.EventName)
			objs = append(objs, pp)
		}
	}

	// 4) Knative Service는 선택된 모든 클러스터로
	ppSvc := renderPropagationPolicyForServices(orch, selected)
	if ppSvc != nil {
		applyCommonLabel(ppSvc, orch.Spec.EventName)
		objs = append(objs, ppSvc)
	}

	return objs
}

func renderPropagationPolicyForServices(
	orch *orchestrationv1alpha1.Orchestrator,
	selected []string,
) *uobj.Unstructured {
	name := fmt.Sprintf("%s-pp-services", orch.Spec.EventName)
	selectors := []any{}
	if len(orch.Spec.Services) > 0 {
		for _, s := range orch.Spec.Services {
			if strings.TrimSpace(s.Name) == "" {
				continue
			}
			selectors = append(selectors, map[string]any{
				"apiVersion": "serving.knative.dev/v1", "kind": "Service", "name": s.Name, "namespace": orch.Spec.Namespace,
			})
		}
	} else {
		selectors = append(selectors, map[string]any{
			"apiVersion": "serving.knative.dev/v1", "kind": "Service", "name": nameKS(orch.Spec.EventName), "namespace": orch.Spec.Namespace,
		})
	}
	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata":   map[string]any{"name": name, "namespace": orch.Spec.Namespace},
		"spec": map[string]any{
			"resourceSelectors": selectors,
			"placement":         map[string]any{"clusterAffinity": map[string]any{"clusterNames": toAnySlice(selected)}},
		},
	}
	return u
}

// ONE PP for WorkflowTemplate placed on all clusters that have sensors
func renderPPWorkflowTemplate(
	orch *orchestrationv1alpha1.Orchestrator,
	clusters []string,
) *uobj.Unstructured {
	if len(clusters) == 0 {
		return nil
	}
	name := fmt.Sprintf("%s-pp-wt", orch.Spec.EventName)
	selectors := []any{
		map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "WorkflowTemplate", "name": nameWT(orch.Spec.EventName), "namespace": orch.Spec.Namespace},
	}
	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata":   map[string]any{"name": name, "namespace": orch.Spec.Namespace},
		"spec": map[string]any{
			"resourceSelectors": selectors,
			"placement":         map[string]any{"clusterAffinity": map[string]any{"clusterNames": toAnySlice(clusters)}},
		},
	}
	return u
}

// Per-cluster Sensors ONLY (WT excluded to avoid conflicts)
func renderPPClusterSensorsOnly(
	orch *orchestrationv1alpha1.Orchestrator,
	cluster string,
	sensorESNames []string, // sensors are named by ES name
) *uobj.Unstructured {
	if len(sensorESNames) == 0 {
		return nil
	}
	name := fmt.Sprintf("%s-pp-sensor-%s", orch.Spec.EventName, cluster)
	selectors := []any{}
	for _, es := range sensorESNames {
		selectors = append(selectors, map[string]any{
			"apiVersion": "argoproj.io/v1alpha1", "kind": "Sensor", "name": nameSNFor(orch.Spec.EventName, es), "namespace": orch.Spec.Namespace,
		})
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata":   map[string]any{"name": name, "namespace": orch.Spec.Namespace},
		"spec": map[string]any{
			"resourceSelectors": selectors,
			"placement":         map[string]any{"clusterAffinity": map[string]any{"clusterNames": []any{cluster}}},
		},
	}
	return u
}

// Per-cluster: EventSources + NodePort Services
func renderPPClusterEventSources(
	orch *orchestrationv1alpha1.Orchestrator,
	cluster string,
	esNames []string,
) *uobj.Unstructured {
	if len(esNames) == 0 {
		return nil
	}
	name := fmt.Sprintf("%s-pp-eventsource-%s", orch.Spec.EventName, cluster)

	selectors := []any{}
	for _, es := range esNames {
		selectors = append(selectors,
			map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "EventSource", "name": es, "namespace": orch.Spec.Namespace},
			map[string]any{"apiVersion": "v1", "kind": "Service", "name": es + "-np", "namespace": orch.Spec.Namespace},
		)
	}

	u := &uobj.Unstructured{}
	u.Object = map[string]any{
		"apiVersion": "policy.karmada.io/v1alpha1",
		"kind":       "PropagationPolicy",
		"metadata":   map[string]any{"name": name, "namespace": orch.Spec.Namespace},
		"spec": map[string]any{
			"resourceSelectors": selectors,
			"placement":         map[string]any{"clusterAffinity": map[string]any{"clusterNames": []any{cluster}}},
		},
	}
	return u
}

// ──────────────────────────────────────────────────────────────
// Ksvc / NodePort
// ──────────────────────────────────────────────────────────────

func (r *OrchestratorReconciler) ensureEventSourceNodePort(
	ctx context.Context,
	orch *orchestrationv1alpha1.Orchestrator,
) (int32, error) {
	esObjs := renderEventSources(orch)
	for _, o := range esObjs {
		applyCommonLabel(o, orch.Spec.EventName)
		setOwner(orch, o, r.Scheme)
	}
	if err := r.applyAll(ctx, esObjs); err != nil {
		return 0, err
	}

	// 레거시 단일 EventSource 경로만 기존 NodePort Svc 유지
	if len(orch.Spec.EventSources) == 0 && orch.Spec.EventSource != nil {
		svc := renderEventSourceNodePortService(orch)
		if svc != nil {
			applyCommonLabel(svc, orch.Spec.EventName)
			setOwner(orch, svc, r.Scheme)
			if err := r.applyAll(ctx, []*uobj.Unstructured{svc}); err != nil {
				return 0, err
			}
		}
		if orch.Spec.EventSource.NodePort > 0 {
			return orch.Spec.EventSource.NodePort, nil
		}
	}
	return 0, nil
}

func (r *OrchestratorReconciler) readEventSourceNodePort(
	ctx context.Context,
	orch *orchestrationv1alpha1.Orchestrator,
) (int32, error) {
	if len(orch.Spec.EventSources) > 0 || orch.Spec.EventSource == nil {
		return 0, nil
	}
	svc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: orch.Spec.Namespace,
		Name:      nameESNodePortSvc(orch.Spec.EventName),
	}, svc); err != nil {
		return 0, err
	}
	if len(svc.Spec.Ports) == 0 {
		return 0, nil
	}
	return svc.Spec.Ports[0].NodePort, nil
}

// --- 베이스 리소스 렌더러 (WT + Sensors(1:1 per ES)) ---
func (r *OrchestratorReconciler) renderBase(
	orch *orchestrationv1alpha1.Orchestrator,
	eventBusName string,
	svcURL map[string]string,
) []*uobj.Unstructured {
	wt := renderWorkflowTemplate(orch, svcURL)

	var sensors []*uobj.Unstructured
	if len(orch.Spec.EventSources) > 0 {
		wtName := nameWT(orch.Spec.EventName)

		for _, es := range orch.Spec.EventSources {
			if strings.TrimSpace(es.Name) == "" {
				continue
			}
			sensorName := nameSNFor(orch.Spec.EventName, es.Name)

			dependencies := []any{
				map[string]any{
					"name":                 es.Name,
					"eventSourceName":      es.Name,
					"eventSourceNamespace": orch.Spec.Namespace,
					"eventName":            es.Name,
				},
			}

			wfObj := map[string]any{
				"apiVersion": "argoproj.io/v1alpha1",
				"kind":       "Workflow",
				"metadata":   map[string]any{"generateName": fmt.Sprintf("%s-", orch.Spec.EventName)},
				"spec": map[string]any{
					"serviceAccountName": "operate-workflow-sa",
					"entrypoint":         "main",
					"arguments": map[string]any{
						"parameters": []any{
							map[string]any{"name": "sensorName", "value": ""},
							map[string]any{"name": "subscriptionID", "value": ""},
							map[string]any{"name": "timestamp", "value": ""},
						},
					},
					"workflowTemplateRef": map[string]any{"name": wtName},
				},
			}

			params := []any{
				map[string]any{
					"src":  map[string]any{"dependencyName": es.Name, "dataKey": "body.sensorName"},
					"dest": "spec.arguments.parameters.0.value",
				},
				map[string]any{
					"src":  map[string]any{"dependencyName": es.Name, "dataKey": "body.subscriptionID"},
					"dest": "spec.arguments.parameters.1.value",
				},
				map[string]any{
					"src":  map[string]any{"dependencyName": es.Name, "dataKey": "body.timestamp"},
					"dest": "spec.arguments.parameters.2.value",
				},
			}

			trigger := map[string]any{
				"template": map[string]any{
					"name": sensorName + "-trigger",
					"argoWorkflow": map[string]any{
						"operation":  "submit",
						"source":     map[string]any{"resource": wfObj},
						"parameters": params,
					},
				},
			}

			spec := map[string]any{
				"template": map[string]any{
					"eventBusName":       eventBusName,
					"serviceAccountName": "operate-workflow-sa",
					"container": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{"cpu": "50m", "memory": "50Mi"},
							"limits":   map[string]any{"cpu": "1", "memory": "500Mi"},
						},
					},
				},
				"dependencies": []any{dependencies[0]},
				"triggers":     []any{trigger},
			}

			sn := &uobj.Unstructured{}
			sn.Object = map[string]any{
				"apiVersion": "argoproj.io/v1alpha1",
				"kind":       "Sensor",
				"metadata": map[string]any{
					"name":      sensorName,
					"namespace": orch.Spec.Namespace,
				},
				"spec": spec,
			}
			sensors = append(sensors, sn)
		}
	} else {
		sn := renderLegacySensor(orch, eventBusName)
		if md, ok := sn.Object["metadata"].(map[string]any); ok {
			md["namespace"] = orch.Spec.Namespace
		}
		sensors = append(sensors, sn)
	}

	applyCommonLabel(wt, orch.Spec.EventName)
	setOwner(orch, wt, r.Scheme)
	objs := []*uobj.Unstructured{wt}
	for _, sn := range sensors {
		applyCommonLabel(sn, orch.Spec.EventName)
		setOwner(orch, sn, r.Scheme)
		objs = append(objs, sn)
	}
	return objs
}

// ──────────────────────────────────────────────────────────────
// 네이밍 규칙(일관 사용)
func nameESNodePortSvc(event string) string { return fmt.Sprintf("%s-eventsource-np", event) }
func nameES(base string) string             { return fmt.Sprintf("%s-event", base) }
func nameSN(base string) string             { return fmt.Sprintf("%s-sensor", base) }
func nameSNFor(base, es string) string      { return fmt.Sprintf("%s-sensor-%s", base, es) }
func nameWT(base string) string             { return fmt.Sprintf("%s-wt", base) }
func nameKS(base string) string             { return fmt.Sprintf("%s-func", base) }
func namePP(base string) string             { return fmt.Sprintf("%s-pp", base) }

// SetupWithManager sets up the controller with the Manager.
func (r *OrchestratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&orchestrationv1alpha1.Orchestrator{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("orchestrator").
		Complete(r)
}
