from django.db import models
import threading
import requests

class GraphDefinition(models.Model):
    version = models.CharField(max_length=32, default="1.0", unique=True)
    description = models.TextField(blank=True)
    default = models.BooleanField(default=False)
    spec = models.JSONField()

    def __str__(self):
        return f"{"GRAPH DEF" if self.default else "graph def"} v{self.version} - {self.description}"

    def save(self, *args, **kwargs):
        if self.default:
            GraphDefinition.objects.filter(default=True).update(default=False)
        super().save(*args, **kwargs)

    def nodes_dict(self):
        return (self.spec or {}).get("nodes", {}) or {}

    def node_names(self):
        return list(self.nodes_dict().keys())

    def start_node(self):
        return (self.spec or {}).get("start")

    def edges(self):
        s = self.spec or {}
        if "edges" in s and isinstance(s["edges"], list):
            out = []
            for e in s["edges"]:
                src, dst = e.get("from"), e.get("to")
                if src is not None and dst is not None:
                    out.append((src, dst))
            return out

        out = []
        for nid, n in self.nodes_dict().items():
            for dep in n.get("depends_on", []):
                out.append((dep, nid))
        return out

    def depends_on(self, node_id):
        n = self.nodes_dict().get(node_id, {})
        deps = n.get("depends_on", [])
        return list(deps) if isinstance(deps, list) else []

    def dependents_of(self, node_id):
        children = []
        for nid, n in self.nodes_dict().items():
            deps = n.get("depends_on", [])
            if isinstance(deps, list) and node_id in deps:
                children.append(nid)
        return children

    def terminal_nodes(self):
        children_map = {}
        for src, dst in self.edges():
            children_map[src] = children_map.get(src, 0) + 1
        return [nid for nid in self.node_names() if children_map.get(nid, 0) == 0]

    def roots(self):
        return [nid for nid in self.node_names() if not self.depends_on(nid)]

    def get_node_url(self, node_id):
        node = self.nodes_dict().get(node_id)
        if not node:
            return None
        return node.get("url")

class GraphRun(models.Model):
    class Status(models.TextChoices):
        RUNNING = "running", "Running"
        SUCCEEDED = "succeeded", "Succeeded"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    run_id = models.AutoField(primary_key=True)
    graph = models.ForeignKey(GraphDefinition, on_delete=models.PROTECT)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.RUNNING)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"Running {self.graph.version}:{self.run_id} - {self.status}"

class NodeInstance(models.Model):
    class NodeStatus(models.TextChoices):
        QUEUED = "queued", "Queued"
        RUNNING = "running", "Running"
        SUCCEEDED = "succeeded", "Succeeded"
        FAILED = "failed", "Failed"

    name = models.CharField(max_length=128, default="node")
    run = models.ForeignKey(GraphRun, on_delete=models.CASCADE, related_name="nodes")
    node_id = models.CharField(max_length=128)
    attempt_id = models.CharField(max_length=64, blank=True, default="")
    status = models.CharField(max_length=16, choices=NodeStatus.choices, default=NodeStatus.QUEUED)
    inputs = models.JSONField(default=dict)
    outputs = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [("run", "node_id", "attempt_id")]
        indexes = [
            models.Index(fields=["run", "node_id"]),
            models.Index(fields=["run", "status"]),
        ]

    def __str__(self):
        return f"Node {self.run.graph.version}:{self.run.run_id}:{self.node_id} - {self.status}"

class Fire(models.Model):
    id = models.AutoField(primary_key=True)
    attempts = models.IntegerField(default=0)
    node_instance = models.ForeignKey(NodeInstance, on_delete=models.CASCADE, related_name="fires")
    created_at = models.DateTimeField(auto_now_add=True)

    def post(self, url, json=None, timeout=0.5):
        t = threading.Thread(target=self._safe_post, args=(url, json, timeout), daemon=True)
        t.start()

    def _safe_post(self, url, payload, timeout):
        try:
            requests.post(url, json=payload, timeout=timeout)
        except Exception:
            pass

    def load_and_fire(self, node: NodeInstance):
        self.node_instance = node
        self.attempts += 1
        self.save()
        dependent_ids = node.run.graph.dependents_of(node.node_id)
        dependent_nodes = node.run.nodes.filter(node_id__in=dependent_ids)

        for dep in dependent_nodes:
            if dep.status != NodeInstance.NodeStatus.SUCCEEDED:
                print("Dependent node not succeeded:", dep.node_id)
                return False

        url = node.run.graph.get_node_url(node.node_id)
        if not url:
            print("No URL for node", node.node_id)
            return False

        self.post(url, node.inputs)
        return True