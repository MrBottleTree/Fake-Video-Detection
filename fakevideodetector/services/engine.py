import uuid
from django.db import transaction
from django.utils import timezone
from typing import Dict, List
from ..models import GraphDefinition, GraphRun, NodeInstance, Fire

def _parents(gdef, node_id):
    return gdef.depends_on(node_id)

def _children(gdef, node_id):
    return gdef.dependents_of(node_id)

def _merge_child_inputs(child_inputs, parent_outputs):
    merged = dict(child_inputs or {})
    if parent_outputs:
        merged.update(parent_outputs)
    return merged

def _ready_to_run(run, node_id):
    gdef = run.graph
    daddy_ji = _parents(gdef, node_id)
    if not daddy_ji:
        return True
    bruh = NodeInstance.objects.filter(run=run, node_id__in=daddy_ji, status=NodeInstance.NodeStatus.SUCCEEDED).count()
    return bruh == len(daddy_ji)

def _fire_node(node):
    fire = Fire.objects.create(node_instance=node, attempts=1)
    fire.load_and_fire(node)

def _dispatch(run, node_id, inputs):
    attempt = str(uuid.uuid4())
    with transaction.atomic():
        ni, pos = NodeInstance.objects.select_for_update().get_or_create(run=run, node_id=node_id, defaults={"name": node_id, "inputs": inputs, "attempt_id": attempt})
        ni.status = NodeInstance.NodeStatus.RUNNING
        ni.inputs = inputs or {}
        ni.attempt_id = attempt
        ni.save()
    _fire_node(ni)

def start_run(version, crazy_starting_inputs):
    gdef = GraphDefinition.objects.get(version=version)
    with transaction.atomic():
        run = GraphRun.objects.create(graph=gdef)
        start = gdef.start_node()
        NodeInstance.objects.create(run=run, node_id=start, name=start, status=NodeInstance.NodeStatus.QUEUED, inputs=crazy_starting_inputs or {})
    _dispatch(run, start, crazy_starting_inputs or {})
    return run.run_id

def complete_and_progress(run_id, node_id, attempt_id, outputs, error = None):
    with transaction.atomic():
        run = GraphRun.objects.select_for_update().get(run_id=run_id)
        gdef = run.graph
        nig = NodeInstance.objects.select_for_update().get(run=run, node_id=node_id, attempt_id=attempt_id)

        if nig.status in [NodeInstance.NodeStatus.SUCCEEDED, NodeInstance.NodeStatus.FAILED]:
            return

        nig.status = NodeInstance.NodeStatus.FAILED if error else NodeInstance.NodeStatus.SUCCEEDED
        nig.outputs = {} if error else (outputs or {})
        nig.save()

        ready_children = []
        for child_id in _children(gdef, node_id):
            child, _ = NodeInstance.objects.select_for_update().get_or_create(run=run, node_id=child_id, defaults={"name": child_id})
            child.inputs = _merge_child_inputs(child.inputs, nig.outputs)
            child.save()
            if _ready_to_run(run, child_id):
                ready_children.append(child_id)

        has_pending = NodeInstance.objects.filter(run=run, status__in=[NodeInstance.NodeStatus.QUEUED, NodeInstance.NodeStatus.RUNNING]).exclude(node_id__in=ready_children).exists()

        if not ready_children and not has_pending:
            run.status = GraphRun.Status.FAILED if error else GraphRun.Status.SUCCEEDED
            run.finished_at = timezone.now()
            run.save()
            return

    for cid in ready_children:
        run = GraphRun.objects.get(run_id=run_id)
        child = NodeInstance.objects.get(run=run, node_id=cid)
        _dispatch(run, cid, child.inputs)
