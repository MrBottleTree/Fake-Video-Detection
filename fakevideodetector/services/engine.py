import uuid
from django.db import transaction
from django.utils import timezone
from typing import Dict, List
from ..models import GraphDefinition, GraphRun, NodeInstance, Fire

def _parents(gdef, node_id):
    return gdef.depends_on(node_id)

def _children(gdef, node_id):
    return gdef.dependents_of(node_id)

def _merge_child_inputs(existing_child_inputs, parent_outputs):
    merged = dict(existing_child_inputs or {})
    if parent_outputs:
        merged.update(parent_outputs)
    return merged

def _ready_to_run(run, node_id):
    gdef = run.graph
    daddy_jis = _parents(gdef, node_id)
    if not daddy_jis:
        return True
    completed = NodeInstance.objects.filter(run=run, node_id__in=daddy_jis, status=NodeInstance.NodeStatus.SUCCEEDED).count()
    return completed == len(daddy_jis)

def _fire_node(node):
    fire = Fire.objects.create(node_instance=node, attempts=1)
    fire.load_and_fire(node)

def _dispatch(run, node_id, inputs):
    attempt = str(uuid.uuid4())

    inputs = dict(inputs or {})
    if "_callback_url" not in inputs:
        inputs["_callback_url"] = inputs.get("_callback_url", "/callback")
    if "_payload" not in inputs:
        inputs["_payload"] = {k: v for k, v in inputs.items() if not k.startswith("_")}

    with transaction.atomic():
        nig, _ = NodeInstance.objects.select_for_update().get_or_create(run=run, node_id=node_id, defaults={"name": node_id, "inputs": inputs, "attempt_id": attempt})
        nig.status = NodeInstance.NodeStatus.RUNNING
        nig.inputs = inputs
        nig.attempt_id = attempt
        nig.save()

    _fire_node(ni)

def start_run(version, initial_inputs):
    gdef = GraphDefinition.objects.get(version=version)
    with transaction.atomic():
        run = GraphRun.objects.create(graph=gdef)
        start = gdef.start_node()
        NodeInstance.objects.create(run=run, node_id=start, name=start, status=NodeInstance.NodeStatus.QUEUED, inputs=initial_inputs or {})
    _dispatch(run, start, initial_inputs or {})
    return run.run_id


def complete_and_progress(run_id, node_id, attempt_id, outputs, error = None):
    with transaction.atomic():
        run = GraphRun.objects.select_for_update().get(run_id=run_id)
        gdef = run.graph

        node_inst = NodeInstance.objects.select_for_update().get(
            run=run,
            node_id=node_id,
            attempt_id=attempt_id
        )

        if node_inst.status in [NodeInstance.NodeStatus.SUCCEEDED, NodeInstance.NodeStatus.FAILED]:
            return

        node_inst.status = NodeInstance.NodeStatus.FAILED if error else NodeInstance.NodeStatus.SUCCEEDED
        node_inst.outputs = {} if error else (outputs or {})
        node_inst.save()

        ready_children: List[str] = []
        for child_id in _children(gdef, node_id):
            child, _ = NodeInstance.objects.select_for_update().get_or_create(run=run, node_id=child_id, defaults={"name": child_id})
            callback_url = (child.inputs or {}).get("_callback_url") or (node_inst.inputs or {}).get("_callback_url") or "/callback"
            existing_payload = (child.inputs or {}).get("_payload", {})
            new_payload = _merge_child_inputs(existing_payload, node_inst.outputs)

            child.inputs = {
                "_callback_url": callback_url,
                "_payload": new_payload
            }
            child.save()

            if _ready_to_run(run, child_id) and child.status in [NodeInstance.NodeStatus.QUEUED, NodeInstance.NodeStatus.RUNNING]:
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
