# views.py
import json
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.urls import reverse
from django.utils.timezone import now
from .services.engine import start_run, complete_and_progress

def _json(request):
    try:
        return json.loads(request.body or "{}")
    except Exception:
        return None

@require_POST
@csrf_exempt
def start_graph(request):
    data = _json(request)
    if data is None:
        return HttpResponseBadRequest("Invalid JSON")

    version = data.get("version", "1.0")
    inputs = data.get("inputs", {})

    callback_url = request.build_absolute_uri(reverse("node_callback"))

    inputs["_callback_url"] = callback_url
    run_id = start_run(version, inputs)
    return JsonResponse({"ok": True, "run_id": run_id, "callback": callback_url})

@require_POST
@csrf_exempt
def node_callback(request):
    data = _json(request)
    if data is None:
        return HttpResponseBadRequest("Invalid JSON")

    missing = [k for k in ("run_id","node_id","attempt_id") if k not in data]
    if missing:
        return HttpResponseBadRequest(f"Missing fields: {', '.join(missing)}")

    run_id = int(data["run_id"])
    node_id = data["node_id"]
    attempt_id = data["attempt_id"]
    result = data.get("result") or data.get("outputs") or {}
    error = data.get("error")

    if result is not None and not isinstance(result, dict):
        return HttpResponseBadRequest("Field 'result' must be an object")

    complete_and_progress(run_id, node_id, attempt_id, outputs=result, error=error)
    return JsonResponse({"ok": True, "ts": now().isoformat()})