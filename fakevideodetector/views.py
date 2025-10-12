# views.py
import json
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from .services.engine import start_run, complete_and_progress

@require_POST
@csrf_exempt
def start_graph(request):
    try:
        data = json.loads(request.body or "{}")
        version = data.get("version", "1.0")
        inputs = data.get("inputs", {})
    except Exception:
        print("what cockshit man:", request.body)
        return HttpResponseBadRequest("send properly n")

    run_id = start_run(version, inputs)
    return JsonResponse({"ok": True, "run_id": run_id})

@require_POST
@csrf_exempt
def node_callback(request):
    try:
        data = json.loads(request.body or "{}")
        run_id = int(data["run_id"])
        node_id = data["node_id"]
        attempt_id = data["attempt_id"]
        result = data.get("result") or data.get("outputs") or {}
        error = data.get("error")
    except Exception:
        print("what cockshit man:", request.body)
        return HttpResponseBadRequest("bro send properly nigger")

    complete_and_progress(run_id, node_id, attempt_id, outputs=result, error=error)
    return JsonResponse({"ok": True})
