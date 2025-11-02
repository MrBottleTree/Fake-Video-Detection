import json
from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.urls import reverse
from django.utils.timezone import now
from .services.engine import start_run, complete_and_progress
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from .models import GraphDefinition
import os
import signal

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

    callback_url = request.build_absolute_uri(reverse("node_callback")).replace("http://", "https://", 1)
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
    result = data.get("_payload") or {}
    error = data.get("error")

    if result is not None and not isinstance(result, dict):
        return HttpResponseBadRequest("quark :)")
    complete_and_progress(run_id, node_id, attempt_id, result, error=error)
    return JsonResponse({"ok": True, "ts": now().isoformat()})

def graph_designer(request):
    return render(request, "fakevideodetector/designer.html")

@require_http_methods(["GET"])
def graph_list(request):
    versions = GraphDefinition.objects.values_list("version", flat=True).order_by("-id")
    return JsonResponse({"versions": list(versions)})

@require_http_methods(["GET", "POST"])
@csrf_exempt
def graph_get_or_save(request, version):
    if request.method == "GET":
        try:
            graph_def = GraphDefinition.objects.get(version=version)
            return JsonResponse({
                "version": version,
                "spec": graph_def.spec
            })
        except GraphDefinition.DoesNotExist:
            return JsonResponse({"error": "Not found"}, status=404)
    
    elif request.method == "POST":
        try:
            body = json.loads(request.body)
            spec = body.get("spec")
            if not spec:
                return JsonResponse({"error": "Missing spec"}, status=400)
            
            graph_def, created = GraphDefinition.objects.update_or_create(
                version=version,
                defaults={
                    "spec": spec,
                    "description": "",
                }
            )
            return JsonResponse({"ok": True, "created": created})
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

@require_http_methods(["GET"])
def shutdown_server(request):
    os.kill(os.getpid(), signal.SIGINT)
    return HttpResponse("Server is shutting down...")