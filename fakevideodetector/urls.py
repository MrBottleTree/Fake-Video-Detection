from django.urls import path
from . import views
from django.shortcuts import redirect

urlpatterns = [
    path('start/', views.start_graph, name='start_graph'),
    path('callback/', views.node_callback, name='node_callback'),
    path("designer/", views.graph_designer, name="graph_designer"),
    path("api/definitions/", views.graph_list, name="graph_list"),
    path("api/definitions/<str:version>/", views.graph_get_or_save, name="graph_get_or_save"),
    path('', lambda request: redirect('graph_designer')),
    path('shutdown/', views.shutdown_server, name='shutdown_server'),
]
