from django.urls import path
from . import views

urlpatterns = [
    path('start/', views.start_graph, name='start_graph'),
    path('callback/', views.node_callback, name='node_callback'),
]
