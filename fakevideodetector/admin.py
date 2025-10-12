from django.contrib import admin
from .models import *

admin.site.register(GraphDefinition)
admin.site.register(GraphRun)
admin.site.register(NodeInstance)
admin.site.register(Fire)