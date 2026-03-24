from django.contrib import admin
from .models import ErrorReport

# Register your models here.

@admin.register(ErrorReport)
class ErrorReportAdmin(admin.ModelAdmin):
    list_display = ('id', 'issue_type', 'page', 'tab', 'status', 'created_at')
    list_filter = ('status', 'issue_type', 'page', 'tab', 'created_at')
    search_fields = ('message', 'reported_url')
    readonly_fields = ('created_at',)
