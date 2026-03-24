from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from .models import ErrorReport

class StatusFilter(admin.SimpleListFilter):
    title = _('Status')
    parameter_name = 'status_filter'

    def lookups(self, request, model_admin):
        return (
            ('all', _('All')),
            ('new', _('New')),
            ('reviewed', _('Reviewed')),
            ('resolved', _('Resolved')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'all':
            return queryset
        if self.value() in ('new', 'reviewed', 'resolved'):
            return queryset.filter(status=self.value())
        # Default behavior: exclude resolved
        return queryset.exclude(status='resolved')

@admin.register(ErrorReport)
class ErrorReportAdmin(admin.ModelAdmin):
    list_display = ('id', 'issue_type', 'page', 'tab', 'status', 'created_at')
    list_filter = (StatusFilter, 'issue_type', 'page', 'tab', 'created_at')
    search_fields = ('message', 'reported_url')
    readonly_fields = ('created_at',)
