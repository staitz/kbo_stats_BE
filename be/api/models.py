from django.db import models

class ErrorReport(models.Model):
    STATUS_CHOICES = (
        ('new', 'New'),
        ('reviewed', 'Reviewed'),
        ('resolved', 'Resolved'),
    )
    
    page = models.CharField(max_length=50)
    tab = models.CharField(max_length=50, blank=True)
    issue_type = models.CharField(max_length=50)
    message = models.TextField()
    reported_url = models.CharField(max_length=500, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='new')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"[{self.status.upper()}] {self.issue_type} on {self.page} ({self.created_at.strftime('%Y-%m-%d %H:%M')})"

# Create your models here.
