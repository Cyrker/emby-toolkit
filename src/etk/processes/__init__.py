"""Independent ETK API-adjacent process roles."""

from .celery_app import create_celery_app

__all__ = ["create_celery_app"]
