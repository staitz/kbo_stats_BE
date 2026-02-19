from django.urls import path

from . import views


urlpatterns = [
    path("health", views.health, name="health"),
    path("leaderboard", views.leaderboard, name="leaderboard"),
    path("predictions/latest", views.predictions_latest, name="predictions_latest"),
    path("players/search", views.player_search, name="player_search"),
]
