from django.urls import path

from . import views


urlpatterns = [
    path("health/", views.health, name="health"),
    path("standings/", views.standings, name="standings"),
    path("home/summary/", views.home_summary, name="home_summary"),
    path("leaderboard/", views.leaderboard, name="leaderboard"),
    path("predictions/latest/", views.predictions_latest, name="predictions_latest"),
    path("players/search/", views.player_search, name="player_search"),
    path("players/compare/", views.player_compare, name="player_compare"),
    path("players/<str:player_name>/", views.player_detail, name="player_detail"),
    path("teams/<str:team>/", views.team_detail, name="team_detail"),
    path("teams/<str:team>/schedule/", views.team_schedule, name="team_schedule"),
    path("games/", views.games_by_date, name="games_by_date"),
    path("games/<str:game_id>/boxscore/", views.game_boxscore, name="game_boxscore"),
]
