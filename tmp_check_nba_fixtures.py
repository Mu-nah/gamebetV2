import importlib
import multi_sport_bot
importlib.reload(multi_sport_bot)

# Force stats load to ensure caching in file
multi_sport_bot._nba_stats_cache.clear()
multi_sport_bot._ensure_nba_stats_loaded()

# Fetch today's NBA games
games = multi_sport_bot.fetch_nba_fixtures()
print('games', len(games))
for g in games[:5]:
    print(g['home_team'], 'vs', g['away_team'], 'kickoff', g.get('kickoff'))
