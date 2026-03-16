import importlib
import multi_sport_bot

importlib.reload(multi_sport_bot)

# Clear cache and load
multi_sport_bot._nba_stats_cache.clear()
multi_sport_bot._ensure_nba_stats_loaded()

print('loaded teams:', len(multi_sport_bot._nba_stats_cache))
for abbr, s in list(multi_sport_bot._nba_stats_cache.items())[:5]:
    print(abbr, s)
