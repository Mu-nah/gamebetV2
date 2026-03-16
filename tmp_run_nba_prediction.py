import importlib
import multi_sport_bot
import basketball_predictor

importlib.reload(multi_sport_bot)
importlib.reload(basketball_predictor)

multi_sport_bot._nba_stats_cache.clear()
multi_sport_bot._ensure_nba_stats_loaded()

games = multi_sport_bot.fetch_nba_fixtures()
print('games', len(games))

pred = basketball_predictor.BasketballPredictor()
for g in games:
    hs = multi_sport_bot.fetch_nba_team_season_stats(g.get('home_id'), team_name=g.get('home_team'))
    aws = multi_sport_bot.fetch_nba_team_season_stats(g.get('away_id'), team_name=g.get('away_team'))
    out = pred.predict(g, home_stats=hs, away_stats=aws, api_win_prob=g.get('win_prob'))
    print('---')
    print(g['home_team'], 'vs', g['away_team'])
    print('home stats', hs)
    print('away stats', aws)
    print('prediction', out)
