from nhlpy.nhl_client import NHLClient
import polars as pl
client = NHLClient(verbose=True)

start_season: int = 2024
end_season: int = 2024
seasons: list[str] = [f'{season}{season+1}' for season in range(start_season, end_season + 1)]

for season in seasons:
  standings = client.standings.get_standings(season=season)['standings'] if season != '20242025' else client.standings.get_standings(date='now')['standings']
  teams: list[str] = [team['teamAbbrev']['default'] for team in standings]

  games_data: list[dict] = [ ]
  seen_game_ids: set[int] = set()

  for team in teams:
      games: list[dict] = client.schedule.get_season_schedule(team, season)['games']
      print(season, team, len(games))

      for game in games:
          game_id: int = game['id']
          game_type: int = game['gameType']

          # only care about playoff and regular season
          # if game_type != 2 and game_type != 3:
          if game_type != 2:
              continue

          if game_id in seen_game_ids:
              continue

          seen_game_ids.add(game_id)
          data = client.game_center.game_story(game_id)
          team_summary = data['summary']['teamGameStats']

          # Make stuff easier to parse
          row = {}
          row['gameId'] = game_id
          row['gameType'] = game_type
          row['homeTeam'] = data['homeTeam']['abbrev']
          row['awayTeam'] = data['awayTeam']['abbrev']
          row['homeScore'] = data['homeTeam']['score']
          row['awayScore'] = data['awayTeam']['score']

          for item in team_summary:
              category = item["category"]
              row[f"home{category[0].upper()}{category[1:]}"] = item["homeValue"]
              row[f"away{category[0].upper()}{category[1:]}"] = item["awayValue"]

          games_data.append(row)

  # Save each season
  game_stats = pl.DataFrame(games_data)
  game_stats.write_parquet(f'games/games_{season}.parquet')

