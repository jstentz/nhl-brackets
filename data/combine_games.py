import os
import polars as pl

folder = 'games/'
dfs = [ ]
for game_file in os.listdir(folder):
  path = folder + game_file
  df = pl.read_parquet(path)
  df = df.with_columns(pl.lit(game_file.split('_')[-1].split('.')[0]).alias('season'))
  dfs.append(df)

combined: pl.DataFrame = pl.concat(dfs)
combined.write_parquet('games/games.parquet')