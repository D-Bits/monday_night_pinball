from utilities.extraction import html_extract, team_roster_extract, team_initials
from bs4 import BeautifulSoup
from functools import reduce
import pyspark.sql.functions as f
import pyspark.pipelines as dp
import datetime
import pyspark
import requests


@dp.table(name="bronze.raw_standings")
def raw_standings():

    df = spark.createDataFrame(html_extract("https://mondaynightpinball.com/standings"))

    return df


@dp.table(name="bronze.raw_stats")
def raw_stats():

    df = spark.createDataFrame(html_extract("https://mondaynightpinball.com/stats"))

    return df


@dp.table(name="bronze.raw_team_roster")
def raw_team_roster():

    def create_df(team_initial: str, team_name: str):

        df = spark.createDataFrame(team_roster_extract(team_initial))
        df = df.withColumn("team_inital", f.lit(team_initial))
        df = df.withColumn("team_name", f.lit(team_name))

        return df

    dfs = []

    for team_init, team_name in team_initials.items():
        df = create_df(team_init, team_name)
        df = df.limit(10)
        dfs.append(df)

    df = reduce(pyspark.sql.dataframe.DataFrame.unionByName, dfs)

    return df
