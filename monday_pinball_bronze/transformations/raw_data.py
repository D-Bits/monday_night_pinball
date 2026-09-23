from utilities.extraction import html_extract, team_roster_extract
from bs4 import BeautifulSoup
import pyspark.pipelines as dp
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

    def create_df(team_initial: str):

        df = spark.createDataFrame(team_roster_extract(team_initial))
        df = df.withColumn("team_inital", team_initial)

        # Assign team names based on team initial
        if team_initial == "ADB":
            df = df.withColumn("team_name", "Admiraballs")
        elif team_initial == "BAD":
            df = df.withColumn("team_name", "Bad Cats")
        elif team_inital == "" 