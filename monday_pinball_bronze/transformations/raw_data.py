from utilities.extraction import html_extract, team_roster_extract, team_initials, venue_initials
from bs4 import BeautifulSoup
from functools import reduce
from pyspark.sql import Row
import pyspark.sql.functions as f
import pyspark.pipelines as dp
import datetime
import pyspark
import requests


@dp.table(name="bronze.raw_standingsI")
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


@dp.table(name="bronze.raw_venue")
def raw_venue():

    df = spark.createDataFrame(
        [(k, v) for k, v in venue_initials.items()],
        schema="venue_init STRING, venue_name STRING"
    )

    return df


@dp.table(name="bronze.raw_machine")
def raw_machine():

    data = html_extract("https://mondaynightpinball.com/machines")
    df = spark.createDataFrame(data)

    # Create a single-row DataFrame from the column names, then union with the original
    header_row = spark.createDataFrame([Row(*df.columns)], schema=df.schema)
    df = header_row.unionByName(df)
    # Rename columns
    df = df.withColumnRenamed("AC/DC", "machine_name").withColumnRenamed("ACDC", "machine_abbrv")

    return df
