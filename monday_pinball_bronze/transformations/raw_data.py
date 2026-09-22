from utilities.extraction import html_extract
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


