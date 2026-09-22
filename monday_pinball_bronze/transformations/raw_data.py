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

    html_src = requests.get("https://mondaynightpinball.com/stats").content
    soup = BeautifulSoup(html_src, "html.parser")
    tables = soup.find_all('table')

    # Extract data from HTML
    for table in tables:
        rows = table.find_all('tr')

        # First row contains the headers
        header_cells = rows[0].find_all(['th', 'td'])
        headers = [cell.text.strip() for cell in header_cells]

        # Remaining rows are data
        table_data = []
        for row in rows[1:]:
            cells = row.find_all('td')
            row_dict = {headers[i]: cell.text.strip() for i, cell in enumerate(cells) if i < len(headers)}
            table_data.append(row_dict)

    # Load all table data into a PySpark DataFrame
    df = spark.createDataFrame(table_data)

    return df