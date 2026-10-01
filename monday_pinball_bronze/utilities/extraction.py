from bs4 import BeautifulSoup
import pyspark.pipelines as dp
import requests


# Create a list of team initials to iterate over and pass into team_roster_extract
team_initials = [
    'ADB',
    'BAD',
    'BLK',
    'CRA',
    'DTP',
    'DSV',
    'DIH',
    'DRK',
    'ETB',
    'FBP',
    'ICB',
    'KNR',
    'KBZ',
    'RMS',
    'JMF',
    'NMC',
    'NQG',
    'NLT',
    'CPO',
    'PYC',
    'PGN',
    'PKT',
    'PBR',
    'RTR',
    'SSD',
    'SCN',
    'SHK',
    'SSS',
    'SKP',
    'SWL',
    'SRF',
    'TBT',
    'POW',
    'DOG',
    'TTT',
    'TWC',
    'TRL',
    'ZOO'
]

# Reusable function to scrape HTML tables and load into dataframe
def html_extract(url: str):

    html_src = requests.get(url).content
    soup = BeautifulSoup(html_src, "html.parser")
    tables = soup.find_all('table')

    # Extract data from HTML
    for table in tables:
        rows = table.find_all('tr')

        # First row contains the headers
        header_cells = rows[0].find_all(['th', 'td'])
        headers = [cell.text.strip() or f"column_{i}" for i, cell in enumerate(header_cells)]

        # Remaining rows are data
        table_data = []
        for row in rows[1:]:
            cells = row.find_all('td')
            row_dict = {headers[i]: cell.text.strip() for i, cell in enumerate(cells) if i < len(headers)}
            table_data.append(row_dict)

    return table_data


# Reusable function to scrape meta data for teams
def team_roster_extract(team_initial: str):

    html_src = requests.get(f"https://mondaynightpinball.com/teams/{team_initial}").content
    soup = BeautifulSoup(html_src, "html.parser")
    table = soup.find(id="team_roster")

    rows = table.find_all('tr')
    # First row contains the headers
    header_cells = rows[0].find_all(['th', 'td'])
    headers = [cell.text.strip() for cell in header_cells]

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

    return table_data

    
def team_info_extract(team_initial: str):

    pass
