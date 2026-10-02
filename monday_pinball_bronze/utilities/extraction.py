from bs4 import BeautifulSoup
import pyspark.pipelines as dp
import requests


# Create a list of team initials to iterate over and pass into team_roster_extract
team_initials = {
    "ADB": "Admiraballs",
    "BAD": "Bad Cats",
    "BLK": "Ballard Locks",
    "CRA": "Castle Crashers",
    "DTP": "DTP",
    "DSV": "Death Savers",
    "DIH": "Drain in Hell",
    "DRK": "Drunken Dumplings",
    "ETB": "Eighteen Ball Deluxe",
    "FBP": "Flippin Big Points",
    "ICB": "Incrediballs",
    "KNR": "Knight Riders",
    "KBZ": "Kraken Ball Z",
    "RMS": "Magic Saves",
    "JMF": "Middle Flippers",
    "NMC": "Neuromancers",
    "NQG": "No Quarter Given",
    "NLT": "Northern Lights",
    "CPO": "Pants Optional",
    "PYC": "Pinballycule",
    "PGN": "Pinguins",
    "PKT": "Poketeers",
    "PBR": "Point Breakers",
    "RTR": "Ramp Tramps",
    "SSD": "Salty Sea Dogs",
    "SCN": "Seacorns",
    "SHK": "Sharks",
    "SSS": "Silver Ball Slayers",
    "SKP": "Slap Kraken Pop",
    "SWL": "Specials When Lit",
    "SRF": "Surf Champs",
    "TBT": "The B Team",
    "POW": "The Power",
    "DOG": "The Stray Dogs",
    "TTT": 'The Trailer Trashers',
    "TWC": "The Wrecking Crew",
    "TRL": "Trolls!",
    "ZOO": "Zoo Crew",
}

venue_initials = {
    "T4B": "4Bs Tavern",
    "8BT": "8-Bit Arcade Bar",
    "AAB": "Add-a-Ball",
    "ADM": "Admiral Pub",
    "ANC": "Another Castle",
    "BSS": "Ballard Smoke Shop",
    "COP": "Corner Pocket Billiards and Lounge",
    "GRY": "Gary's Place",
    "GPA": "Georgetown Pizza and Arcade",
    "HND": "Hounds Tooth",
    "IBX": "Ice Box",
    "JUP": "Jupiter",
    "KRA": "Kraken",
    "OLF": "Olaf's",
    "RSW": "Rickshaw",
    "STN": "Seattle Tavern and Pool Hall",
    "SHR": "Shorty's",
    "TLT": "Tilted Table",
    "TWP": "Time Warp",
    "TDN": "Touchdown",
    "WAT": "Waterland",
    "ZTE": "Zoo Tavern",
}



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
    # Normalize roster column name (varies by team size: "Roster (9 Players)", "Roster (10 Players)", etc.)
    headers = ["Roster" if h.startswith("Roster") else h for h in headers]

    # Remaining rows are data
    table_data = []

    for row in rows[1:]:
        cells = row.find_all('td')
        row_dict = {headers[i]: cell.text.strip() for i, cell in enumerate(cells) if i < len(headers)}
        table_data.append(row_dict)

    return table_data


def team_info_extract(team_initial: str):

    pass
