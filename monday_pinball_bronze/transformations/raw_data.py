from utif.lities.extraction import html_extract, team_roster_extract, team_initials
from bs4 import BeautifulSoup
import pyspark.sql.functions as f
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


@dp.table(name="bronze.raw_team")
def raw_team():

    pass


@dp.table(name="bronze.raw_team_roster")
def raw_team_roster():

    def create_df(team_initial: str):

        df = spark.createDataFrame(team_roster_extract(team_initial))
        df = df.withColumn("team_inital", f.lit(team_initial))

        # Assign team names based on team initial
        if team_initial == "ADB":
            df = df.withColumn("team_name", f.lit("Admiraballs"))
        elif team_initial == "BAD":
            df = df.withColumn("team_name", f.lit("Bad Cats"))
        elif team_initial == "BLK":
            df = df.withColumn("team_name", f.lit("Ballard Locks"))
        elif team_initial == "CRA":
            df = df.withColumn("team_name", f.lit("Castle Crashers"))
        elif team_initial == "DTP":
            df = df.withColumn("team_name", f.lit("DTP"))
        elif team_initial == "DSV":
            df = df.withColumn("team_name", f.lit("Death Savers"))
        elif team_initial == "DIH":
            df = df.withColumn("team_name", f.lit("Drain in Hell"))
        elif team_initial == "DRK":
            df = df.withColumn("team_name", f.lit("Drunken Dumplings"))
        elif team_initial == "ETB":
            df = df.withColumn("team_name", f.lit("Eighteen Ball Deluxe"))
        elif team_initial == "FBP":
            df = df.withColumn("team_name", f.lit("Flippin Big Points"))
        elif team_initial == "ICB":
            df = df.withColumn("team_name", f.lit("Incrediballs"))
        elif team_initial == "KNR":
            df = df.withColumn("team_name", f.lit("Knight Riders"))
        elif team_initial == "KBZ":
            df = df.withColumn("team_name", f.lit("Kraken Ball Z"))
        elif team_initial == "RMS":
            df = df.withColumn("team_name", f.lit("Magic Saves"))
        elif team_initial == "JMF":
            df = df.withColumn("team_name", f.lit("Middle Flippers"))
        elif team_initial == "NMC":
            df = df.withColumn("team_name", f.lit("Neuromancers"))
        elif team_initial == "NQG":
            df = df.withColumn("team_name", f.lit("No Quarter Given"))
        elif team_initial == "Northern Lights":
            df = df.withColumn("team_name", f.lit("NLT"))
        elif team_initial == "CPO":
            df = df.withColumn("team_name", f.lit("Pants Optional"))
        elif team_initial == "PYC":
            df = df.withColumn("team_name", f.lit("Pinballycule"))
        elif team_initial == "PGN":
            df = df.withColumn("team_name", f.lit("Pinguins"))
        elif team_initial == "PKT":
            df = df.withColumn("team_name", f.lit("Pocketeers"))
        elif team_initial == "PBR":
            df = df.withColumn("team_name", f.lit("Point Breakers"))
        elif team_initial == "RTR":
            df = df.withColumn("team_name", f.lit("Ramp Tramps"))
        elif team_initial == "SSD":
            df = df.withColumn("team_name", f.lit("Salty Sea Dogs"))
        elif team_initial == "SCN":
            df = df.withColumn("team_name", f.lit("Seacorns"))
        elif team_initial == "SHK":
            df = df.withColumn("team_name", f.lit("Sharks"))
        elif team_initial == "SSS":
            df = df.withColumn("team_name", f.lit("Silver Ball Slayers"))
        elif team_initial == "SKP":
            df = df.withColumn("team_name", f.lit("Slap Kraken Pop"))
        elif team_initial == "SWL":
            df = df.withColumn("team_name", f.lit("Specials When f.lit"))
        elif team_initial == "SRF":
            df = df.withColumn("team_name", f.lit("Surf Champs"))
        elif team_initial == "TBT":
            df = df.withColumn("team_name", f.lit("The B Team"))
        elif team_initial == "POW":
            df = df.withColumn("team_name", f.lit("The Power"))
        elif team_initial == "DOG":
            df = df.withColumn("team_name", f.lit("The Stray Dogs"))
        elif team_initial == "TTT":
            df = df.withColumn("team_name", f.lit("The Trailer Trashers"))
        elif team_initial == "TWC":
            df = df.withColumn("team_name", f.lit("The Wrecking Crew"))
        elif team_initial == "TRL":
            df = df.withColumn("team_name", f.lit("Trolls!"))
        elif team_initial == "ZOO":
            df = df.withColumn("team_name", f.lit("Zoo Crew"))
        else:
            pass

    