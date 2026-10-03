from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pyspark.pipelines as dp


@dp.table(name="silver.venue")
def venue_preproc():

    df = spark.read.table("bronze.raw_venue")
    win_part = Window.partitionBy("venue_init").orderBy("IPR")
    df = df.withColumn("id", f.dense_rank().over(Window.orderBy(df.venue_name)))
    df = df.withColumn("id", f.monotonically_increasing_id())
    df = df.select([
        "id",
        "venue_name",
        "venue_init"
    ])

    return df


@dp.table(name="silver.player")
def player_preproc():

    # Assign a venue ID to each team
    def assign_venue_id(df, team_initial):

        if team_initial == "ADB":
            df.withColum("venue_id", f.lit(4))
        elif team_initial == "BAD":
            df.withColum("venue_id", f.lit(3))
        elif team_initial == "BLK":
            df.withColum("venue_id", f.lit(6))
        elif team_initial == "CRA":
            df.withColum("venue_id", f.lit(5))
        elif team_initial == "DTP":
            df.withColum("venue_id", f.lit(12))
        elif team_initial == "DSV":
            df.withColum("venue_id", f.lit(3))
        elif team_initial == "DIH":
            df.withColum("venue_id", f.lit(17))
        elif team_initial == "DRK":
            df.withColum("venue_id", f.lit(15))
        elif team_initial == "ETB":
            df.withColum("venue_id", f.lit(20))
        elif team_initial == "FBP":
            df.withColum("venue_id", f.lit(12))
        elif team_initial == "ICB":
            df.withColum("venue_id", f.lit(4))
        elif team_initial == "KNR":
            df.withColum("venue_id", f.lit(9))
        elif team_initial == "KBZ":
            df.withColum("venue_id", f.lit(13))
        elif team_initial == "RMS":
            df.withColum("venue_id", f.lit(18))
        elif team_initial == "JMF":
            df.withColum("venue_id", f.lit(12))
        elif team_initial == "NMC":
            df.withColum("venue_id", f.lit(19))
        elif team_initial == "NQG":
            df.withColum("venue_id", f.lit(10))
        elif team_initial == "NLT":
            df.withColum("venue_id", f.lit(14))
        elif team_initial == "CPO":
            df.withColum("venue_id", f.lit(5))
        elif team_initial == "PYC":
            df.withColum("venue_id", f.lit(11))
        elif team_initial == "PGN":
            df.withColum("venue_id", f.lit(2))
        elif team_initial == "PKT":
            df.withColum("venue_id", f.lit(7))
        elif team_initial == "PBR":
            df.withColum("venue_id", f.lit(3))
        elif team_initial == "RTR":
            df.withColum("venue_id", f.lit(14))
        elif team_initial == "SSD":
            df.withColum("venue_id", f.lit(6))
        elif team_initial == "SCN":
            df.withColum("venue_id", f.lit(9))
        elif team_initial == "SHK":
            df.withColum("venue_id", f.lit(21))
        elif team_initial == "SSS":
            df.withColum("venue_id", f.lit(17))
        elif team_initial == "SKP":
            df.withColum("venue_id", f.lit(13))
        elif team_initial == "SWL":
            df.withColum("venue_id", f.lit(2))
        elif team_initial == "SRF":
            df.withColum("venue_id", f.lit(8))
        elif team_initial == "TBT":
            df.withColum("venue_id", f.lit(1))
        elif team_initial == "POW":
            df.withColum("venue_id", f.lit(11))
        elif team_initial == "DOG":
            df.withColum("venue_id", f.lit(10))
        elif team_initial == "TTT":
            df.withColum("venue_id", f.lit(16))
        elif team_initial == "TWC":
            df.withColum("venue_id", f.lit(9))
        elif team_initial == "TRL":
            df.withColum("venue_id", f.lit(1))
        elif team_initial == "ZOO":
            df.withColum("venue_id", f.lit(22))
        else:
            pass

        return df

    df = spark.read.table("monday_pinball.bronze.raw_team_roster")
    # Assign each player a team ID
    win_part = Window.partitionBy("team_name").orderBy("IPR")
    df = df.withColumn("id", f.monotonically_increasing_id())

    df = df.withColumn("team_id", f.dense_rank().over(Window.orderBy(df.team_name)))
    # Rename columns
    df = df.withColumnRenamed("IPR", "ipr")
    df = df.withColumnRenamed("M", "m")
    df = df.withColumnRenamed("POPS", "pops")
    df = df.withColumnRenamed("PPM", "ppm")
    df = df.withColumnRenamed("Roster", "player_name")

    df = df.select([
        "id",
        "team_id",
        "player_name",
        "ipr",
        "m",
        "pops",
        "ppm"
    ])

    return df


@dp.table(name="silver.team")
def team_preproc():

    df = spark.read.table("bronze.raw_team_roster")
    # Assign each player a team ID
    win_part = Window.partitionBy("team_name").orderBy("IPR")
    df = df.withColumn("id", f.monotonically_increasing_id())

    df = df.withColumn("team_id", f.dense_rank().over(Window.orderBy(df.team_name)))
    # Drop duplicate team ids
    df = df.drop_duplicates(["team_id"])
    # Drop unnecessary fields
    df = df.select([
        "id",
        "team_id",
        "team_name",
        "team_inital"
    ])

    return df
