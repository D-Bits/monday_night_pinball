from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pyspark.pipelines as dp


@dp.table(name="silver.venue")
def venue_preproc():

    df = spark.read.table("bronze.raw_venue")
    win_part = Window.partitionBy("venue_init").orderBy("IPR")
    df = df.withColumn("id", f.dense_rank().over(Window.orderBy(df.venue_name)))
    df = df.select([
        "id",
        "venue_name",
        "venue_init"
    ])

    return df


@dp.table(name="silver.player")
def player_preproc():

    # Map a venue ID to each team
    def assign_venue_id(df, team_initial):

        if team_initial == "ADB":
            df = df.withColumn("venue_id", f.lit(4))
        elif team_initial == "BAD":
            df = df.withColumn("venue_id", f.lit(3))
        elif team_initial == "BLK":
            df = df.withColumn("venue_id", f.lit(6))
        elif team_initial == "CRA":
            df = df.withColumn("venue_id", f.lit(5))
        elif team_initial == "DTP":
            df = df.withColumn("venue_id", f.lit(12))
        elif team_initial == "DSV":
            df = df.withColumn("venue_id", f.lit(3))
        elif team_initial == "DIH":
            df = df.withColumn("venue_id", f.lit(17))
        elif team_initial == "DRK":
            df = df.withColumn("venue_id", f.lit(15))
        elif team_initial == "ETB":
            df = df.withColumn("venue_id", f.lit(20))
        elif team_initial == "FBP":
            df = df.withColumn("venue_id", f.lit(12))
        elif team_initial == "ICB":
            df = df.withColumn("venue_id", f.lit(4))
        elif team_initial == "KNR":
            df = df.withColumn("venue_id", f.lit(9))
        elif team_initial == "KBZ":
            df = df.withColumn("venue_id", f.lit(13))
        elif team_initial == "RMS":
            df = df.withColumn("venue_id", f.lit(18))
        elif team_initial == "JMF":
            df = df.withColumn("venue_id", f.lit(12))
        elif team_initial == "NMC":
            df = df.withColumn("venue_id", f.lit(19))
        elif team_initial == "NQG":
            df = df.withColumn("venue_id", f.lit(10))
        elif team_initial == "NLT":
            df = df.withColumn("venue_id", f.lit(14))
        elif team_initial == "CPO":
            df = df.withColumn("venue_id", f.lit(5))
        elif team_initial == "PYC":
            df = df.withColumn("venue_id", f.lit(11))
        elif team_initial == "PGN":
            df = df.withColumn("venue_id", f.lit(2))
        elif team_initial == "PKT":
            df = df.withColumn("venue_id", f.lit(7))
        elif team_initial == "PBR":
            df = df.withColumn("venue_id", f.lit(3))
        elif team_initial == "RTR":
            df = df.withColumn("venue_id", f.lit(14))
        elif team_initial == "SSD":
            df = df.withColumn("venue_id", f.lit(6))
        elif team_initial == "SCN":
            df = df.withColumn("venue_id", f.lit(9))
        elif team_initial == "SHK":
            df = df.withColumn("venue_id", f.lit(21))
        elif team_initial == "SSS":
            df = df.withColumn("venue_id", f.lit(17))
        elif team_initial == "SKP":
            df = df.withColumn("venue_id", f.lit(13))
        elif team_initial == "SWL":
            df = df.withColumn("venue_id", f.lit(2))
        elif team_initial == "SRF":
            df = df.withColumn("venue_id", f.lit(8))
        elif team_initial == "TBT":
            df = df.withColumn("venue_id", f.lit(1))
        elif team_initial == "POW":
            df = df.withColumn("venue_id", f.lit(11))
        elif team_initial == "DOG":
            df = df.withColumn("venue_id", f.lit(10))
        elif team_initial == "TTT":
            df = df.withColumn("venue_id", f.lit(16))
        elif team_initial == "TWC":
            df = df.withColumn("venue_id", f.lit(9))
        elif team_initial == "TRL":
            df = df.withColumn("venue_id", f.lit(1))
        elif team_initial == "ZOO":
            df = df.withColumn("venue_id", f.lit(22))
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


@dp.table(name="silver.machine")
def machine_preproc():

    df = spark.read.table("bronze.raw_venue_machine")
    df = df.drop_duplicates(["machine_name"])
    win_part = Window.partitionBy("machine_name").orderBy("machine_name")
    df = df.withColumn("id", f.dense_rank().over(Window.orderBy(df.machine_name)))
    df = df.select([
        "id",
        "machine_name"
    ])

    return df


# Create a linking table between venue and machine
@dp.table(name="silver.machine_venue_id")
def machine_venue_id():

    df = spark.read.table("monday_pinball.bronze.raw_venue_machine")
    # Assign venue ids
    df = df.withColumn("venue_id",
        f.when(f.col("venue_init") == "T4B", f.lit(1))
        .when(f.col("venue_init") == "8BT", f.lit(2))
        .when(f.col("venue_init") == "AAB", f.lit(3))
        .when(f.col("venue_init") == "ADM", f.lit(4))
        .when(f.col("venue_init") == "ANC", f.lit(5))
        .when(f.col("venue_init") == "BSS", f.lit(6))
        .when(f.col("venue_init") == "COP", f.lit(7))
        .when(f.col("venue_init") == "GRY", f.lit(8))
        .when(f.col("venue_init") == "GPA", f.lit(9))
        .when(f.col("venue_init") == "HND", f.lit(10))
        .when(f.col("venue_init") == "IBX", f.lit(11))
        .when(f.col("venue_init") == "JUP", f.lit(12))
        .when(f.col("venue_init") == "KRA", f.lit(13))
        .when(f.col("venue_init") == "OLF", f.lit(14))
        .when(f.col("venue_init") == "RSW", f.lit(15))
        .when(f.col("venue_init") == "STN", f.lit(16))
        .when(f.col("venue_init") == "SHR", f.lit(17))
        .when(f.col("venue_init") == "TLT", f.lit(18))
        .when(f.col("venue_init") == "TWP", f.lit(19))
        .when(f.col("venue_init") == "TDN", f.lit(20))
        .when(f.col("venue_init") == "WAT", f.lit(21))
        .when(f.col("venue_init") == "ZTE", f.lit(22))
    )

    # Assign machine ids
    df = df.dropDuplicates(["machine_name"])

    df = df.sort(f.asc("machine_name"))
    df = df.withColumn("machine_id", f.dense_rank().over(Window.orderBy(df.machine_name)))

    # Add an id for each record
    df = df.withColumn("id", f.monotonically_increasing_id())
    df = df.select([
        "id",
        "venue_id",
        "machine_id"
    ])

    return df
