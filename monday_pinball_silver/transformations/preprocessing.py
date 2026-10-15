from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pyspark.pipelines as dp


@dp.table(name="silver.venue")
def venue_preproc():

    df = spark.read.table("bronze.raw_venue")
    win_part = Window.partitionBy("venue_init").orderBy("IPR")
    df = df.withColumn("id", f.dense_rank().over(Window.orderBy("venue_name")))
    df = df.select([
        "id",
        "venue_name",
        "venue_init"
    ])

    return df




@dp.table(name="silver.player")
def player_preproc():

    df = spark.read.table("monday_pinball.bronze.raw_team_roster")
    # Assign each player a team ID
    win_part = Window.partitionBy("team_name").orderBy("IPR")
    df = df.withColumn("id", f.monotonically_increasing_id())

    df = df.withColumn("team_id", f.dense_rank().over(Window.orderBy("team_name")))

    # Map a venue ID to each team
    df = df.withColumn("venue_id",
        f.when(f.col("team_inital") == "ADB", f.lit(4))
        .when(f.col("team_inital") == "BAD", f.lit(3))
        .when(f.col("team_inital") == "BLK", f.lit(6))
        .when(f.col("team_inital") == "CRA", f.lit(5))
        .when(f.col("team_inital") == "DTP", f.lit(12))
        .when(f.col("team_inital") == "DSV", f.lit(3))
        .when(f.col("team_inital") == "DIH", f.lit(17))
        .when(f.col("team_inital") == "DRK", f.lit(15))
        .when(f.col("team_inital") == "ETB", f.lit(20))
        .when(f.col("team_inital") == "FBP", f.lit(12))
        .when(f.col("team_inital") == "ICB", f.lit(4))
        .when(f.col("team_inital") == "KNR", f.lit(9))
        .when(f.col("team_inital") == "KBZ", f.lit(13))
        .when(f.col("team_inital") == "RMS", f.lit(18))
        .when(f.col("team_inital") == "JMF", f.lit(12))
        .when(f.col("team_inital") == "NMC", f.lit(19))
        .when(f.col("team_inital") == "NQG", f.lit(10))
        .when(f.col("team_inital") == "NLT", f.lit(14))
        .when(f.col("team_inital") == "CPO", f.lit(5))
        .when(f.col("team_inital") == "PYC", f.lit(11))
        .when(f.col("team_inital") == "PGN", f.lit(2))
        .when(f.col("team_inital") == "PKT", f.lit(7))
        .when(f.col("team_inital") == "PBR", f.lit(3))
        .when(f.col("team_inital") == "RTR", f.lit(14))
        .when(f.col("team_inital") == "SSD", f.lit(6))
        .when(f.col("team_inital") == "SCN", f.lit(9))
        .when(f.col("team_inital") == "SHK", f.lit(21))
        .when(f.col("team_inital") == "SSS", f.lit(17))
        .when(f.col("team_inital") == "SKP", f.lit(13))
        .when(f.col("team_inital") == "SWL", f.lit(2))
        .when(f.col("team_inital") == "SRF", f.lit(8))
        .when(f.col("team_inital") == "TBT", f.lit(1))
        .when(f.col("team_inital") == "POW", f.lit(11))
        .when(f.col("team_inital") == "DOG", f.lit(10))
        .when(f.col("team_inital") == "TTT", f.lit(16))
        .when(f.col("team_inital") == "TWC", f.lit(9))
        .when(f.col("team_inital") == "TRL", f.lit(1))
        .when(f.col("team_inital") == "ZOO", f.lit(22))
    )

    # Rename columns
    df = df.withColumnRenamed("IPR", "ipr")
    df = df.withColumnRenamed("M", "m")
    df = df.withColumnRenamed("POPS", "pops")
    df = df.withColumnRenamed("PPM", "ppm")
    df = df.withColumnRenamed("Roster", "player_name")

    df = df.select([
        "id",
        "team_id",
        "venue_id",
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

    df = df.withColumn("team_id", f.dense_rank().over(Window.orderBy("team_name")))
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
