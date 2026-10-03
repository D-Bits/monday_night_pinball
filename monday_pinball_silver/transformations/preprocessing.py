from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pyspark.pipelines as dp


@dp.table(name="silver.venue")
def venue_preproc():

    df = spark.createDatarame("bronze.raw_venue")
    win_part = Window.partitionBy("venue_init").orderBy("IPR")
    df = df.withColumn("venue_id", f.dense_rank().over(Window.orderBy(df.venue_name)))
    df = df.withColumn("id", f.monotonically_increasing_id())


    return df


@dp.table(name="silver.player")
def player_preproc():

    # Assign a venue ID to each team
    def assign_venue_id(df, team_id):

        if team_id == 1:
            df.withColum("venue_id", f.lit(4))
        else:
            pass

        return df


    df = spark.read.table("bronze.raw_team_roster")
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
