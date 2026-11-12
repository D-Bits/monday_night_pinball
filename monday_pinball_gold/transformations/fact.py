import pyspark.sql.functions as f
import pyspark.pipelines as dp


@dp.table(name="gold.fact_team")
def fact_team():

    df = spark.read.table("silver.team").select([
      "team_id",
      "team_name",
      "team_initial"
    ])
    df = df.withColumn("id", f.monotonically_increasing_id())
    df = df.select([
      "id",
      "team_id",
      "team_name",
      "team_initial",
    ])

    return df
