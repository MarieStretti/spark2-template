package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)

  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val pop = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")
    pop.show

    pop
      .agg(sum($"Population"))
      .show

    val agg = pop
      .groupBy($"Departement")
      .agg(sum($"Population")
        .as("totalPop"))
      .orderBy($"totalPop".desc)

    agg
      .show

    val dep = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .csv("data/input/departements.txt")
      .select($"_c0".as("DepartementName"),$"_c1".as("Departement"))
    dep.show()

    agg.join(dep,Seq("Departement")).show
  }
}
