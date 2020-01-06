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
      .option("mode", "PERMISSIVE")
      .csv("data/input/departements.txt")
      .select($"_c0".as("DepartementName"),$"_c1".as("Departement"))
    dep.show()

    agg.join(dep,Seq("Departement")).show
  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s7 = spark.read
      .option("mode", "PERMISSIVE")
      .option("delimiter", "\\t")
      .csv("data/input/sample_07")
        .select($"_c0".as("code"),$"_c1".as("description"),$"_c2".as("nbPeople7"),$"_c3".as("salary7"))
    s7.show

    val s8 = spark.read
      .option("mode", "PERMISSIVE")
      .option("delimiter", "\\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("code"),$"_c1".as("description"),$"_c2".as("nbPeople8"),$"_c3".as("salary8"))
    s8.show

    s7
    .select($"description",$"salary7")
      .filter($"salary7">100000)
    .orderBy($"salary7".desc)
      .limit(10)
    .show

    s7
      .join(s8,Seq("code"))
      .withColumn("growth", $"salary8" - $"salary7")
    .filter($"salary7" < $"salary8")
    .orderBy($"growth".desc)
      .limit(50)
      .show

    s7
      .join(s8,Seq("code"))
      .withColumn("loss", $"nbPeople8" - $"nbPeople7")
    .filter($"salary7" > 100000)
    .orderBy($"loss".asc)
    .limit(50)
      .drop(s8("description"))
      .drop("salary7")
      .drop("salary8")
      .drop("code")
      .show
  }
}
