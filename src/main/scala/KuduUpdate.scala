import org.apache.spark.sql.SparkSession
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import collection.JavaConverters._
import org.apache.kudu.client.KuduClient

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object KuduUpdate {


  def main(args: Array[String]): Unit = {
    println("ok")
    //val kuduMasters: String = System.getProperty("kuduMasters", "192.168.99.100:7051")
    val kuduMasters: String = System.getProperty("kuduMasters", "localhost:32769")

    // The name of a table to create.
    val tableName: String = System.getProperty("tableName", "kudu_spark_example")

    val spark = SparkSession.builder.appName("KuduSparkExample").master("local[2]").getOrCreate()

    val client = new KuduClient.KuduClientBuilder(kuduMasters).build
    val kuduContext = new KuduContext(kuduMasters, spark.sqlContext.sparkContext)


    import spark.implicits._

    // The schema of the table we're going to create.
    val nameCol = "name"
    val idCol = "id"

    val schema = StructType(
      List(
        StructField(idCol, IntegerType, false),
        StructField(nameCol, StringType, false)
      )
    )
    if (!client.getTablesList(tableName).getTablesList.isEmpty) {
      System.out.println("deleting table if exists...")
      //client.deleteTable(tableName)
    }

    var tableIsCreated = false
    // Make sure the table does not exist. This is mostly to demonstrate
      // the capabilities of the API. In general, there might be a racing
      // request to create the table coming from elsewhere, so even
      // if tableExists() returned false at this time, the table might appear
      // while createTable() is running below. In the latter case, appropriate
      // Kudu exception will be thrown by createTable().
      if (kuduContext.tableExists(tableName)) {
        //throw new RuntimeException(tableName + ": table already exists")

      }




      // each with the specified number of replicas.
    /*  kuduContext.createTable(tableName, schema, Seq(idCol),
        new CreateTableOptions()
          .addHashPartitions(List(idCol).asJava, 3)
          .setNumReplicas(1))
      tableIsCreated = true*/



   // Write to the table.
    //logger.info(s"writing to table '$tableName'")
    val data = Array(User("userA", 1234), User("userB", 5678))
    val userRDD = spark.sparkContext.parallelize(data)
    val userDF = userRDD.toDF()
    kuduContext.insertRows(userDF, tableName)

  }

  case class User(name:String, id:Int)
}
