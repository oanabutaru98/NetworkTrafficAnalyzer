import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import scala.concurrent.duration._
import com.datastax.driver.core.Cluster
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
object task2 {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    // set username and password of cassandra as well
    val spark = SparkSession
      .builder()
      .appName("Rate Source")
      .config("spark.cassandra.connection.host", "10.128.0.5")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra") 
      .master("local[*]")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    // Create Streaming DataFrame by reading data from socket.
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "final")
      .load()
    //Split the value and Cast it into two different columns 
    val qwerty1 = df.selectExpr(
                                "split(value, ',')[0]  as unixtime",
                                "split(value, ',')[1] as miliseconds",
                                "split(value, ',')[2] as ip_source",
                                "split(value, ',')[3] as ip_destination",
                                "split(value, ',')[4] as sport",
                                "split(value, ',')[5] as dport",
                                "CAST( (split(value, ',')[6] ) as int) as framesize"
                               )
    //GroupBy unixtime, ipSource and ipDestination. 
    //The groupby unixtime is because there are many events per second. 
    val gBy_ipS_data = qwerty1.groupBy("unixtime" , "ip_source", "ip_destination").agg(
      avg("framesize").as("pckgavg"),
      min("framesize").as("pckgmin"),
      max("framesize").as("pckgmax"),
      count("framesize").as("pckgcount"))

    //Shows in console 
   // gBy_ipS_data.writeStream.outputMode("complete").format("console").start()

    //save in cassandra in final_space.metric table
     val query1 = gBy_ipS_data 
        .writeStream
        .foreachBatch( (batchDF: DataFrame, batchID: Long) => {
          println(s"Writing to Cassandra $batchID")
          batchDF.write
                 .cassandraFormat("metric", "final_space") // table, keyspace
                 .mode("append")
                 .save()
                }
              )
        .outputMode("update")
        .start()

    query1.awaitTermination()
  }
}
