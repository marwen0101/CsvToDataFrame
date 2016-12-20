package csvspark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.Row
object FromCsvToDataFrame {
  
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("FromCsvToDataFrame").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    val df = loadCsvWithHeader(sc, sqlc, "file:///Users/hasnaoui/Desktop/soft/test_Data/test_Data1.csv",',')
    df.limit(30).show()
  }

  def loadCsvWithHeader(sc: SparkContext, sqlc: SQLContext, path: String, sep: Char): DataFrame = {

    val rdd = sc.textFile(path)
    val splitedRdd = rdd.map(line => line.replace("\"", "").stripLineEnd)
      .map(line => line.split(sep))// parse the line words
      
   val header = splitedRdd.first()
   val filtredRdd = splitedRdd.filter { line => line(0) != header(0)}// field names 
   val rowRdd = filtredRdd.map(rdd => Row.fromSeq(rdd))// transform rdd of array to rdd of Row
   val schema = StructType(header.map( h => new StructField(h, StringType, true)))// create the schema
   val df = sqlc.createDataFrame(rowRdd, schema)
   df
  }

}