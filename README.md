# CsvToDataFrame
This repository contains a code that convert csv data file to a spark sql DataFrame
## requirement
this code require spark 1.2 +, scala 2.10
## how it works
The main function create de spark context and the sql spark context and make use of loadCsvWithHeader function to onvert csv file to dataFrame
### create the spark context
```scala 
 val conf = new SparkConf().setAppName("FromCsvToDataFrame").setMaster("local[2]")
 val sc = new SparkContext(conf)
 val sqlc = new SQLContext(sc)
``` 
### load function
loadCsvWithHeader is the function that make the conversion it takes as args:
- sc: the spark context
- sqlc: the sql context
- "test.csv" the csv file path
```scala 
val df = loadCsvWithHeader(sc, sqlc, "test_Data1.csv",',')
df.limit(30).show()// show the data table
```
the load function steps in the rdd collection
1- load the data
```scala
val rdd = sc.textFile(path)
```
2- split each line of the rdd collection into words 
```scala
val splitedRdd = rdd.map(line => line.replace("\"", "").stripLineEnd)
      .map(line => line.split(sep))// parse the line words
```
3- extract the header and create the rdd of rows obtained from the remaining rdds
```scala
val header = splitedRdd.first()
val filtredRdd = splitedRdd.filter { line => line(0) != header(0)}// field names 
val rowRdd = filtredRdd.map(rdd => Row.fromSeq(rdd))// transform rdd of array to rdd of Row
```
4- finally we can create the datfaram using the createDatafrmae function wich takes as args:
  - the rdd of Rows
  - schema structype obtained from the  header

```scala
val schema = StructType(header.map( h => new StructField(h, StringType, true)))// create the schema
val df = sqlc.createDataFrame(rowRdd, schema)
```


