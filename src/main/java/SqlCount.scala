/**
  * Created by kalit_000 on 12/04/2016.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object SqlCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val file=sc.textFile("C:\\Users\\kalit_000\\Desktop\\2016\\scalasqlconvertcode\\sales_data.txt")

    val noheaders=file.zipWithIndex().filter(x => x._2 > 0)

    //SQL Query1:-
    /*select * from [AdventureWorks2014].[dbo].[SalesPerson]*/

    val fulldataset=
      noheaders
      .map(x => x._1.split(","))
      .map(x => (x(0),x(1).replaceAll("NULL","0"),x(2).replace("NULL","0"),x(3),x(4),x(5),x(6),x(7),x(8)))
      .map{case (businessentityid,territoryid,salesquota,bonus,commissionpct,salesytd,saleslastyear,rowguid,modifieddate) => (businessentityid,territoryid,salesquota,bonus,commissionpct,salesytd,saleslastyear,rowguid,modifieddate)}

   //fulldataset.foreach(println)

    //SQL Query2:-
    /*select TerritoryID,sum(salesquota) as salesquota from [AdventureWorks2014].[dbo].[SalesPerson] group by TerritoryID*/

   val quotegroupby=
     noheaders
     .map(x => x._1.split(",")).map(x => (x(1).replaceAll("NULL","0"),x(2).replace("NULL","0")))
     .map{case(territoryid,salesquota) => ((territoryid,salesquota.toDouble))}
     .reduceByKey((x,y) => (x+y))

    //quotegroupby.foreach(println)

    //SQL Query3:-
    /*select TerritoryID,sum(salesquota) as salesquota ,max(salesquota) from [AdventureWorks2014].[dbo].[SalesPerson] group by TerritoryID*/

    val sumandgmax=
      noheaders
      .map(x => x._1.split(",")).map(x => (x(1).replaceAll("NULL","0"),x(2).replace("NULL","0"),x(2).replace("NULL","0")))
      .map{case (territoryid,salesquota,maxsales) => ((territoryid),(salesquota.toDouble,maxsales.toDouble))}
      .reduceByKey((x,y) => (x._1 + y._1,math.max(x._2,y._2)))
   // sumandgmax.foreach(println)

    //SQL Query3:-
    /*select TerritoryID,sum(salesquota) as salesquota ,max(salesquota),count(1) from [AdventureWorks2014].[dbo].[SalesPerson] group by TerritoryID*/

    val sumandgmaxcount=
      noheaders
      .map(x => x._1.split(",")).map(x => (x(1).replaceAll("NULL","0").toInt,x(2).replace("NULL","0"),x(2).replace("NULL","0"),x(1).replaceAll("NULL","0"),x(2).replaceAll("NULL","0")))
      .map{case (territoryid,salesquota,maxsales,countofterritoryid,minsales) => ((territoryid.toInt),(salesquota.toDouble,maxsales.toDouble,1,minsales.toDouble))}
      .reduceByKey((x,y) => (x._1 + y._1,math.max(x._2,y._2),x._3+y._3,math.min(x._4,y._4)))

    //sumandgmaxcount.foreach(println)

    //sql query 4:-
    /*select * from flights inner join airports on flights.a=airports.b inner join airlines on flights.a=airlines.b/
    */

    // Fact table
    val flights = sc.parallelize(List(
      ("SEA", "JFK", "DL", "418",  "7:00"),
      ("SFO", "LAX", "AA", "1250", "7:05"),
      ("SFO", "JFK", "VX", "12",   "7:05"),
      ("JFK", "LAX", "DL", "424",  "7:10"),
      ("LAX", "SEA", "DL", "5737", "7:10")))

    // Dimension table
    val airports = sc.parallelize(List(
      ("JFK", "John F. Kennedy International Airport", "New York", "NY"),
      ("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
      ("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
      ("SFO", "San Francisco International Airport", "San Francisco", "CA")))

    // Dimension table
    val airlines = sc.parallelize(List(
      ("AA", "American Airlines"),
      ("DL", "Delta Airlines"),
      ("VX", "Virgin America")))

    val airportsmap=sc.broadcast(airports.map{ case(a,b,c,d) => (a,b)}.collectAsMap())
    val airlinesmap=sc.broadcast(airlines.collectAsMap())

    val test=flights.map{ case (a,b,c,d,e) => (airportsmap.value.get(a).get, airportsmap.value.get(b).get,airlinesmap.value.get(c).get,d,e) }

    //test.foreach(println)

    //SQL Query3:-
    /*select TerritoryID,sum(salesquota) as salesquota ,max(salesquota),count(1) from [AdventureWorks2014].[dbo].[SalesPerson] group by TerritoryID having TerritoryID=2*/

    val sumandgmaxcountwhere=
            noheaders
            .map(x => x._1.split(",")).filter(x => x(1) == "2")
            .map(x => (x(1).replaceAll("NULL","0").toInt,x(2).replace("NULL","0"),x(2).replace("NULL","0"),x(1).replaceAll("NULL","0"),x(2).replaceAll("NULL","0")))
            .map{case (territoryid,salesquota,maxsales,countofterritoryid,minsales) => ((territoryid.toInt),(salesquota.toDouble,maxsales.toDouble,1,minsales.toDouble))}
            .reduceByKey((x,y) => (x._1 + y._1,math.max(x._2,y._2),x._3+y._3,math.min(x._4,y._4)))

    //sumandgmaxcountwhere.foreach(println)

    //SQL Query 4:-
    /*select * from [AdventureWorks2014].[dbo].[SalesPerson]  where substring(convert(char(10),ModifiedDate,126),06,02)='05';*/

    val substring=
      noheaders
        .map(x => x._1.split(",")).filter(x => x(8).substring(5,7) =="05")
        .map(x => (x(1).replaceAll("NULL","0").toInt,x(2).replace("NULL","0"),x(2).replace("NULL","0"),x(1).replaceAll("NULL","0"),x(2).replaceAll("NULL","0"),x(8)))
        .map{case (territoryid,salesquota,maxsales,countofterritoryid,minsales,modifiedDate) => ((territoryid.toInt,modifiedDate),(salesquota.toDouble,maxsales.toDouble,1,minsales.toDouble))}
        .reduceByKey((x,y) => (x._1 + y._1,math.max(x._2,y._2),x._3+y._3,math.min(x._4,y._4)))

    substring.foreach(println)

    //SQL Query 4:-
    /*select case when TerritoryID = 2 then 'kali'     when TerritoryID = 3 then 'sri' else 'charan' end , * from [AdventureWorks2014].[dbo].[SalesPerson] ;*/

    val casestatements=
      noheaders
        .map(x => x._1.split(",")).filter(x => x(8).substring(5,7) =="05")
        .map(x => (x(1).replaceAll("NULL","0") match { case "2" => ("kali") case "3" => ("sri") case _ => ("charan")},x(2).replace("NULL","0"),x(2).replace("NULL","0"),x(1).replaceAll("NULL","0"),x(2).replaceAll("NULL","0"),x(8)))
        .map{case (territoryid,salesquota,maxsales,countofterritoryid,minsales,modifiedDate) => ((territoryid,modifiedDate),(salesquota.toDouble,maxsales.toDouble,1,minsales.toDouble))}
        .reduceByKey((x,y) => (x._1 + y._1,math.max(x._2,y._2),x._3+y._3,math.min(x._4,y._4)))

    casestatements.foreach(println)


    sc.stop()

  }
}
