/**
  * Created by kalit_000 on 17/04/2016.
  */

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

case class Employee(employeenumber:BigInt,firstname:String,lastname:String,emailid:String)

object DataStructuresAlgo_1 {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val employee1=new Employee(100000009, "John","Doe", "john.doe@dsnalgos.com")
    val employee2=new Employee(100000002, "Patrick","Dwight", "patrick.dwight@dsnalgos.com")
    val employee3=new Employee(100000011, "Marlo","Thomas", "marlo.thomas@dsnalgos.com")

    var arraytest=new util.ArrayList[Employee]()

    arraytest.add(employee1)

    arraytest.add(employee2)
    arraytest.add(employee3)

    println(arraytest.get(0))
    println(arraytest.get(1))

  }


}
