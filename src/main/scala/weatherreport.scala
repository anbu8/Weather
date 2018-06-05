import org.apache.spark.sql.Row;

import org.apache.spark.sql.types.{StructType,StructField,StringType};

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

import org.apache.spark.sql.Row;

import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.{SparkConf, SparkContext}

import java.time._ 
import java.time.format._
import java.util.Calendar._
import org.joda.time._

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.{Date, TimeZone}
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import math._
import scala.math.{Pi, cos}

/**
 *  Output weather information
 *
 *  @author anbue
 *  @version 0.1
 */

	object WeatherStats {
		 /**
		   * get weather information string
		   *
		   * @param city name
		   * @return WeatherInfo String
		   * 
		   */
		def main(args: Array[String]) 
		{


		try {
		val conf= new SparkConf().setAppName("WeatherStations").setMaster("yarn-client")
		val sc=new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

				val schema =
				StructType(Seq(
				StructField("ID",StringType, true), StructField("LATITUDE", StringType, true),StructField("LONGITUDE", StringType, true),StructField("ELEVATION", StringType, true),StructField("NAME", StringType, true)))

				val iataschema  =
				StructType(Seq(
				StructField("CITY",StringType, true), StructField("IATA", StringType, true),StructField("ZONEID", StringType, true),StructField("ID", StringType, true)))


				val dataschema  =
				StructType(Seq(
				StructField("STATION_ID",StringType, true), StructField("DATE", StringType, true),StructField("OBSERVATION_TYPE", StringType, true),StructField("OBSERVATION_VALUE", StringType, true)))
				val Aus = sc.textFile(args(0)) 


				val header = Aus.first() 
				val Au = Aus.filter(row => row != header) 
				val rowRDD = Au.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3), p(4)))


				val AusDataFrame = sqlContext.createDataFrame(rowRDD, schema)

				AusDataFrame.registerTempTable("Austab")

				val Ausdf = sqlContext.sql("SELECT * FROM Austab")

				
				val IATA = sc.textFile(args(1)) 

				val header2 = IATA.first() 
				val IA = IATA.filter(row => row != header2) 

				val IArowRDD = IA.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3)))


				val IATADataFrame = sqlContext.createDataFrame(IArowRDD, iataschema)

				IATADataFrame.registerTempTable("iatatab")

				println("\nPlease enter city name : ")
				val city = readLine()

				val iatadf = sqlContext.sql("SELECT  * FROM iatatab where CITY = '"+city.toUpperCase+"'")

				//iatadf.show()

				val iataZone = iatadf.select("ZONEID").first().getAs[String](0)

				val iataID = iatadf.select("ID").first().getAs[String](0)

				val IATA1 =  iatadf.select("IATA").first().getAs[String](0)


				val data = sc.textFile(args(2))
				val header3 = data.first() 
				val da = data.filter(row => row != header3) 

				val datarowRDD = da.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3)))


				val dDataFrame = sqlContext.createDataFrame(datarowRDD, dataschema)

				dDataFrame.registerTempTable("datatab")

				val datadf = sqlContext.sql("SELECT  OBSERVATION_TYPE, OBSERVATION_VALUE,STATION_ID,DATE FROM datatab where STATION_ID = '" +iataID+ "'")

				val Ausdf2 = sqlContext.sql("SELECT LATITUDE,LONGITUDE,ELEVATION,ID FROM Austab where ID = '" +iataID+ "'")

				datadf.registerTempTable("datavalues")
				Ausdf2.registerTempTable("Ausvalues")

				/**
				* *
				* 
				* @return previous year date
				* 
				*/
				val dafo = new SimpleDateFormat("yyyyMMdd'T'HH:mm'Z'")
				val tz = TimeZone.getTimeZone("UTC") 
				dafo.setTimeZone(tz) 
				val calendar = Calendar.getInstance() 
				calendar.add(Calendar.YEAR, -1) 
				val prevyr = dafo.format(calendar.getTime)

				val y = prevyr.substring(0,8)



				val combine = sqlContext.sql("SELECT  a.OBSERVATION_TYPE, a.OBSERVATION_VALUE,a.STATION_ID, b.LATITUDE,b.LONGITUDE,b.ELEVATION,a.DATE FROM datavalues a, Ausvalues b where a.DATE = '"+y+"' and a.STATION_ID = b.ID and b.ID = '" +iataID+ "'")

				//combine.show()

				val df  = combine.select("OBSERVATION_TYPE", "OBSERVATION_VALUE")   

				var a = df.count 

				df.registerTempTable("df1")

				val arr = 	df.select("OBSERVATION_TYPE","OBSERVATION_VALUE").map(r => r(0).asInstanceOf[String]).collect()
				
				var finaldest:String = iatadf.select("IATA").first().getAs[String](0) + "|" +Ausdf2.select("LATITUDE").first().getAs[String](0)+ "," + Ausdf2.select("LONGITUDE").first().getAs[String](0) + "," +Ausdf2.select("ELEVATION").first().getAs[String](0) + "|"


			  
				
				 /**
				* get Temperature for the station in Celsius
				*      
				* @param  Nil
				* @return Temperature
				* 
				*/
				
				def Temperature(): Double ={

						val Tmx = sqlContext.sql("select cast (OBSERVATION_VALUE as Double) from df1 where OBSERVATION_TYPE = '" +arr.apply(0).toString+ "'")


						val TMAX = Tmx.select("OBSERVATION_VALUE").first().getAs[Double](0)

						val Tmn = sqlContext.sql("select cast (OBSERVATION_VALUE as Double) from df1 where OBSERVATION_TYPE = '" +arr.apply(1).toString+ "'")
						val TMIN = Tmn.select("OBSERVATION_VALUE").first().getAs[Double](0)

						
						
						val now = Calendar.getInstance()
						val currentHour = now.get(Calendar.HOUR_OF_DAY)
						val hr: Double = if (currentHour > 4) currentHour - 5 else currentHour + 19
						def Temp(TMAX: Double, TMIN: Double): Double = {
						var sum : Double =0
						sum = ((TMAX+TMIN)/2) - ((TMAX-TMIN)/2)*cos(hr * Pi/9)
						return sum
						}

						def Temp2(TMAX: Double, TMIN: Double): Double = {
						var sum : Double =0
						sum = ((TMAX+TMIN)/2) + ((TMAX-TMIN)/2)*cos((hr-10) * Pi/13)
						return sum
						}
						val Temperature:Double = if (hr <10) Temp(TMAX,TMIN) else Temp2(TMAX,TMIN)

						val temp1 = Temperature/2
						return "%.2f".format(temp1).toDouble

				}
                
				 /**
				* get Rainy/Sunny for the station
				*      
				* @param precipitation 
				* @return Determine Hour of Day
				* 
				*/
				def prcp(precipitation:String): String ={
				var p:String = ""
				if (precipitation > "0" ){	p = "Rainy" }
				else {
				 p  = "Sunny"}
				return p
				}
				val PRP = sqlContext.sql("select cast(OBSERVATION_VALUE as String) from df1 where OBSERVATION_TYPE = '"+arr.apply(2).toString+"'")
				PRP.registerTempTable("precpt")
				val tgt = PRP.select("OBSERVATION_VALUE").first().getAs[String](0)
				val precp= prcp(tgt)
				var conct = finaldest+ prevyr+ "|" + precp
				val tmp =  Temperature()
				val report = conct +"|"+ tmp
				println("Weather report : " + report   + ".")
		}
		catch {
			case ex: Exception                        => ex.printStackTrace()
			  }
			

		}
	}
