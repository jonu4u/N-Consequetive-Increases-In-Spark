

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import scala.collection.mutable.ListBuffer


object Solution {
  
  val CARRIER_ID= 7 //Index of carrier id on the data
  val DESTINATION_AIRPORT= 22 //Index of destination airport on the data
  val DELAY= 33 //Index of delay on the data
  val N=10  //Say 10 consequetive increases in delay to raise an alarm
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    
    val file=sc.textFile("Data File Location https://transtats.bts.gov/dl_selectfields.asp?table_id=236&db_short_name=on-time")
    
    val sortedDataBasedOnCaseClass= file.map { row => row.split(",") }.map { dataArr =>
      (FlightData(dataArr(CARRIER_ID),dataArr(DESTINATION_AIRPORT),dataArr(DELAY).toInt),dataArr(DELAY).toInt) }
    
    
    //Partitioning based on carrier id
    val sortedAndPartitioned=sortedDataBasedOnCaseClass.repartitionAndSortWithinPartitions(new CarrierPartitioner(1))
 
   // 
    val alarmGenerator=sortedAndPartitioned.mapPartitions(partitionIterator=>{
      var outputList=new ListBuffer[(String)]()
      var prevCarrier =""
      var prevDelayValue=0
      var count=0;
      while(partitionIterator.hasNext){
        val nextElement=partitionIterator.next()
        val nextFlightData=nextElement._1
        if(prevCarrier.equals("")){
          prevCarrier=nextFlightData.carrierId
          prevDelayValue=nextElement._2
        }else{
           if(prevCarrier.equals(nextFlightData.carrierId)){
              if(nextElement._2 > prevDelayValue){
                count=count+1
              }else{
                count=0
              }
              if(count>N ) {
               var rec=("The carrier which violates N is--->"+prevCarrier)
                outputList += rec
                count=0
              }
             
            }else{
              prevCarrier=nextFlightData.carrierId
              count=0
            }
          
          
          
        }
      }
    outputList.iterator
    })
    
  
    //Prints the carrier with N consequetive delays.
    alarmGenerator.foreach { println }
    }
  
  
  
}


//This case class along with its companion object determines the ordering of the sort of the data.
case class FlightData(carrierId:String,destinationAirport:String,delay:Int)
object FlightData{
implicit def orderbyattributes[A <: FlightData]:Ordering[A]={
  Ordering.by { compKey=>(compKey.carrierId,compKey.destinationAirport,compKey.delay) }
  
}
}


///Partitions using the carrier ID so that all data of a single carrier ends up on the same partition
class CarrierPartitioner(partitions:Int) extends Partitioner{
  
 
  
  override def numPartitions: Int = partitions
  
  override def getPartition(key: Any): Int = {
    val k=key.asInstanceOf[FlightData]
    k.carrierId.hashCode()%partitions
    
  }

  
}