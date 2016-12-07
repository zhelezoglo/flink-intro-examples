package io.github.rezolya.intro.flink.exercises
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.util.serialization.SerializationSchema
import org.apache.flink.util.Collector

/**
  * DataStream Job to play with TaxiRide data stream, provided by data artisans for training purposes
  * You can get the dataset at: http://dataartisans.github.io/flink-training/trainingData/nycTaxiRides.gz
  */
object TaxisJob {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // configure event-time processing
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.setParallelism(1)

    val maxDelay = 1
    val servingSpeed = 60

    // get the taxi ride data stream
    val rides: DataStream[TaxiRide] = env.addSource(
      new TaxiRideSource("/tmp/nycTaxiRides.gz", maxDelay, servingSpeed))

    //TODO: Exercise 2.1. Write the taxi rides to a socket stream, and see how this works
//    val serialisationSchema = new SerializationSchema[String]() {
//      override def serialize(t: String): Array[Byte] = s"$t\n".getBytes
//    }
//    rides.map(_.toString).addSink(new SocketClientSink("localhost", 9999, serialisationSchema, 0, true))

    //TODO: Exercise 2.2. Taxi Ride Cleansing. Filter out only taxi rides which start and end in the New York City
    //GeoUtils.isInNYC() can tell you whether a location is in NYC.
    val nyRides: DataStream[TaxiRide] = rides.filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
    nyRides.writeAsText("/tmp/taxis/nyRides.txt", WriteMode.OVERWRITE)

    /* TODO: Exercise 3.3. Popular places. Find popular places by counting places where the taxi rides stop or start
       use GeoUtils.mapToGridCell(float lon, float lat) to group locations
       count every five minutes the number of taxi rides that started and ended in the same area within the last 15 minutes
    */

    //1. determine the sells of start and end of the taxi rides
    // result type: (Int - cell id, TaxiRide)
    val withCell: DataStream[(Int, TaxiRide)] = nyRides.map{ r =>
      if(r.isStart)
        (GeoUtils.mapToGridCell(r.startLon, r.startLat), r)
      else
        (GeoUtils.mapToGridCell(r.endLon, r.endLat), r)
    }

    //2. find out how many times each cell is visited
    // result type: (Int - cell id, Long - timestamp of the count, Boolean - arrival or departure, Int - count)
    val cellVisits: DataStream[(Int, Long, Boolean, Int)] = withCell.keyBy(cr => (cr._1, cr._2.isStart))
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
      .apply{(key, timeWindow, windowData, collector: Collector[(Int, Long, Boolean, Int)]) =>
        val cellId = key._1
        val isStart = key._2
        val result = (cellId, timeWindow.getEnd, isStart, windowData.size)
        collector.collect(result)
      }

    //3. filter the popular cells only - the ones that have more visits that popularityThreshold
    val popularityThreshold = 20
    val popularCells: DataStream[(Int, Long, Boolean, Int)] = cellVisits.filter(_._4 >= popularityThreshold)

    //4. convert the cell id to coordinates of the cell (use GeoUtils.getGridCellCenterLat(cellId))
    // result type: (Float - cell longitude, Float - cell latitude, Long - timestamp of the count, Boolean - arrival or departure, Int - count)
    val popularPlaces: DataStream[(Float, Float, Long, Boolean, Integer)] = popularCells.map(new CellToCoordinatesMapper)
    popularPlaces.writeAsText("/tmp/taxis/popularPlaces.txt", WriteMode.OVERWRITE)

    env.execute()
  }

  class CellToCoordinatesMapper extends MapFunction[(Int, Long, Boolean, Int), (Float, Float, Long, Boolean, Integer)] {
    override def map(cellVisits: (Int, Long, Boolean, Int)): (Float, Float, Long, Boolean, Integer) =
      (GeoUtils.getGridCellCenterLon(cellVisits._1), GeoUtils.getGridCellCenterLat(cellVisits._1), cellVisits._2, cellVisits._3, cellVisits._4)
  }
}
