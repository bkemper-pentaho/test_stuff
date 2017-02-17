package org.pentaho.pdi.engine.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bkemper on 2/17/17.
 */
public class SparkStreamTest {


  public static void main( String[] args ) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName( "StreamingTransformTest" ).setMaster( "local" );
    JavaStreamingContext ssc = new JavaStreamingContext( sparkConf, Durations.seconds( 10 ) );

    KettleEnvironment.init();

    String filename = "/Users/bkemper/myspark_app/test_stuff/pdi-spark-streaming-engine/src/main/resources/JustDataGrid.ktr";
    TransMeta transMeta = new TransMeta( filename );
    Trans trans = new Trans( transMeta );
    trans.prepareExecution( null );
    trans.setRunning( true );   // fake out the executor right now...

    List<StepMetaDataCombi> transSteps = trans.getSteps();

    if ( transSteps != null && transSteps.size() != 1 ) {
      throw new Exception( "Oops- not what I was expecting right now!" );
    }


    // create an input step wrapping a PDI Step..
    PDIStepReceiver receiver1 = new PDIStepReceiver( StorageLevel.MEMORY_AND_DISK_2(), transSteps.get( 0 ) );
    JavaReceiverInputDStream<Object[]> lines = ssc.receiverStream( receiver1 );

    PDIStepRecieverListener listener = new PDIStepRecieverListener();
    ssc.addStreamingListener( listener );


    JavaDStream<String> words = lines.flatMap( new FlatMapFunction<Object[], String>() {
      @Override
      public Iterator<String> call( Object[] x ) {
        List<String> objArray = new ArrayList<>();
        for ( int i = 0; i < x.length; i++ ) {
          objArray.add( x[ i ] != null ? x[ i ].toString() : "<null>" );
        }
        return objArray.iterator();
      }
    } );

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override public Tuple2<String, Integer> call( String s ) {
          return new Tuple2<>( s, 1 );
        }
      } ).reduceByKey( new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call( Integer i1, Integer i2 ) {
        return i1 + i2;
      }
    } );

    lines.print( 1000 );
    words.print( 1000 );
    wordCounts.print( 1000 );
    ssc.start();
    Thread thread = new Thread( () -> {
      while ( !listener.isReceiverStopped() ) {
        try {
          Thread.sleep( 500 ); /*check every 1/2 seconds*/
        } catch ( InterruptedException e ) {
          e.printStackTrace();
        }
      }
    } );
    thread.start();
    thread.join();
    ssc.stop( true, true );

    ssc.awaitTermination();
  }

}
