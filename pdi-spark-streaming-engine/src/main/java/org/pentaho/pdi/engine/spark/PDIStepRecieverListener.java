package org.pentaho.pdi.engine.spark;

import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;

/**
 * Created by bkemper on 2/17/17.
 */
public class PDIStepRecieverListener implements StreamingListener {
  private boolean receiverStopped = false;

  public boolean isReceiverStopped() {
    return receiverStopped;
  }

  @Override public void onReceiverStarted( StreamingListenerReceiverStarted streamingListenerReceiverStarted ) {

  }

  @Override public void onReceiverError( StreamingListenerReceiverError streamingListenerReceiverError ) {

  }

  @Override
  public void onReceiverStopped( StreamingListenerReceiverStopped receiverStopped ) {
    this.receiverStopped = true;
  }

  @Override public void onBatchSubmitted( StreamingListenerBatchSubmitted streamingListenerBatchSubmitted ) {

  }

  @Override public void onBatchStarted( StreamingListenerBatchStarted streamingListenerBatchStarted ) {

  }

  @Override public void onBatchCompleted( StreamingListenerBatchCompleted streamingListenerBatchCompleted ) {

  }

  @Override public void onOutputOperationStarted(
    StreamingListenerOutputOperationStarted streamingListenerOutputOperationStarted ) {

  }

  @Override public void onOutputOperationCompleted(
    StreamingListenerOutputOperationCompleted streamingListenerOutputOperationCompleted ) {

  }
}


