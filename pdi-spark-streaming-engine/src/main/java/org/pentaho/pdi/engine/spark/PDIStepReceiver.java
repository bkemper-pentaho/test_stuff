package org.pentaho.pdi.engine.spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import java.util.HashMap;
import java.util.UUID;

/**
 * Created by bkemper on 2/17/17.
 */
public class PDIStepReceiver extends Receiver<Object[]> implements RowListener {

  @Override public void rowReadEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {

  }

  @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    Object[] copiedRow = new Object[ rowMeta.size() ];
    // PDI over allocates.  Trim this to the right size for the RDD....

    for ( int i = 0; i < copiedRow.length; i++ ) {
      copiedRow[ i ] = row[ i ];
    }
    store( copiedRow );
  }

  @Override public void errorRowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {

  }

  private static class StepMetaHolder {
    HashMap<String, StepMetaDataCombi> combis = new HashMap<>();

    public void addCombi( String combiId, StepMetaDataCombi combi ) {
      combis.put( combiId, combi );
    }

    public StepMetaDataCombi getCombi( String combiId ) {
      return combis.get( combiId );
    }

    public void remove( String combiId ) {
      combis.remove( combiId );
    }
  }

  private static final StepMetaHolder holder = new StepMetaHolder();

  private String combiId;

  public PDIStepReceiver( StorageLevel storageLevel, StepMetaDataCombi stepMetaDataCombi ) {
    super( storageLevel );
    combiId = UUID.randomUUID().toString();
    holder.addCombi( combiId, stepMetaDataCombi );
  }

  @Override public void onStart() {
    // Start the thread that receives data over a connection
    new Thread() {
      @Override public void run() {
        try {
          processStep();
        } catch ( Exception ex ) {
          // should do SOMETHING here..
          System.out.println( "Yikes - we're dead!" );
        }
      }
    }.start();
  }

  @Override public void onStop() {
    holder.remove( combiId );
    this.combiId = null;
  }

  private void processStep() throws Exception {

    StepMetaDataCombi combi = holder.getCombi( combiId );

    if ( combi.step instanceof BaseStep ) {
      ( (BaseStep) combi.step ).addRowListener( this );
    } else {
      throw new Exception( "Error - not a base step!" );
    }
    // fire off the rows until done - this is a HUGE hack for now....
    boolean cont = true;
    while ( cont ) {
      cont = combi.step.processRow( combi.meta, combi.data );
    }
    stop( "No more rows!" );
  }
}
