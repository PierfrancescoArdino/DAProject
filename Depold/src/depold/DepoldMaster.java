package depold;

import org.apache.giraph.aggregators.BooleanOverwriteAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import javax.sound.midi.SysexMessage;

/**
 * Created by mariapia on 04/06/16.
 */
public class DepoldMaster extends DefaultMasterCompute {

    public static String PHASE = "phase";
    public static final String CONVERGED = "DEPOLD_COMPUTE.converged";

    public IntWritable limite;


    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(PHASE, IntOverwriteAggregator.class);
        registerAggregator(CONVERGED, IntSumAggregator.class);
        limite = new IntWritable(5);
    }

    @Override
    public void compute() {
        if(getSuperstep() == 0){
            setAggregatedValue(PHASE, new IntWritable(1));
        } else {
            System.out.println("FASE : " + getAggregatedValue(PHASE));
            int i = ((IntWritable)getAggregatedValue(PHASE)).get();
            setAggregatedValue(PHASE,new IntWritable(i));
        }
    }
}


