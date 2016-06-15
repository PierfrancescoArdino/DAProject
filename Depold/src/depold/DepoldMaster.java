package depold;

import org.apache.giraph.aggregators.BooleanOverwriteAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import javax.sound.midi.SysexMessage;

/**
 * Created by mariapia on 04/06/16.
 */
public class DepoldMaster extends DefaultMasterCompute {

    public static final LongConfOption converge =
            new LongConfOption("Depold.converge", 1,
                    "converge threshold");

    /**
     * Aggregator that stores the current phase
     */
    public static final String PHASE = "scccompute.phase";

    /**
     * Flags whether a new maximum was found in the Forward Traversal phase
     */
    public static final String DELETED_NODES = "scccompute.max";

    /**
     * Flags whether a vertex converged in the Backward Traversal phase
     */
    public static final String CONVERGED = "scccompute.converged";

    /**
     * Enumerates the possible phases of the algorithm.
     */
    public enum Phases {
        PRE_PROCESSING_TWO_HOP_FIRST_PHASE, PRE_PROCESSING_TWO_HOP_SECOND_PHASE,
        PRE_PROCESSING_TWO_HOP_THIRD_PHASE,
        PRE_PROCESSING_SECOND_PHASE,
        CORE_PROCESSING_SIMILARITY_PHASE, CORE_PROCESSING_TOPOLOGY_FIRST_PHASE, CORE_PROCESSING_TOPOLOGY_SECOND_PHASE,
        POST_PROCESSING
    };

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(PHASE, IntSumAggregator.class);
        registerPersistentAggregator(DELETED_NODES, IntSumAggregator.class);
    }

    @Override
    public void compute() {
        if (getSuperstep() == 0) {
            setPhase(Phases.PRE_PROCESSING_TWO_HOP_FIRST_PHASE);
            setAggregatedValue(DELETED_NODES,new IntWritable(0));
        } else {
            Phases currPhase = getPhase();
            int converge_value = ((int) converge.get(getConf()));
            //System.out.println("phase " + currPhase + " max ad cazzum " + m.get());
            switch (currPhase) {
                case PRE_PROCESSING_TWO_HOP_FIRST_PHASE:
                    setPhase(Phases.PRE_PROCESSING_TWO_HOP_SECOND_PHASE);
                    break;
                case PRE_PROCESSING_TWO_HOP_SECOND_PHASE:
                    setPhase(Phases.PRE_PROCESSING_TWO_HOP_THIRD_PHASE);
                    break;
                case PRE_PROCESSING_TWO_HOP_THIRD_PHASE:
                    setPhase(Phases.PRE_PROCESSING_SECOND_PHASE);
                    break;
                case PRE_PROCESSING_SECOND_PHASE:
                    setPhase(Phases.CORE_PROCESSING_SIMILARITY_PHASE);
                    break;
                case CORE_PROCESSING_SIMILARITY_PHASE:
                    setPhase(Phases.CORE_PROCESSING_TOPOLOGY_FIRST_PHASE);
                    break;
                case CORE_PROCESSING_TOPOLOGY_FIRST_PHASE:
                    setPhase((Phases.CORE_PROCESSING_TOPOLOGY_SECOND_PHASE));
                    break;
                case CORE_PROCESSING_TOPOLOGY_SECOND_PHASE:
                    IntWritable tmp = getAggregatedValue(DELETED_NODES);
                    System.out.println("i nodi eliminati sono " + tmp);
                    int m = tmp.get()/2;
                    if(converge_value>m)
                    {
                        setAggregatedValue(DELETED_NODES, new IntWritable(0));
                        setPhase((Phases.POST_PROCESSING));
                    }
                    else{
                        System.out.println("conv value " + converge_value + " del values " + m);
                        setPhase(Phases.CORE_PROCESSING_SIMILARITY_PHASE);
                        setAggregatedValue(DELETED_NODES, new IntWritable(0));
                    }
                    break;
                case POST_PROCESSING:
                    break;
                default :
                    break;
            }
        }
    }

    /**
     * Sets the next phase of the algorithm.
     * @param phase
     *          Next phase.
     */
    private void setPhase(Phases phase) {
        setAggregatedValue(PHASE, new IntWritable(phase.ordinal()));
    }

    /**
     * Get current phase.
     * @return Current phase as enumerator.
     */
    private Phases getPhase() {
        IntWritable phaseInt = getAggregatedValue(PHASE);
        return getPhase(phaseInt);
    }

    /**
     * Helper function to convert from internal aggregated value to a Phases
     * enumerator.
     * @param phaseInt
     *          An integer that matches a position in the Phases enumerator.
     * @return A Phases' item for the given position.
     */
    public static Phases getPhase(IntWritable phaseInt) {
        return Phases.values()[phaseInt.get()];
    }

}


