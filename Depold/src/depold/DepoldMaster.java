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

    public static final String PHASE = "scccompute.phase";

    public static final String DELETED_NODES = "scccompute.max";

    public static final String WCC = "depold.wcc";
    public static final String GROUP_DEGREE = "depold.group_degree";

    public enum Phases {
        PRE_PROCESSING_TWO_HOP_FIRST_PHASE, PRE_PROCESSING_TWO_HOP_SECOND_PHASE,
        PRE_PROCESSING_TWO_HOP_THIRD_PHASE,
        PRE_PROCESSING_SECOND_PHASE,
        CORE_PROCESSING_SIMILARITY_PHASE, CORE_PROCESSING_TOPOLOGY_FIRST_PHASE, CORE_PROCESSING_TOPOLOGY_SECOND_PHASE,
        POST_PROCESSING_WCC_FIRST, POST_PROCESSING_WCC_SECOND, POST_PROCESSING_DEGREE_CALCULATOR_FIRST, POST_PROCESSING_DEGREE_CALCULATOR_SECOND,POST_PROCESSING_DEGREE_CALCULATOR_THIRD,
        POST_PROCESSING_GROUP_DETECTOR, POST_PROCESSING_COMPUTE_COMMUNITIES
    };

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(PHASE, IntSumAggregator.class);
        registerPersistentAggregator(DELETED_NODES, IntSumAggregator.class);
        registerAggregator(WCC, IntSumAggregator.class);
        registerAggregator(GROUP_DEGREE,IntSumAggregator.class);
    }

    @Override
    public void compute() {
        if (getSuperstep() == 0) {
            setPhase(Phases.PRE_PROCESSING_TWO_HOP_FIRST_PHASE);
            setAggregatedValue(DELETED_NODES,new IntWritable(0));
            setAggregatedValue(WCC, new IntWritable(1));
            setAggregatedValue(GROUP_DEGREE, new IntWritable(1));
        } else {
            Phases currPhase = getPhase();
            int converge_value = ((int) converge.get(getConf()));
            System.out.println("la fase in cui mi trovo e' " + currPhase);
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
                    int m = tmp.get()/2;
                    if(converge_value>m)
                    {
                        setAggregatedValue(DELETED_NODES, new IntWritable(0));
                        setPhase((Phases.POST_PROCESSING_WCC_FIRST));
                    }
                    else{
                        setPhase(Phases.CORE_PROCESSING_SIMILARITY_PHASE);
                        setAggregatedValue(DELETED_NODES, new IntWritable(0));
                    }
                    break;
                case POST_PROCESSING_WCC_FIRST:
                    setPhase(Phases.POST_PROCESSING_WCC_SECOND);
                    break;
                case POST_PROCESSING_WCC_SECOND:
                    IntWritable wcc = getAggregatedValue(WCC);
                    if(wcc.get() == 0){
                        setPhase(Phases.POST_PROCESSING_DEGREE_CALCULATOR_FIRST);
                    } else {
                        setPhase((Phases.POST_PROCESSING_WCC_SECOND));
                        setAggregatedValue(WCC, new IntWritable(0));
                    }
                    break;
                case POST_PROCESSING_DEGREE_CALCULATOR_FIRST:
                    setPhase(Phases.POST_PROCESSING_DEGREE_CALCULATOR_SECOND);
                    break;
                case POST_PROCESSING_DEGREE_CALCULATOR_SECOND:
                    IntWritable degree = getAggregatedValue(GROUP_DEGREE);
                    System.out.println("Degree e' " + degree);
                    if(degree.get() == 0){
                        setPhase(Phases.POST_PROCESSING_DEGREE_CALCULATOR_THIRD);
                    } else {
                        setPhase(Phases.POST_PROCESSING_DEGREE_CALCULATOR_SECOND);
                    }
                    break;
                case POST_PROCESSING_DEGREE_CALCULATOR_THIRD:
                    setPhase(Phases.POST_PROCESSING_GROUP_DETECTOR);
                    break;
                case POST_PROCESSING_GROUP_DETECTOR:
                    setPhase(Phases.POST_PROCESSING_COMPUTE_COMMUNITIES);
                    break;
                case POST_PROCESSING_COMPUTE_COMMUNITIES:
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


