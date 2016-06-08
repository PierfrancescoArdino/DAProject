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

    public enum Phases {
        TWO_HOP_CREATION_FIRST,
        TWO_HOP_CREATION_SECOND,
        TWO_HOP_CREATION_THIRD,
        PRE_PROCESSING,
        CORE_PROCESSING,
        POST_PROCESSING
    };

    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
        registerPersistentAggregator(PHASE, IntOverwriteAggregator.class);
        registerAggregator(CONVERGED, IntSumAggregator.class);
        limite = new IntWritable(5);
    }

    @Override
    public void compute() {
        if (getSuperstep() == 0) {
            setPhase(Phases.TWO_HOP_CREATION_FIRST);
        } else {
            Phases currPhase = getPhase();
            System.out.println("kln" + currPhase);
            switch (currPhase) {
                case TWO_HOP_CREATION_FIRST:
                    //System.out.print("sto cambiando la fase da " + currPhase + " " + Phases.TWO_HOP_CREATION_SECOND);
                    //setPhase(Phases.TWO_HOP_CREATION_SECOND);
                    setAggregatedValue(PHASE, new IntWritable(1));
                    System.out.println(" a " + currPhase);
                    break;
                case TWO_HOP_CREATION_SECOND:
                    System.out.print("sto cambiando la fase da " + currPhase + "km");
                    //setPhase(Phases.TWO_HOP_CREATION_THIRD);
                    setAggregatedValue(PHASE, new IntWritable(2));
                    System.out.println(" a " + currPhase);
                    break;
                case TWO_HOP_CREATION_THIRD:
                  //  System.out.print("sto cambiando la fase da " + currPhase);
                    //setPhase(Phases.PRE_PROCESSING);
                    setAggregatedValue(PHASE, new IntWritable(3));
                    System.out.println(" a " + currPhase);
                    break;
                case PRE_PROCESSING:
                    setPhase(Phases.CORE_PROCESSING);
                    break;
                case CORE_PROCESSING:
                    IntSumAggregator sum = getAggregatedValue(CONVERGED);
                    if (sum.getAggregatedValue().get() > limite.get()) {
                        setPhase(Phases.POST_PROCESSING);
                    }
                    break;
                case POST_PROCESSING:
                    setPhase(Phases.POST_PROCESSING);
                    break;
                default:
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
        System.out.println("pahse ordinal " + phase.ordinal());
        setAggregatedValue(PHASE, new IntWritable(phase.ordinal()));
    }

    /**
     * Get current phase.
     * @return Current phase as enumerator.
     */
    private Phases getPhase() {
        IntWritable phaseInt = getAggregatedValue(PHASE);
        System.out.println("lkmlkmlkm" + phaseInt);
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
