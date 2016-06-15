package depold;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.giraph.conf.ConfOptionType;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.IOException;
import java.security.BasicPermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static depold.DepoldMaster.*;

/**
 * Created by mariapia on 04/06/16.
 */
public class Depold extends BasicComputation <LongWritable, THALS, FloatWritable, MessagesWritable>{

   // private DepoldMaster.Phases currPhase;
    //public MessagesWritable messageValue = new MessagesWritable();


   /* @Override
    public void preSuperstep() {
        IntWritable phaseInt = getAggregatedValue(DepoldMaster.PHASE);
        currPhase = DepoldMaster.getPhase(phaseInt);

    }*/
   private DepoldMaster.Phases currPhase;
   public static final LongConfOption threshold =
           new LongConfOption("Depold.threshold", 1,
                   "threshold");
    public static final LongConfOption similarity_limit = new LongConfOption("Depold.similarity_limit" , 1, "similarity_limit");
    //public int threshold;
    @Override
    public void preSuperstep() {
        IntWritable phaseInt = getAggregatedValue(PHASE);
        currPhase = DepoldMaster.getPhase(phaseInt);

    }
    @Override
    public void compute(Vertex<LongWritable, THALS, FloatWritable> vertex, Iterable<MessagesWritable> messages) throws IOException {
        THALS vertexValue = vertex.getValue();
        MessagesWritable messageValue = new MessagesWritable();
        if(vertexValue.getActive().equals("true")){

            switch (currPhase){
                /**
                 * MANDO LA LISTA DEI MIEI VICINI AI MIEI VICINI
                 */
                case PRE_PROCESSING_TWO_HOP_FIRST_PHASE:{
                    for(Edge e : vertex.getEdges()){
                      messageValue.addVicini(Long.valueOf(e.getTargetVertexId().toString())); //mi salvo tutti i miei vicini in una lista
                    }


                    messageValue.setID(Long.valueOf(vertex.getId().toString()));

                    vertex.setValue(vertexValue);
                    if (vertex.getNumEdges() != 0) {
                        for (Edge e : vertex.getEdges()) {
                            sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                        }
                    }
                    break;
                }
                /**
                 * RICEVO I VICINI DEI MIEI VICINI, INVIO I MIEI VICINI A CHI MI HA MANDATO IL MESSAGGIO
                 */
                case PRE_PROCESSING_TWO_HOP_SECOND_PHASE: {
                    for (MessagesWritable m : messages){
                        Boolean trovato = false;
                        for (Edge e : vertex.getEdges()){
                            if(e.getTargetVertexId().toString().equals(m.getID().toString())){
                                trovato = true;
                            }
                        }
                        if (trovato == false){
                            vertex.addEdge(EdgeFactory.create(new LongWritable(m.getID()), new FloatWritable(0)));
                        }
                    }

                    /*for (Edge e : vertex.getEdges()){
                        System.out.println("Archii di "+ vertex.getId()+ " : " + e.getTargetVertexId());
                    }*/
                    for (Edge e : vertex.getEdges()){
                        messageValue.addVicini(Long.valueOf(e.getTargetVertexId().toString()));
                    }
                    messageValue.setID(Long.valueOf(vertex.getId().toString()));
                    /*for (MessagesWritable m : messages) {
                        System.out.println("Messaggi inviati da" + vertex.getId() + " a " + m.getID() + " vicini " + messageValue.getVicini());
                        sendMessage(new LongWritable(m.getID()), messageValue);
                    }*/
                    sendMessageToAllEdges(vertex,messageValue);
                    //System.out.println("Sono : " +vertex.getId() + " sto finendo la fase : " + vertexValue.getFase());
                    vertex.setValue(vertexValue);




                    break;
                }
                    /**
                     * COSTRUISCO LA TWO_HOP
                     */
                case PRE_PROCESSING_TWO_HOP_THIRD_PHASE:{


                    for (MessagesWritable m : messages){
                        for (Long x : m.getVicini()) {
                            vertexValue.addTwo_hop(new Nodo(m.getID().toString(), "true"), new Nodo(x.toString(), "true"));

                        }
                    }

                    /*if (!(x.toString().equals(vertex.getId().toString()))) {
                    }*/
                    int test = ((int) threshold.get(getConf()));

                    if(vertex.getNumEdges() > test){

                        vertexValue.setActive("false");
                        messageValue.setActive("false");

                        messageValue.setID(vertex.getId().get());

                    }else{

                        messageValue.setActive("true");
                        messageValue.setID(vertex.getId().get());
                    }

                    vertex.setValue(vertexValue);
                    for (Edge e : vertex.getEdges()) {
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                    }
                    break;
                }
                /**
                 * FILTRAGGIO DEI NODI
                 */
                case PRE_PROCESSING_SECOND_PHASE: {
                    //System.out.println("Nella terza fase: " + vertex.getId());
                    for(MessagesWritable m : messages){
                        if(m.getActive().equals("false")) {

                            for (Map.Entry e : vertexValue.getTwo_hop().entrySet()) {
                                if (((Nodo) e.getKey()).getID().equals(String.valueOf(m.getID()))) { //controllo che il nodo mittente è uno dei miei vicini
                                    ((Nodo) e.getKey()).setActive("false");
                                } else if (((Nodo) e.getValue()).getID().equals(String.valueOf(m.getID()))) { //controllo che il nodo mittente è uno dei miei two_hop
                                    ((Nodo) e.getValue()).setActive("false");
                                }
                            }
                        }
                    }

                    vertex.setValue(vertexValue);



                    break;
                }
                /**
                 * calcolo della similarità
                 */
                case CORE_PROCESSING_SIMILARITY_PHASE:
                    /*for (Map.Entry m : vertexValue.two_hop.entrySet())
                    {
                        System.out.println("Entry di " + vertex.getId() + " e' : key " + m.getKey() + " value: " + m.getValue());
                    }*/
                    for (Edge e : vertex.getEdges()){
                        vertexValue.addSimilarity_map(new Nodo((e.getTargetVertexId().toString()), "true"), new DoubleWritable(0));
                    }
                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        for (Map.Entry t : vertexValue.getTwo_hop().entrySet()){
                            if (((Nodo)e.getKey()).getID().equals(((Nodo)t.getKey()).getID())){
                                ((Nodo) e.getKey()).setActive(((Nodo) t.getKey()).getActive());
                            }
                        }
                    }
                    ArrayList<Nodo> neighbor_of_neighbor = new ArrayList<>();
                    for(Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        if(((Nodo)e.getKey()).getActive().equals("true")){

                            neighbor_of_neighbor = getNoN(((Nodo)e.getKey()).getID(), vertexValue.getTwo_hop());
                            double t = Similarity_calculator(vertexValue.getTwo_hop(), neighbor_of_neighbor, vertexValue.getSimilarity_map(), vertex.getId(), ((Nodo) e.getKey()).getID());
                            //System.out.println(" t = " + t);
                            e.setValue(new DoubleWritable(t));
                        }
                    }




                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        System.out.println("SIMILARITY del nodo : " + vertex.getId() + " key : " + e.getKey() + " value: " + e.getValue());
                    }
                    vertex.setValue(vertexValue);
                    break;
                case CORE_PROCESSING_TOPOLOGY_FIRST_PHASE: {
                    //long p = similarity_limit.get(getConf());
                    ArrayList<Nodo> to_be_deleted_similarity = new ArrayList<Nodo>();
                    ArrayList<Nodo> to_be_deleted_two_hop = new ArrayList<Nodo>();
                    double limite = ((double) similarity_limit.get(getConf())) / 10.0;
                    int removed = 0;
                    //System.out.println("Il numero degli archi del nodo: " + vertex.getId() + " PRIMA è: " + vertex.getNumEdges());
                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()) {
                        Nodo key = (Nodo) e.getKey();
                        DoubleWritable value = ((DoubleWritable) e.getValue());
                        if (key.getActive().equals("true")) {
                            if (value.get() < limite) {
                                //vertexValue.getSimilarity_map().remove(e.getKey());
                                to_be_deleted_similarity.add(((Nodo) e.getKey()));
                                messageValue.addEliminati(Long.valueOf(key.getID()));
                                removed++;
                                for (Map.Entry k : vertexValue.getTwo_hop().entrySet()) {
                                    if (((Nodo) k.getKey()).getActive().equals("true") && ((Nodo) k.getKey()).getID().equals(key.getID())) {
                                        //vertexValue.getTwo_hop().remove(k.getKey());
                                        to_be_deleted_two_hop.add(((Nodo) k.getKey()));
                                    }
                                }
                            }
                        }

                    }
                    messageValue.setID(vertex.getId().get());
                    for (Nodo e : to_be_deleted_similarity){
                        vertexValue.similarity_map.remove(e);
                    }
                    for (Nodo e : to_be_deleted_two_hop)
                    {
                        for (Iterator<Map.Entry<Nodo,Nodo>> it = vertexValue.getTwo_hop().entrySet().iterator(); it.hasNext();) {
                            Map.Entry<Nodo,Nodo> k = it.next();
                            if (e.getID().equals(k.getKey().getID())) {
                                /*Boolean trovato = false;
                                for (Edge j : vertex.getEdges()) {
                                    if (j.getTargetVertexId().toString().equals(k.getValue().getID())) {
                                        trovato = true;
                                        break;
                                    }
                                }
                                if (trovato != true) {
                                    sendMessage(new LongWritable(Long.valueOf(k.getValue().getID())), messageValue);
                                }*/
                                it.remove();
                            }
                        }
                    }
                    for (Nodo n : to_be_deleted_similarity ){
                        vertex.removeEdges(new LongWritable(Long.valueOf(n.getID())));
                    }
                    for (Edge e : vertex.getEdges()) {
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                    }
                    aggregate(DELETED_NODES, new IntWritable(removed));
                    vertex.setValue(vertexValue);
                    //System.out.println("Message value " + messageValue.getEliminati() + " eliminati dal nodo " + vertex.getId() + " il limite era " + limite);
                    break;
                }
                case CORE_PROCESSING_TOPOLOGY_SECOND_PHASE:
                {
                    Map<Nodo,Nodo> to_be_deleted = new HashMap<Nodo,Nodo>();
                    for (MessagesWritable m: messages)
                    {
                        System.out.println("Sono " + vertex.getId() + " devo eliminare " + m.getEliminati() + " inviatomi da " + m.getID());
                        for(long deleted : m.getEliminati())
                        {

                            for(Map.Entry e : vertexValue.getTwo_hop().entrySet())
                            {
                                Nodo key = ((Nodo) e.getKey());
                                Nodo value = ((Nodo)e.getValue());
                                if (key.getActive().equals("true") && key.getID().equals(m.getID().toString()) && value.getActive().equals("true") && value.getID().equals(String.valueOf(deleted))){
                                    to_be_deleted.put(((Nodo) e.getKey()), ((Nodo) e.getValue()));
                                    //vertexValue.getTwo_hop().remove(e.getKey());
                                }

                            }
                        }

                    }

                    for (Map.Entry e : to_be_deleted.entrySet())
                    {
                        for (Iterator<Map.Entry<Nodo,Nodo>> it = vertexValue.getTwo_hop().entrySet().iterator(); it.hasNext();) {
                            Map.Entry<Nodo,Nodo> k = it.next();
                            if (((Nodo) e.getKey()).getID().equals(k.getKey().getID()) && ((Nodo) e.getValue()).getID().equals(k.getValue().getID())) {
                                it.remove();
                            }
                        }
                    }
                    for(Map.Entry e : vertexValue.getSimilarity_map().entrySet())
                    {
                        System.out.println(" vicini key " + e.getKey() + " nodo " + e.getValue() + " id " + vertex.getId());
                    }
                    for(Map.Entry e : vertexValue.getTwo_hop().entrySet())
                    {
                        System.out.println(" map two_hop key " + e.getKey() + " nodo " + e.getValue() + " id " + vertex.getId());
                    }
                    vertex.setValue(vertexValue);

                    break;
                }
                case POST_PROCESSING:
                    vertex.voteToHalt();
                    break;
            }
        }
        else
        {
            vertex.voteToHalt();
        }

    }

    /**
     *
     * @param id - intermediario
     * @param two_hop - vicini dell'intermediario
     * @return
     */

    public ArrayList<Nodo> getNoN (String id, Map two_hop){
        ArrayList<Nodo> tmp = new ArrayList<>();
        for (Object e : two_hop.entrySet()){
            Nodo key = ((Nodo)((Map.Entry) e).getKey());
            Nodo value = ((Nodo)((Map.Entry)e).getValue());
            if(key.getID().equals(id)){
                if(value.getActive().equals("true")){
                    tmp.add(value);
                }
            }
        }
        return tmp;
    }
    public double Similarity_calculator(Map two_hop, ArrayList array, Map similarity, LongWritable id, String vicino){
        double p =1d;
        double d = 0;
        ArrayList<Nodo> tmp = new ArrayList<>();
        for (Object e : similarity.entrySet())
        {
            Nodo e_key = ((Nodo)((Map.Entry)e).getKey());
            if (e_key.getActive().equals("true"))
            {
                d++;
            }
        }
        /*Boolean trovato = false;
        for (Object e : two_hop.entrySet())
        {
            Nodo e_key = ((Nodo)((Map.Entry)e).getKey());
            Nodo e_value = ((Nodo)((Map.Entry)e).getValue());
            System.out.println("Sono id " + id + " key " + e_key + " value " + e_value);
            if(e_value.getID().equals(vicino) && e_value.getActive().equals("true") && e_key.getActive().equals("true"))
            {
                System.out.println("Sono qui");
                trovato = true;
            }
        }*/
        ArrayList<Nodo> common_nodes = new ArrayList<>();
        int q = 0;
        /**
         * otteniamo C(u,v) - intersezione dei vicini
         */
        for (Object e : similarity.entrySet()) {
            Nodo e_key = ((Nodo) ((Map.Entry) e).getKey());
            if (e_key.getActive().equals("true")) {

                for (Object n : array) {
                    Nodo x = ((Nodo) n);

                    if (e_key.getID().equals(x.getID())) {
                        common_nodes.add(e_key);
                    }
                }
            }
        }
        p = common_nodes.size();
        /*for (Nodo x : common_nodes){
            System.out.println("Common nodes tra: " + id + " e " + vicino +" è: " + x);
        }*/
        for (Nodo c : common_nodes) {
            for (Object k : getNoN(c.getID(), two_hop)) {
                //System.out.println("k : " + k.toString()+ " common:nodes " + common_nodes);
                for (Nodo j : common_nodes) {
                    if (j.getID().equals(((Nodo) k).getID())) {
                        // System.out.println(" Sono nell'if ");
                        q = q + 1;
                    }
                }
            }
            p = p + q / 2;
        }

        tmp = getNoN(vicino, two_hop);
        d = d + tmp.size();
        double res = p/d;
        //System.out.println("P : " + p + " D : " + d + " tmp " + tmp + " id " + id + " vicino " + vicino + " res " + res);
        return res;
    }
}
