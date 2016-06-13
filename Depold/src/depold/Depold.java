package depold;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import org.python.antlr.ast.Str;
import org.python.apache.xerces.impl.XMLEntityManager;
import sun.misc.resources.Messages;

import java.io.IOException;
import java.security.BasicPermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
   public static final LongConfOption threshold =
           new LongConfOption("Depold.threshold", 1,
                   "threshold");
    //public int threshold;

    @Override
    public void compute(Vertex<LongWritable, THALS, FloatWritable> vertex, Iterable<MessagesWritable> messages) throws IOException {
        THALS vertexValue = vertex.getValue();
        MessagesWritable messageValue = new MessagesWritable();

        if(vertexValue.getActive().equals("true")){

            switch (vertexValue.getFase()){
                /**
                 * MANDO LA LISTA DEI MIEI VICINI AI MIEI VICINI
                 */
                case 0:{
                    for(Edge e : vertex.getEdges()){
                      messageValue.addVicini(Long.valueOf(e.getTargetVertexId().toString())); //mi salvo tutti i miei vicini in una lista
                    }


                    messageValue.setID(Long.valueOf(vertex.getId().toString()));

                    vertexValue.setFase(1);
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
                case 1: {
                    for (Edge e : vertex.getEdges()){
                        messageValue.addVicini(Long.valueOf(e.getTargetVertexId().toString()));
                    }
                    messageValue.setID(Long.valueOf(vertex.getId().toString()));

                    for (MessagesWritable m : messages) {
                        sendMessage(new LongWritable(m.getID()), messageValue);
                    }
                    //System.out.println("Sono : " +vertex.getId() + " sto finendo la fase : " + vertexValue.getFase());
                    vertexValue.setFase(vertexValue.getFase()+1);
                    vertex.setValue(vertexValue);




                    break;
                }
                    /**
                     * COSTRUISCO LA TWO_HOP
                     */
                case 2:{


                    for (MessagesWritable m : messages){
                        for (Long x : m.getVicini()) {

                            if (!(x.toString().equals(vertex.getId().toString()))) {
                                vertexValue.addTwo_hop(new Nodo(m.getID().toString(), "true"), new Nodo(x.toString(), "true"));
                            }
                        }
                    }

                    int test = ((int) threshold.get(getConf()));

                    if(vertex.getNumEdges() > test){

                        vertexValue.setActive("false");
                        messageValue.setActive("false");

                        messageValue.setID(vertex.getId().get());

                    }else{

                        messageValue.setActive("true");
                        messageValue.setID(vertex.getId().get());
                    }


                    vertexValue.setFase(vertexValue.getFase()+1);
                    vertex.setValue(vertexValue);
                    for (Edge e : vertex.getEdges()) {
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                    }
                    break;
                }
                /**
                 * FILTRAGGIO DEI NODI
                 */
                case 3: {
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



                    vertexValue.setFase(vertexValue.getFase()+1);
                    vertex.setValue(vertexValue);
                    messageValue.setID(vertex.getId().get());
                    for (Edge e: vertex.getEdges()){
                        //System.out.println("sono: " + vertex.getId() + " sto mandando a : " + e.getTargetVertexId());
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                    }


                    break;
                }
                case 4:
                    //System.out.println("Vertice: " + vertex.getId() + " nella core-processing");
                    for (Edge e : vertex.getEdges()){
                       /* for (Map.Entry m : vertexValue.getTwo_hop().entrySet()){
                            if(vertexValue.getSimilarity_map().containsKey(((Nodo)m.getKey()))){
                                System.out.println("Sono nell'if : " + m.getKey() + " del nodo " + vertex.getId());
                                continue;
                            } else {
                               vertexValue.addSimilarity_map(((Nodo)m.getKey()), new DoubleWritable(0));
                            }
                        }*/
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
                           // e.setValue(Similarity_calculator());

                            neighbor_of_neighbor = getNoN(((Nodo)e.getKey()).getID(), vertexValue.getTwo_hop());
                            //System.out.println("PROVA del vicino: "+ ((Nodo)e.getKey()).getID() + " è: " + neighbor_of_neighbor + " sono nel nodo " + vertex.getId());
                            //e.getValue()
                            double t = Similarity_calculator(vertexValue.getTwo_hop(), neighbor_of_neighbor, vertexValue.getSimilarity_map(), vertex.getId(), ((Nodo) e.getKey()).getID());
                            System.out.println(" t = " + t);
                            e.setValue(t);
                        }
                    }



                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        System.out.println("SIMILARITY del nodo : " + vertex.getId() + " key : " + e.getKey() + " value: " + e.getValue());
                    }
                    vertex.voteToHalt();
                    break;
                case 5:
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
        double d = similarity.size();
        ArrayList<Nodo> common_nodes = new ArrayList<>();
        int q = 0;
        /**
         * otteniamo C(u,v) - intersezione dei vicini
         */
        for (Object e : similarity.entrySet()){
            Nodo e_key = ((Nodo)((Map.Entry)e).getKey());
            if (e_key.getActive().equals("true")) {

                for (Object n : array) {
                    Nodo x = ((Nodo) n);

                    if (e_key.getID().equals(x.getID())) {
                      common_nodes.add(e_key);
                    }
                }
            }
        }
        p = p + common_nodes.size();
        for (Nodo x : common_nodes){
            System.out.println("Common nodes tra: " + id + " e " + vicino +" è: " + x);
        }
        for (Nodo c : common_nodes){
            for(Object k : getNoN(c.getID(), two_hop)){
                //System.out.println("k : " + k.toString()+ " common:nodes " + common_nodes);
                for (Nodo j : common_nodes) {
                    if (j.getID().equals(((Nodo)k).getID())) {
                       // System.out.println(" Sono nell'if ");
                        q = q + 1;
                    }
                }
            }
            p = p+q/2;
        }

        ArrayList<Nodo> tmp = getNoN(vicino, two_hop);
        d = d+tmp.size()+1;
        double res = p/d;
        System.out.println("P : " + p + " D : " + d + " tmp " + tmp + " id " + id + " vicino " + vicino + " res " + res);
        return res;
    }
}
