package depold;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import sun.misc.resources.Messages;

import java.io.IOException;
import java.security.BasicPermission;
import java.util.ArrayList;
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
                    System.out.println("Sono : " +vertex.getId() + " sto finendo la fase : " + vertexValue.getFase());
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
                    System.out.println("Nella terza fase: " + vertex.getId());
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
                        System.out.println("sono: " + vertex.getId() + " sto mandando a : " + e.getTargetVertexId());
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                    }


                    break;
                }
                case 4:
                    System.out.println("Vertice: " + vertex.getId() + " nella core-processing");
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
}
