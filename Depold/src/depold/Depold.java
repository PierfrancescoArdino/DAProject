package depold;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import sun.misc.resources.Messages;

import java.io.IOException;
import java.security.BasicPermission;
import java.util.ArrayList;

/**
 * Created by mariapia on 04/06/16.
 */
public class Depold extends BasicComputation <LongWritable, THALS, FloatWritable, MessagesWritable>{

    private DepoldMaster.Phases currPhase;
    public MessagesWritable messageValue = new MessagesWritable();


    @Override
    public void preSuperstep() {
        IntWritable phaseInt = getAggregatedValue(DepoldMaster.PHASE);
        currPhase = DepoldMaster.getPhase(phaseInt);

    }


    @Override
    public void compute(Vertex<LongWritable, THALS, FloatWritable> vertex, Iterable<MessagesWritable> messages) throws IOException {
        THALS vertexValue = vertex.getValue();
        MessagesWritable messageValue = new MessagesWritable();
        if(vertexValue.getActive().equals("true")){
            System.out.println("VERTEX VALUE: " + vertexValue.getActive());
            System.out.println("CurrPhase " + currPhase);
            switch (currPhase){
                case TWO_HOP_CREATION_FIRST:{
                    System.out.println("TWO_HOP_FIRST");
                    for(Edge e : vertex.getEdges()){
                      messageValue.addVicini(Long.valueOf(e.getTargetVertexId().toString())); //mi salvo tutti i miei vicini in una lista
                    }
                    messageValue.setID(Long.valueOf(vertex.getId().toString()));
                    for(Edge e : vertex.getEdges()){
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())),messageValue);
                    }
                    //sendMessageToAllEdges(vertex, messageValue);
                    System.out.println("MESSAGGIO: " + messageValue.getVicini() + " setID " + messageValue.getID());
                    vertex.voteToHalt();
                    break;
                }
                case TWO_HOP_CREATION_SECOND: {

                    //System.out.println("TWO_HOP_SECOND");
                    for (Edge e : vertex.getEdges()) {
                        messageValue.addVicini(Long.valueOf(e.getTargetVertexId().toString())); //mi salvo tutti i miei vicini in una lista
                    }
                    messageValue.setID(Long.valueOf(vertex.getId().toString()));
                    for (MessagesWritable m : messages) {
                        sendMessage(new LongWritable(m.getID()), messageValue);
                    }

                       // System.out.println("MESSAGGIO: " + messageValue.getVicini() + " setID " + messageValue.getID());*/
                    vertex.voteToHalt();
                    break;
                }
                case TWO_HOP_CREATION_THIRD:{
                    System.out.println("TWO_HOP_THIRD E VERTICE: " + vertex);
                    ArrayList<Long> archi = new ArrayList<>();
                    for (Edge e : vertex.getEdges()){
                        archi.add(Long.valueOf(e.getTargetVertexId().toString()));
                    }
                    for (MessagesWritable m : messages){
                        if((m.getID() != Long.valueOf(vertex.getId().toString())) || (!archi.contains(m))){
                            continue;
                        } else {
                            for (Long v : m.getVicini()) {
                                vertexValue.addTwo_hop(new Nodo(m.getID().toString(), "true"), new Nodo(v.toString(),"true"));
                            }
                        }
                    }
                    System.out.println("ID: " + vertex.getId() + " map: " + vertexValue.getTwo_hop());
                    vertex.voteToHalt();
                    break;
                }
                case PRE_PROCESSING: {

                    break;
                }
                case CORE_PROCESSING:
                    break;
                case POST_PROCESSING:
                    break;
            }
        }

    }
}
