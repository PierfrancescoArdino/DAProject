package depold;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static depold.DepoldMaster.*;

/**
 * Created by mariapia on 04/06/16.
 */
public class Depold extends BasicComputation <LongWritable, THALS, FloatWritable, MessagesWritable>{

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
        if(vertexValue.getActive().equals("true") || currPhase == Phases.POST_PROCESSING_GROUP_DETECTOR){

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

                    for (Edge e : vertex.getEdges()){
                        messageValue.addVicini(Long.valueOf(e.getTargetVertexId().toString()));
                    }
                    messageValue.setID(Long.valueOf(vertex.getId().toString()));
                    sendMessageToAllEdges(vertex,messageValue);
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
                    vertexValue.getSimilarity_map().clear();
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
                            e.setValue(new DoubleWritable(t));
                        }
                    }

                    vertex.setValue(vertexValue);
                    break;
                /**
                 * vengono eliminati i nodi nella two_hop e nella lista dei vicini
                 */
                case CORE_PROCESSING_TOPOLOGY_FIRST_PHASE: {
                    ArrayList<Nodo> to_be_deleted_similarity = new ArrayList<Nodo>();
                    ArrayList<Nodo> to_be_deleted_two_hop = new ArrayList<Nodo>();
                    double limite = ((double) similarity_limit.get(getConf())) / 10.0;
                    int removed = 0;
                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()) {
                        Nodo key = (Nodo) e.getKey();
                        DoubleWritable value = ((DoubleWritable) e.getValue());
                        if (key.getActive().equals("true")) {
                            if (value.get() < limite) {
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
                    break;
                }
                /**
                 * vengono eliminati i nodi restanti della two_hop
                 */
                case CORE_PROCESSING_TOPOLOGY_SECOND_PHASE:
                {
                    Map<Nodo,Nodo> to_be_deleted = new HashMap<Nodo,Nodo>();
                    for (MessagesWritable m: messages)
                    {
                        for(long deleted : m.getEliminati())
                        {

                            for(Map.Entry e : vertexValue.getTwo_hop().entrySet())
                            {
                                Nodo key = ((Nodo) e.getKey());
                                Nodo value = ((Nodo)e.getValue());
                                if (key.getActive().equals("true") && key.getID().equals(m.getID().toString()) && value.getActive().equals("true") && value.getID().equals(String.valueOf(deleted))){
                                    to_be_deleted.put(((Nodo) e.getKey()), ((Nodo) e.getValue()));
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
                    vertex.setValue(vertexValue);

                    break;
                }
                case POST_PROCESSING_WCC_FIRST:{
                    vertexValue.setGroup_id(vertex.getId().get());
                    messageValue.setID(vertex.getId().get());
                    messageValue.setGroup_id(vertex.getId().get());
                    sendMessageToAllEdges(vertex, messageValue);
                    vertex.setValue(vertexValue);
                    break;
                }
                /**
                 * calcolo delle componenti connesse
                 */
                case POST_PROCESSING_WCC_SECOND: {
                    long minValue = vertexValue.getGroup_id();
                    for (MessagesWritable m : messages) {
                        if (m.getGroup_id() < minValue) {
                            minValue = m.getGroup_id();
                        }
                    }
                    if (minValue < vertexValue.getGroup_id()) {
                        vertexValue.setGroup_id(minValue);
                        messageValue.setID(vertex.getId().get());
                        messageValue.setGroup_id(minValue);
                        aggregate(WCC, new IntWritable(1));
                        sendMessageToAllEdges(vertex, messageValue);
                    }
                    vertex.setValue(vertexValue);
                    break;
                }
                case POST_PROCESSING_DEGREE_CALCULATOR_FIRST:{
                    int vicini=0;

                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        Nodo key = ((Nodo) e.getKey());
                        if (key.getActive().equals("true")){
                            vicini++;
                            ArrayList<Nodo> tmp = getNoN(key.getID(),vertexValue.getTwo_hop());
                            int size = tmp.size();
                            vertexValue.addComunita(new Nodo_Degree(Long.valueOf(key.getID()),Long.valueOf((long)size)));
                            messageValue.addElementiComunita(new Nodo_Degree(Long.valueOf(key.getID()),Long.valueOf((long)size)));
                        }
                    }
                    if (vicini >0){
                        vertexValue.addComunita(new Nodo_Degree(new Long(vertex.getId().get()),new Long((long)vicini)));}
                    else{
                        vertexValue.addComunita(new Nodo_Degree(new Long(vertex.getId().get()),new Long(1)));
                    }

                    messageValue.setID(vertex.getId().get());
                    vertex.setValue(vertexValue);
                    sendMessageToAllEdges(vertex,messageValue);
                    break;
                }
                case POST_PROCESSING_DEGREE_CALCULATOR_SECOND:{
                    int inseriti = 0;
                    for (MessagesWritable m : messages){
                        for (Nodo_Degree v : m.getElementi_comunita()){
                            Boolean trovato = false;
                            for (Nodo_Degree x : vertexValue.getComunita()){

                                if(x.getId().longValue() == v.getId().longValue()){
                                    trovato = true;
                                }
                            }
                            if (!trovato){
                                inseriti++;
                                vertexValue.addComunita(v);
                            }
                        }
                    }
                    for (MessagesWritable m : messages){
                        for (Nodo_Degree v:m.getElementi_comunita()){
                            Boolean trovato = false;
                            for (Nodo_Degree f : messageValue.getElementi_comunita()){
                                if (f.getId().longValue()==v.getId().longValue()){
                                    trovato = true;
                                }
                            }
                            if (!trovato){
                                messageValue.addElementiComunita(v);
                            }
                        }
                    }
                    aggregate(GROUP_DEGREE, new IntWritable(inseriti));
                    messageValue.setID(vertex.getId().get());
                    vertex.setValue(vertexValue);
                    sendMessageToAllEdges(vertex, messageValue);

                    break;
                }
                case POST_PROCESSING_DEGREE_CALCULATOR_THIRD:{

                    int total_degree=0;
                    for(Nodo_Degree v : vertexValue.getComunita()){
                        total_degree=total_degree + v.getDegree().intValue();
                    }
                    double degree_media=0.0d;
                    degree_media = total_degree/vertexValue.getComunita().size();

                    messageValue.setElementi_comunita(vertexValue.getComunita());
                    messageValue.setGroup_id(vertexValue.getGroup_id());
                    messageValue.setID(vertex.getId().get());

                    for (Map.Entry m : vertexValue.getSimilarity_map().entrySet()){
                        if(((Nodo) m.getKey()).getActive().equals("false")){
                            sendMessage(new LongWritable(Long.valueOf(((Nodo) m.getKey()).getID())),messageValue);
                        }
                    }
                    break;
                }
                case POST_PROCESSING_GROUP_DETECTOR:{
                    if (vertexValue.getActive().equals("false")){
                        vertexValue.setActive("true");
                    } else {
                        int vicini_true=0;

                        for(Map.Entry m : vertexValue.getSimilarity_map().entrySet()){
                            if(((Nodo) m.getKey()).getActive().equals("false")){
                                ((Nodo) m.getKey()).setActive("true");
                            } else {
                                vicini_true++;
                            }
                        }
                        vertexValue.addComunita_filtrati(new Nodo_Degree((new Long( vertexValue.getGroup_id())), new Long(vicini_true)));
                        for(Map.Entry m : vertexValue.getTwo_hop().entrySet()){
                            if((((Nodo) m.getKey()).getActive().equals("false")) || (((Nodo) m.getValue()).getActive().equals("false"))){
                                ((Nodo) m.getKey()).setActive("true");
                                ((Nodo) m.getValue()).setActive("true");
                            }
                        }
                    }

                    ArrayList<Long> comunita_viste = new ArrayList<>();
                    for (MessagesWritable m : messages) {
                        if (!comunita_viste.contains(new Long(m.getGroup_id().longValue()))) {
                            comunita_viste.add(new Long(m.getGroup_id().longValue()));
                            double grado = 0d;
                            double grado_community = 0d;
                            for (Nodo_Degree n : m.getElementi_comunita()) {
                                for (Edge e : vertex.getEdges()) {
                                    if (e.getTargetVertexId().toString().equals(n.getId().toString())) {
                                        grado++;
                                    }
                                }
                                grado_community += n.getDegree().doubleValue();

                            }
                            double average_degree = grado_community / m.getElementi_comunita().size();
                            if(grado>average_degree){
                                vertexValue.addComunita_filtrati(new Nodo_Degree(new Long(m.getGroup_id().longValue()), new Long((long) grado)));
                                messageValue.setElementi_comunita(m.getElementi_comunita());
                                messageValue.addElementiComunita(new Nodo_Degree(new Long(vertex.getId().get()), new Long(((long) grado))));
                                messageValue.setID(vertex.getId().get());
                                sendMessage(new LongWritable(m.getID()),messageValue);
                            }

                        }
                    }
                    break;
                }
                case POST_PROCESSING_COMPUTE_COMMUNITIES:{
                    vertex.voteToHalt();
                    break;
                }

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
        Boolean trovato = false;
        for (Object e : two_hop.entrySet())
        {
            Nodo e_key = ((Nodo)((Map.Entry)e).getKey());
            Nodo e_value = ((Nodo)((Map.Entry)e).getValue());
            if(e_value.getID().equals(vicino) && e_value.getActive().equals("true") && e_key.getActive().equals("true"))
            {
               trovato = true;
            }
        }
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
        if (trovato == true) {
            p = common_nodes.size();
        }
        for (Nodo c : common_nodes) {
            for (Object k : getNoN(c.getID(), two_hop)) {
                for (Nodo j : common_nodes) {
                    if (j.getID().equals(((Nodo) k).getID())) {
                        q = q + 1;
                    }
                }
            }
            p = p + q / 2;
        }
        tmp = getNoN(vicino, two_hop);
        if(trovato==true)
        {
            d = 1 + Math.min((int)d, tmp.size());
        }
        else{
        d = d + tmp.size();}
        double res = p/d;
        return res;
    }
}
