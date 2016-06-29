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
 * Created by 
 * Ardino Pierfrancesco
 * Natale Maria Pia
 * Tovo Alessia
 */
public class Depold extends BasicComputation <LongWritable, THALS, FloatWritable, MessagesWritable>{

    private DepoldMaster.Phases currPhase;
    private static final LongConfOption threshold = new LongConfOption("Depold.threshold", 1, "threshold");
    private static final LongConfOption similarity_limit = new LongConfOption("Depold.similarity_limit" , 1, "similarity_limit");
    
    @Override
    public void preSuperstep() {
        IntWritable phaseInt = getAggregatedValue(PHASE);
        currPhase = DepoldMaster.getPhase(phaseInt);
    }
    @Override
    public void compute(Vertex<LongWritable, THALS, FloatWritable> vertex, Iterable<MessagesWritable> messages) throws IOException {
        THALS vertexValue = vertex.getValue();
        MessagesWritable messageValue = new MessagesWritable();
        if(vertexValue.getActive().equals(true) || currPhase == Phases.POST_PROCESSING_GROUP_DETECTOR){

            switch (currPhase){
                /**
                 * phase used to send the adjacency list to the neighbors of the node
                 */
                case PRE_PROCESSING_TWO_HOP_FIRST_PHASE:{
                    for(Edge e : vertex.getEdges()){
                      messageValue.addNeighbors(Long.valueOf(e.getTargetVertexId().toString()));
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
                 * phase used to send the adjacency list received from the neighbors of the node to all its neighbors
                 * here the undirected graph is computed
                 */
                case PRE_PROCESSING_TWO_HOP_SECOND_PHASE: {
                    for (MessagesWritable m : messages){
                        Boolean found = false;
                        for (Edge e : vertex.getEdges()){
                            if(e.getTargetVertexId().toString().equals(m.getID().toString())){
                                found = true;
                            }
                        }
                        if (!found){
                            vertex.addEdge(EdgeFactory.create(new LongWritable(m.getID()), new FloatWritable(0)));
                        }
                    }

                    for (Edge e : vertex.getEdges()){
                        messageValue.addNeighbors(Long.valueOf(e.getTargetVertexId().toString()));
                    }
                    
                    messageValue.setID(Long.valueOf(vertex.getId().toString()));
                    sendMessageToAllEdges(vertex,messageValue);
                    vertex.setValue(vertexValue);
                    break;
                }
                    /**
                     * phase used to create and store the two_hop map
                     * the nodes with a degree higher than the filtering_limit are deactivated
                     */
                case PRE_PROCESSING_TWO_HOP_THIRD_PHASE:{
                    for (MessagesWritable m : messages){
                        for (Long x : m.getNeighbors()) {
                            vertexValue.addTwo_hop(new Node(m.getID(), true), new Node(x, true));

                        }
                    }

                    int filtering_limit = ((int) threshold.get(getConf()));
                    if(vertex.getNumEdges() > filtering_limit){
                        aggregate(DepoldMaster.FILTERED_NODES,new IntWritable(1));
                        vertexValue.setActive(false);
                        messageValue.setActive(false);
                        messageValue.setID(vertex.getId().get());

                    }else{
                        aggregate(DepoldMaster.FILTERED_NODES,new IntWritable(0));
                        messageValue.setActive(true);
                        messageValue.setID(vertex.getId().get());
                    }

                    vertex.setValue(vertexValue);
                    for (Map.Entry e : vertexValue.getTwo_hop().entrySet())
                    {
                        sendMessage(new LongWritable(((Node) e.getValue()).getID()),messageValue);
                    }
                    for (Edge e : vertex.getEdges()) {
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                    }
                    break;
                }
                /**
                 * phase used to deactivate the edges between the filtered nodes and their neighbors
                 */
                case PRE_PROCESSING_SECOND_PHASE: {
                    for(MessagesWritable m : messages){
                        if(m.getActive().equals(false)) {

                            for (Map.Entry e : vertexValue.getTwo_hop().entrySet()) {
                                if (((Node) e.getKey()).getID().longValue()==m.getID().longValue()) {
                                    ((Node) e.getKey()).setActive(false);
                                } else if (((Node) e.getValue()).getID().longValue()==m.getID()) {
                                    ((Node) e.getValue()).setActive(false);
                                }
                            }
                        }
                    }

                    vertex.setValue(vertexValue);

                    break;
                }
                /**
                 * phase used to compute the similarity between a node and its active neighbors
                 */
                case CORE_PROCESSING_SIMILARITY_PHASE:
                    vertexValue.getSimilarity_map().clear();
                    for (Edge e : vertex.getEdges()){
                        vertexValue.addSimilarity_map(new Node(new Long(e.getTargetVertexId().toString()), true), new DoubleWritable(0));
                    }

                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        for (Map.Entry t : vertexValue.getTwo_hop().entrySet()){
                            if (((Node)e.getKey()).getID().equals((((Node)t.getKey()).getID()))){
                                ((Node) e.getKey()).setActive(((Node) t.getKey()).getActive());
                            }
                        }
                    }
                    ArrayList<Node> neighbor_of_neighbor;
                    for(Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        if(((Node)e.getKey()).getActive().equals(true)){
                            
                            neighbor_of_neighbor = getNoN(((Node)e.getKey()).getID(), vertexValue.getTwo_hop());
                            double similarity = Similarity_calculator(vertexValue.getTwo_hop(), neighbor_of_neighbor, vertexValue.getSimilarity_map(), ((Node) e.getKey()).getID());
                            e.setValue(new DoubleWritable(similarity));
                        }
                    }
                    vertex.setValue(vertexValue);
                    break;
                /**
                 * phase used to delete the edges between nodes with a similarity lower than a threshold
                 */
                case CORE_PROCESSING_TOPOLOGY_FIRST_PHASE: {
                    ArrayList<Node> to_be_deleted_similarity = new ArrayList<Node>();
                    ArrayList<Node> to_be_deleted_two_hop = new ArrayList<Node>();
                    String tmp = String.valueOf(similarity_limit.get(getConf()));
                    double similarity_threshold = Double.valueOf(tmp)/ Math.pow(10,tmp.length());
                    int removed = 0;

                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()) {
                        Node key = (Node) e.getKey();
                        DoubleWritable value = ((DoubleWritable) e.getValue());
                        if (key.getActive().equals(true)) {
                            if (value.get() < similarity_threshold) {
                                to_be_deleted_similarity.add(((Node) e.getKey()));
                                messageValue.addDeleted_nodes(key.getID());
                                removed++;
                                
                                for (Map.Entry k : vertexValue.getTwo_hop().entrySet()) {
                                    if (((Node) k.getKey()).getActive().equals(true) && ((Node) k.getKey()).getID().equals(key.getID())) {
                                        to_be_deleted_two_hop.add(((Node) k.getKey()));
                                    }
                                }
                            }
                        }

                    }
                    messageValue.setID(vertex.getId().get());
                    for (Node e : to_be_deleted_similarity){
                        vertexValue.similarity_map.remove(e);
                    }
                    for (Node e : to_be_deleted_two_hop)
                    {
                        for (Iterator<Map.Entry<Node, Node>> it = vertexValue.getTwo_hop().entrySet().iterator(); it.hasNext();) {
                            Map.Entry<Node, Node> k = it.next();
                            if (e.getID().equals(k.getKey().getID())) {
                                it.remove();
                            }
                        }
                    }
                    for (Node n : to_be_deleted_similarity ){
                        vertex.removeEdges(new LongWritable(n.getID()));
                    }
                    for (Edge e : vertex.getEdges()) {
                        sendMessage(new LongWritable(Long.valueOf(e.getTargetVertexId().toString())), messageValue);
                    }
                    aggregate(DELETED_NODES, new IntWritable(removed));
                    vertex.setValue(vertexValue);
                    break;
                }
                /**
                 * phase used to delete entries of the two_hop map of the nodes deleted by the neighbor of the node
                 */
                case CORE_PROCESSING_TOPOLOGY_SECOND_PHASE:
                {
                    Map<Node, Node> to_be_deleted = new HashMap<Node, Node>();
                    for (MessagesWritable m: messages)
                    {
                        for(long deleted : m.getDeleted_nodes())
                        {

                            for(Map.Entry e : vertexValue.getTwo_hop().entrySet())
                            {
                                Node key = ((Node) e.getKey());
                                Node value = ((Node)e.getValue());
                                if (key.getActive().equals(true) && key.getID().equals(m.getID()) && value.getActive().equals(true) && value.getID().equals(deleted)){
                                    to_be_deleted.put(((Node) e.getKey()), ((Node) e.getValue()));
                                }

                            }
                        }
                    }

                    for (Map.Entry e : to_be_deleted.entrySet())
                    {
                        for (Iterator<Map.Entry<Node, Node>> it = vertexValue.getTwo_hop().entrySet().iterator(); it.hasNext();) {
                            Map.Entry<Node, Node> k = it.next();
                            if (((Node) e.getKey()).getID().equals(k.getKey().getID()) && ((Node) e.getValue()).getID().equals(k.getValue().getID())) {
                                it.remove();
                            }
                        }
                    }
                    vertex.setValue(vertexValue);

                    break;
                }
                /**
                 * phase used to initialize the weakly connected components processing
                 */
                case POST_PROCESSING_WCC_FIRST:{
                    vertexValue.setGroup_id(vertex.getId().get());
                    messageValue.setID(vertex.getId().get());
                    messageValue.setGroup_id(vertex.getId().get());
                    sendMessageToAllEdges(vertex, messageValue);
                    vertex.setValue(vertexValue);
                    break;
                }
                /**
                 * phase used to compute the weakly connected components
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
                /**
                 * phase used to compute the average degree of a community first part
                 */
                case POST_PROCESSING_DEGREE_CALCULATOR_FIRST:{
                    System.out.println("Io sono il nodo " + vertex.getId() + " la mia sim_map e' " + vertexValue.getSimilarity_map() + " la mia two hop e' " + vertexValue.getTwo_hop());
                    int neighbors=0;

                    for (Map.Entry e : vertexValue.getSimilarity_map().entrySet()){
                        Node key = ((Node) e.getKey());
                        if (key.getActive().equals(true)){
                            neighbors++;
                            ArrayList<Node> tmp = getNoN(key.getID(),vertexValue.getTwo_hop());
                            System.out.println("Sono il nodo " + vertex.getId() + " NoN del nodo " + key.getID() + " e' "+ tmp);
                            int size = tmp.size();
                            vertexValue.addCommunity_members(new Node_Degree(key.getID(),(long)size));
                            messageValue.addCommunity_members(new Node_Degree(key.getID(),(long)size));
                        }
                    }
                    if (neighbors >0){
                        vertexValue.addCommunity_members(new Node_Degree(vertex.getId().get(),(long)neighbors));}
                    else{
                        vertexValue.addCommunity_members(new Node_Degree(vertex.getId().get(),(long)(1)));
                    }

                    messageValue.setID(vertex.getId().get());
                    vertex.setValue(vertexValue);
                    sendMessageToAllEdges(vertex,messageValue);
                    break;
                }
                /**
                 * phase used to compute the average degree of a community second part
                 */
                case POST_PROCESSING_DEGREE_CALCULATOR_SECOND:{
                    int inserted = 0;
                    for (MessagesWritable m : messages){
                        for (Node_Degree v : m.getCommunity_members()){
                            Boolean found = false;
                            for (Node_Degree x : vertexValue.getCommunity_members()){

                                if(x.getId().longValue() == v.getId().longValue()){
                                    found = true;
                                }
                            }
                            if (!found){
                                inserted++;
                                vertexValue.addCommunity_members(v);
                            }
                        }
                    }
                    for (MessagesWritable m : messages){
                        for (Node_Degree v:m.getCommunity_members()){
                            Boolean found = false;
                            for (Node_Degree f : messageValue.getCommunity_members()){
                                if (f.getId().longValue()==v.getId().longValue()){
                                    found = true;
                                }
                            }
                            if (!found){
                                messageValue.addCommunity_members(v);
                            }
                        }
                    }
                    aggregate(GROUP_DEGREE, new IntWritable(inserted));
                    messageValue.setID(vertex.getId().get());
                    vertex.setValue(vertexValue);
                    sendMessageToAllEdges(vertex, messageValue);

                    break;
                }
                /**
                 * phase used to send the list of the community members to the neighbors that are deactivated
                 */
                case POST_PROCESSING_DEGREE_CALCULATOR_THIRD:{
                    messageValue.setCommunity_members(vertexValue.getCommunity_members());
                    messageValue.setGroup_id(vertexValue.getGroup_id());
                    messageValue.setID(vertex.getId().get());

                    for (Map.Entry m : vertexValue.getSimilarity_map().entrySet()){
                        if(((Node) m.getKey()).getActive().equals(false)){
                            sendMessage(new LongWritable(((Node) m.getKey()).getID()),messageValue);
                        }
                    }
                    break;
                }
                /**
                 * phase used to activate the deactivated nodes and assign them to one or more communities
                 */
                case POST_PROCESSING_GROUP_DETECTOR:{
                    if (vertexValue.getActive().equals(false)){
                        vertexValue.setActive(true);
                    } else {
                        /*int neighbors_true=0;

                        for(Map.Entry m : vertexValue.getSimilarity_map().entrySet()){
                            if(((Node) m.getKey()).getActive().equals(false)){
                                ((Node) m.getKey()).setActive(true);
                            } else {
                                neighbors_true++;
                            }
                        }
                        vertexValue.addCommunity_filtered_node(new Node_Degree(vertexValue.getGroup_id(), (long)(neighbors_true)));*/
                        for(Map.Entry m : vertexValue.getTwo_hop().entrySet()){
                            if((((Node) m.getKey()).getActive().equals(false)) || (((Node) m.getValue()).getActive().equals(false))){
                                ((Node) m.getKey()).setActive(true);
                                ((Node) m.getValue()).setActive(true);
                            }
                        }
                    }

                    ArrayList<Long> seen_communities = new ArrayList<>();
                    for (MessagesWritable m : messages) {
                        System.out.println("I'm node " + vertex.getId() + " " + m.getCommunity_members() + " " + m.getID());
                        if (!seen_communities.contains(m.getGroup_id())) {
                            seen_communities.add(m.getGroup_id());
                            double degree = 0d;
                            double degree_community = 0d;
                            for (Node_Degree n : m.getCommunity_members()) {
                                for (Edge e : vertex.getEdges()) {
                                    if (e.getTargetVertexId().toString().equals(n.getId().toString())) {
                                        degree++;
                                    }
                                }
                                degree_community += n.getDegree().doubleValue();

                            }
                            double average_degree = degree_community / m.getCommunity_members().size();
                            System.out.println(average_degree + " " + m.getGroup_id() + " " + degree + " id " + vertex.getId());
                            if(degree>average_degree){
                                vertexValue.addCommunity_filtered_node(new Node_Degree(m.getGroup_id(), (long) degree));
                            }


                        }
                    }
                    vertex.setValue(vertexValue);
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
     * @param id - neighbor of the node
     * @param two_hop - two_hop map
     * @return the active two_hop adjacency list of the neighbor
     */

    private ArrayList<Node> getNoN (Long id, Map two_hop){
        ArrayList<Node> tmp = new ArrayList<>();
        for (Object e : two_hop.entrySet()){
            Node key = ((Node)((Map.Entry) e).getKey());
            Node value = ((Node)((Map.Entry)e).getValue());
            if(key.getID().equals(id)){
                if(value.getActive().equals(true)){
                    tmp.add(value);
                }
            }
        }
        return tmp;
    }

    /**
     * 
     * @param two_hop - two_hop map
     * @param target_node_neighbors - neighbors of the target neighbor
     * @param similarity - similarity map
     * @param neighbor - the ID of the target neighbor
     * @return similarity value between the two nodes
     */
    private double Similarity_calculator(Map two_hop, ArrayList target_node_neighbors, Map similarity, Long neighbor){
        double p =1d;
        double d = 0;
        ArrayList<Node> tmp;
        for (Object e : similarity.entrySet())
        {
            Node e_key = ((Node)((Map.Entry)e).getKey());
            if (e_key.getActive().equals(true))
            {
                d++;
            }
        }
        Boolean found = false;
        for (Object e : two_hop.entrySet())
        {
            Node e_key = ((Node)((Map.Entry)e).getKey());
            Node e_value = ((Node)((Map.Entry)e).getValue());
            if(e_value.getID().equals(neighbor) && e_value.getActive().equals(true) && e_key.getActive().equals(true))
            {
               found = true;
            }
        }
        ArrayList<Node> common_nodes = new ArrayList<>();
        int q = 0;

        for (Object e : similarity.entrySet()) {
            Node e_key = ((Node) ((Map.Entry) e).getKey());
            if (e_key.getActive().equals(true)) {

                for (Object n : target_node_neighbors) {
                    Node x = ((Node) n);

                    if (e_key.getID().equals(x.getID())) {
                        common_nodes.add(e_key);
                    }
                }
            }
        }
        if (found) {
            p = common_nodes.size();
        }
        for (Node c : common_nodes) {
            for (Object k : getNoN(c.getID(), two_hop)) {
                for (Node j : common_nodes) {
                    if (j.getID().equals(((Node) k).getID())) {
                        q = q + 1;
                    }
                }
            }
            p = p + q / 2;
        }
        tmp = getNoN(neighbor, two_hop);
        if(!found)
        {
            d = 1 + Math.max((int)d, tmp.size());
        }
        else{
        d = d + tmp.size();}
        return p/d;
    }
}
