package depold;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 
 * Ardino Pierfrancesco 
 * Natale Maria Pia
 * Tovo Alessia
 * 
 * class used to store the information of a vertex
 */
public class THALS implements Writable{
    /**
     * map which stores the two_hop adjacent list of a node
     * @key neighbor
     * @value two_hop neighbor
     */
    Map <Node, Node> two_hop;
    /**
     * variable used to activate and deactivate a node for filthering phase
     */
    Boolean active;
    /**
     * variable used to store the id of the community which the node belongs
     */
    long group_id;
    /**
     * array used to store all members of the community which the node belongs
     */
    ArrayList<Node_Degree> community_members;
    /**
     * map used to store the similarity values of the neighbors
     * @key neighbor
     * @value similarity value
     */
    Map<Node, DoubleWritable> similarity_map;
    /**
     * array used to store multiple communities
     * used only by filtered nodes
     */
    ArrayList<Node_Degree> community_filtered_node;

    public Map<Node, Node> getTwo_hop() {
        return two_hop;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public void addTwo_hop(Node neighbor, Node two_hopNeighbor){
        if (this.two_hop == null || this.two_hop.isEmpty()){
            this.two_hop = new HashMap<>();
        }
        this.two_hop.put(neighbor, two_hopNeighbor);
    }

    public void addSimilarity_map(Node key, DoubleWritable value){
        if (this.similarity_map == null || this.similarity_map.isEmpty()){
            this.similarity_map = new HashMap<>();
        }
        this.similarity_map.put(key,value);
    }

    public void addCommunity_members(Node_Degree s){
        community_members.add(s);
    }
    public void addCommunity_filtered_node(Node_Degree s) {community_filtered_node.add(s);}


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(active);
        WritableUtils.writeString(dataOutput,String.valueOf(two_hop.size()));
        for (Map.Entry<Node, Node> e : two_hop.entrySet()){
            e.getKey().write(dataOutput);
            e.getValue().write(dataOutput);
        }

        WritableUtils.writeString(dataOutput,String.valueOf(similarity_map.size()));
        for (Map.Entry<Node,DoubleWritable> e : similarity_map.entrySet()){
            e.getKey().write(dataOutput);
            e.getValue().write(dataOutput);
        }

        dataOutput.writeLong(group_id);

        int size = community_members == null ? 0 : community_members.size();
        dataOutput.writeInt(size);
        if (size != 0) {
            for (Node_Degree v : community_members) {
                v.write(dataOutput);
            }
        }

        int dim = community_filtered_node == null ? 0 : community_filtered_node.size();
        dataOutput.writeInt(dim);
        if (dim != 0 ){
            for(Node_Degree n : community_filtered_node){
                n.write(dataOutput);
            }
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        active = dataInput.readBoolean();
        int size = Integer.valueOf(WritableUtils.readString(dataInput));
        if(size != 0) {
            for (int i = 0; i < size; i++) {
                Node key = new Node();
                key.readFields(dataInput);
                Node value = new Node();

                value.readFields(dataInput);
                addTwo_hop(key,value);
            }
        }

        int dim = Integer.valueOf(WritableUtils.readString(dataInput));
        if(dim != 0) {
            for (int i = 0; i < dim; i++) {
                Node key = new Node();
                key.readFields(dataInput);
                DoubleWritable value = new DoubleWritable();

                value.readFields(dataInput);
                addSimilarity_map(key,value);
            }
        }
        long group_id = dataInput.readLong();
        int d = dataInput.readInt();
        community_members.clear();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                Node_Degree s = new Node_Degree(dataInput.readLong(),dataInput.readLong());
                addCommunity_members(s);
            }
        }

        int dime = dataInput.readInt();
        community_filtered_node.clear();
        if(dime != 0){
            for (int i=0; i<dime; i++){
                Node_Degree s = new Node_Degree(dataInput.readLong(),dataInput.readLong());
                addCommunity_filtered_node(s);
            }
        }

    }

    public THALS (){
        this.two_hop = new HashMap<>(); 
        this.active = Boolean.TRUE;
        this.similarity_map = new HashMap<>(); 
        this.community_members= new ArrayList<>(); 
        this.community_filtered_node = new ArrayList<>();
    }

    public Map<Node, DoubleWritable> getSimilarity_map() {
        return similarity_map;
    }

    public void setSimilarity_map(Map<Node, DoubleWritable> similarity_map) {
        this.similarity_map = similarity_map;
    }

    public long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(long group_id) {
        this.group_id = group_id;
    }

    public ArrayList<Node_Degree> getCommunity_members() {
        return community_members;
    }

    public void setCommunity_members(ArrayList community_members) {
        this.community_members = community_members;
    }

    public ArrayList<Node_Degree> getCommunity_filtered_node() {
        return community_filtered_node;
    }

    public void setCommunity_filtered_node(ArrayList<Node_Degree> community_filtered_node) {
        this.community_filtered_node = community_filtered_node;
    }
}