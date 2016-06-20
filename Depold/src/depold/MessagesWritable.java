package depold;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;


/**
 * Created by 
 * Ardino Pierfrancesco
 * Natale Maria Pia
 * Tovo Alessia
 * 
 * class used to exchange messages between nodes
 */
public class MessagesWritable implements Writable{
    /**
     * variable used to send the ID of the sender node
     */
    Long ID;
    /**
     * array used to send the list of neighbors
     */
    LongArrayList neighbors;
    /**
     * variable used to send the status of the node
     */
    String active;
    /**
     * array used to send the ID of the deleted nodes
     */
    LongArrayList deleted_nodes;
    /**
     * variable used to send the ID of the community which the node belongs
     */
    Long group_id;
    /**
     * array used to send the members of the community which the node belongs
     */
    ArrayList<Node_Degree> community_members;

    public MessagesWritable(){
        neighbors = new LongArrayList();
        active = "true";
        deleted_nodes = new LongArrayList();
        group_id = new Long(0);
        community_members = new ArrayList<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        int size = neighbors == null ? 0 : neighbors.size();
        dataOutput.writeInt(size);
        if (size != 0) {
            for (Long v : neighbors) {
                dataOutput.writeLong(v);
            }
        }
        WritableUtils.writeString(dataOutput,active);
        dataOutput.writeLong(ID);
        int dim = deleted_nodes == null ? 0:deleted_nodes.size();
        dataOutput.writeInt(dim);
        if(dim != 0){
            for (Long v: deleted_nodes){
                dataOutput.writeLong(v);
            }
        }

        dataOutput.writeLong(group_id);

        int d = community_members == null ? 0 : community_members.size();
        dataOutput.writeInt(d);
        if (d != 0){
            for (Node_Degree s : community_members){
                s.write(dataOutput);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        neighbors.clear();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                addNeighbors(dataInput.readLong());
            }
        }
        active = WritableUtils.readString(dataInput);
        ID = dataInput.readLong();

        int dim = dataInput.readInt();
        deleted_nodes.clear();
        if (dim != 0){
            for (int i=0; i<dim ; i++){
                addDeleted_nodes(dataInput.readLong());
            }
        }
        group_id = dataInput.readLong();

        int d = dataInput.readInt();
        community_members.clear();
        if (d != 0 ){
            for (int i=0; i<d; i++){
                Node_Degree tmp = new Node_Degree(dataInput.readLong(),dataInput.readLong());
                addCommunity_members(tmp);
            }
        }
    }

    public void addNeighbors(long value){
        neighbors.add(value);
    }

    public void addDeleted_nodes(long value){
        deleted_nodes.add(value);
    }

    public void addCommunity_members(Node_Degree s ) { community_members.add(s);}

    public LongArrayList getNeighbors(){
        return neighbors;
    }

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
    }

    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }


    public LongArrayList getDeleted_nodes() {
        return deleted_nodes;
    }

    public void setDeleted_nodes(LongArrayList deleted_nodes) {
        this.deleted_nodes = deleted_nodes;
    }


    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }


    public ArrayList<Node_Degree> getCommunity_members() {
        return community_members;
    }

    public void setCommunity_members(ArrayList<Node_Degree> community_members) {
        this.community_members = community_members;
    }



}
