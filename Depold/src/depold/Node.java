package depold;


import com.kenai.jaffl.struct.Struct;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by
 * Ardino Pierfrancesco
 * Natale Maria Pia
 * Tovo Alessia
 *
 * class used to store information for the maps of two_hop adjacent list and similarity values
 */

public class Node implements Writable{
    private Long ID;
    private Boolean active = Boolean.TRUE;

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }



    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public Node(){}

    public Node(Long ID, Boolean active){
        this.ID = ID;
        this.active = active;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(ID);
        dataOutput.writeBoolean(active);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            ID = dataInput.readLong();
            active = dataInput.readBoolean();
    }

    @Override
    public String toString(){
        return this.ID.toString() + " " + this.active.toString();
    }
}
