package depold;

/**
 * Created by mariapia on 04/06/16.
 */

import com.kenai.jaffl.struct.Struct;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * classe utilizzata per ricavare la two-hop adjacent list
 */

public class Nodo implements Writable{
    private String ID;
    private String active = "true";

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
    }



    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public Nodo(){}

    public Nodo(String ID, String active){
        this.ID = ID;
        this.active = active;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput,ID);
        WritableUtils.writeString(dataOutput,active);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            ID = WritableUtils.readString(dataInput);
            active = WritableUtils.readString(dataInput);
    }
}
