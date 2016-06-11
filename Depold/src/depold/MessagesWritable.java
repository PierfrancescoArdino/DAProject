package depold;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by mariapia on 05/06/16.
 */
public class MessagesWritable implements Writable{
    LongArrayList vicini;
    String active; //per mandare un messaggio e segnalare che non sono più attivo
    Long ID;


    public MessagesWritable(){
        vicini = new LongArrayList();
        active = "true";
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        int size = vicini == null ? 0 : vicini.size();
        dataOutput.writeInt(size);
        if (size != 0) {
            for (Long v : vicini) {
                dataOutput.writeLong(v);
            }
        }
        WritableUtils.writeString(dataOutput,active);
        dataOutput.writeLong(ID);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        vicini.clear();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                addVicini(dataInput.readLong());
                //System.out.println("Vicini : " + getVicini());
            }
        }
        active = WritableUtils.readString(dataInput);
        ID = dataInput.readLong();

    }

    public void addVicini(long value){
        vicini.add(value);
    }

    public LongArrayList getVicini(){
        return vicini;
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




}
