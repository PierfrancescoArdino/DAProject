package depold;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mariapia on 09/06/16.
 */
public class Store_value implements Writable {
    private long dest;
    private long source;

    public long getDest() {
        return dest;
    }

    public void setDest(long dest) {
        this.dest = dest;
    }

    public long getSource() {
        return source;
    }

    public void setSource(long source) {
        this.source = source;
    }


    public Store_value(){}

    public Store_value(long source, long dest){
        this.source = source;
        this.dest = dest;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(source);
        dataOutput.writeLong(dest);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        source = dataInput.readLong();
        dest = dataInput.readLong();
    }

    @Override
    public String toString(){
        return this.source + " " + this.dest;
    }
}
