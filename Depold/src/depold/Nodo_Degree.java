package depold;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mariapia on 09/06/16.
 */
public class Nodo_Degree implements Writable {
    private Long id;
    private Long degree;



    public Nodo_Degree(){}

    public Nodo_Degree(Long id, Long degree){
        this.id = id;
        this.degree = degree;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(degree);
        dataOutput.writeLong(id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        degree = dataInput.readLong();
        id = dataInput.readLong();
    }

    @Override
    public String toString(){
        return this.id + " " + this.degree;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDegree() {
        return degree;
    }

    public void setDegree(Long degree) {
        this.degree = degree;
    }

}
