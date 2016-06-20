package depold;

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
 * class used to store the ID and the degree of a node
 */
public class Node_Degree implements Writable {
    private Long id;
    private Long degree;



    public Node_Degree(){}

    public Node_Degree(Long id, Long degree){
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
