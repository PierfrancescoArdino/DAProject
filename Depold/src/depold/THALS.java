package depold;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mariapia on 04/06/16.
 */
public class THALS implements Writable{
    Map <Nodo,Nodo> two_hop;
    String active; //per capire se io stesso sono stato filtrato o meno

    public Map<Nodo, Nodo> getTwo_hop() {
        return two_hop;
    }

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
    }

    public void addTwo_hop(Nodo neighbor, Nodo two_hopNeighbor){
        if (this.two_hop == null || this.two_hop.isEmpty()){
            this.two_hop = new HashMap<>();
        }
        this.two_hop.put(neighbor, two_hopNeighbor);
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput,active);
        //WritableUtils.write(dataOutput,two_hop.size());
        WritableUtils.writeString(dataOutput,String.valueOf(two_hop.size()));
        for (Map.Entry<Nodo,Nodo> e : two_hop.entrySet()){
            e.getKey().write(dataOutput);
            e.getValue().write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        active = WritableUtils.readString(dataInput);
        int size = Integer.valueOf(WritableUtils.readString(dataInput));
        if(size != 0) {
            for (int i = 0; i < size; i++) {
                Nodo key = new Nodo();
                key.readFields(dataInput);
                Nodo value = new Nodo();

                value.readFields(dataInput);
                addTwo_hop(key,value);
            }
        }
    }

    public THALS (){this.two_hop = new HashMap<>(); this.active = "true"; }
    public THALS (HashMap two_hop, String active){
        this.two_hop = two_hop;
        this.active = active;
    }


}

