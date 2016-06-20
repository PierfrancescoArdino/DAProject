package depold;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mariapia on 04/06/16.
 */
public class THALS implements Writable{
    Map <Nodo,Nodo> two_hop; //map che ha come chiave il mio vicino(intermediario) e come valore il vicino two_hop
    String active; //per capire se io stesso sono stato filtrato o meno
    long group_id;
    ArrayList<Nodo_Degree> comunita; //contiene l'id dei nodi appartententi alla stessa comunità e il grado del nodo
    Map<Nodo, DoubleWritable> similarity_map;
    ArrayList<Nodo_Degree> comunita_filtrati;

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

    public void addSimilarity_map(Nodo key, DoubleWritable value){
        if (this.similarity_map == null || this.similarity_map.isEmpty()){
            this.similarity_map = new HashMap<>();
        }
        this.similarity_map.put(key,value);
    }

    public void addComunita(Nodo_Degree s){
        comunita.add(s);
    }
    public void addComunita_filtrati(Nodo_Degree s) {comunita_filtrati.add(s);}


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput,active);
        //WritableUtils.write(dataOutput,two_hop.size());
        WritableUtils.writeString(dataOutput,String.valueOf(two_hop.size()));
        for (Map.Entry<Nodo,Nodo> e : two_hop.entrySet()){
            e.getKey().write(dataOutput);
            e.getValue().write(dataOutput);
        }


        WritableUtils.writeString(dataOutput,String.valueOf(similarity_map.size()));
        for (Map.Entry<Nodo,DoubleWritable> e : similarity_map.entrySet()){
            e.getKey().write(dataOutput);
            e.getValue().write(dataOutput);
        }

        dataOutput.writeLong(group_id);

        int size = comunita == null ? 0 : comunita.size();
        dataOutput.writeInt(size);
        if (size != 0) {
            for (Nodo_Degree v : comunita) {
                v.write(dataOutput);
            }
        }

        int dim = comunita_filtrati == null ? 0 : comunita_filtrati.size();
        dataOutput.writeInt(dim);
        if (dim != 0 ){
            for(Nodo_Degree n : comunita_filtrati){
                n.write(dataOutput);
            }
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

        int dim = Integer.valueOf(WritableUtils.readString(dataInput));
        if(dim != 0) {
            for (int i = 0; i < dim; i++) {
                Nodo key = new Nodo();
                key.readFields(dataInput);
                DoubleWritable value = new DoubleWritable();

                value.readFields(dataInput);
                addSimilarity_map(key,value);
            }
        }
        long group_id = dataInput.readLong();
        int d = dataInput.readInt();
        comunita.clear();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                Nodo_Degree s = new Nodo_Degree(dataInput.readLong(),dataInput.readLong());
                addComunita(s);
            }
        }

        int dime = dataInput.readInt();
        comunita_filtrati.clear();
        if(dime != 0){
            for (int i=0; i<dime; i++){
                Nodo_Degree s = new Nodo_Degree(dataInput.readLong(),dataInput.readLong());
                addComunita_filtrati(s);
            }
        }

    }

    public THALS (){this.two_hop = new HashMap<>(); this.active = "true"; this.similarity_map = new HashMap<>(); this.comunita= new ArrayList<>(); this.comunita_filtrati = new ArrayList<>();}

    public Map<Nodo, DoubleWritable> getSimilarity_map() {
        return similarity_map;
    }

    public void setSimilarity_map(Map<Nodo, DoubleWritable> similarity_map) {
        this.similarity_map = similarity_map;
    }

    public long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(long group_id) {
        this.group_id = group_id;
    }

    public ArrayList<Nodo_Degree> getComunita() {
        return comunita;
    }

    public void setComunita(ArrayList comunita) {
        this.comunita = comunita;

    }

    public ArrayList<Nodo_Degree> getComunita_filtrati() {
        return comunita_filtrati;
    }

    public void setComunita_filtrati(ArrayList<Nodo_Degree> comunita_filtrati) {
        this.comunita_filtrati = comunita_filtrati;
    }



}

