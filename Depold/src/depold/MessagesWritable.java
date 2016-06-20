package depold;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;


/**
 * Created by mariapia on 05/06/16.
 */
public class MessagesWritable implements Writable{
    LongArrayList vicini;
    String active; //per mandare un messaggio e segnalare che non sono più attivo
    Long ID;
    LongArrayList eliminati;
    Long group_id;
    ArrayList<Nodo_Degree> elementi_comunita;

    public MessagesWritable(){
        vicini = new LongArrayList();
        active = "true";
        eliminati = new LongArrayList();
        group_id = new Long(0);
        elementi_comunita = new ArrayList<>();
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
        int dim = eliminati == null ? 0:eliminati.size();
        dataOutput.writeInt(dim);
        if(dim != 0){
            for (Long v: eliminati){
                dataOutput.writeLong(v);
            }
        }

        dataOutput.writeLong(group_id);

        int d = elementi_comunita == null ? 0 : elementi_comunita.size();
        dataOutput.writeInt(d);
        if (d != 0){
            for (Nodo_Degree s : elementi_comunita){
                s.write(dataOutput);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        vicini.clear();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                addVicini(dataInput.readLong());
            }
        }
        active = WritableUtils.readString(dataInput);
        ID = dataInput.readLong();

        int dim = dataInput.readInt();
        eliminati.clear();
        if (dim != 0){
            for (int i=0; i<dim ; i++){
                addEliminati(dataInput.readLong());
            }
        }
        group_id = dataInput.readLong();

        int d = dataInput.readInt();
        elementi_comunita.clear();
        if (d != 0 ){
            for (int i=0; i<d; i++){
                Nodo_Degree tmp = new Nodo_Degree(dataInput.readLong(),dataInput.readLong());
                addElementiComunita(tmp);
            }
        }
    }

    public void addVicini(long value){
        vicini.add(value);
    }

    public void addEliminati(long value){
        eliminati.add(value);
    }

    public void addElementiComunita(Nodo_Degree s ) { elementi_comunita.add(s);}
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


    public LongArrayList getEliminati() {
        return eliminati;
    }

    public void setEliminati(LongArrayList eliminati) {
        this.eliminati = eliminati;
    }


    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }


    public ArrayList<Nodo_Degree> getElementi_comunita() {
        return elementi_comunita;
    }

    public void setElementi_comunita(ArrayList<Nodo_Degree> elementi_comunita) {
        this.elementi_comunita = elementi_comunita;
    }



}
