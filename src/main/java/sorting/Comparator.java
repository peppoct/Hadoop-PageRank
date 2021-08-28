package sorting;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Comparator implements WritableComparable<Comparator> {

    private String title;
    private double rank;

    public Comparator(){}

    public void setTitle(String title){
        this.title = title;
    }

    public void setRank(double rank){
        this.rank = rank;
    }

    public String getTitle(){
        return this.title;
    }

    public double getRank(){
        return this.rank;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.title.toString());
        out.writeDouble(this.rank);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.title = in.readUTF();
        this.rank = in.readDouble();
    }


    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Comparator)) {
            return false;
        }

        Comparator c = (Comparator) o;
        return c.getTitle().equals(this.title)
                && this.rank == c.getRank();
    }

    @Override
    public String toString() {
        return title + String.valueOf(rank) ;
    }

    @Override
    public int hashCode() { return this.title.hashCode(); }

    @Override
    public int compareTo(Comparator c) {
        double rank = c.getRank();
        String title = c.getTitle();
        return this.rank < rank ? 1 : (this.rank == rank ? this.title.compareTo(title)  : -1);
    }
}


