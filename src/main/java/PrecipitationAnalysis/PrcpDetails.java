package PrecipitationAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PrcpDetails implements Writable {

    private IntWritable prcp;
    private Text date;

    public PrcpDetails(){
        prcp= new IntWritable();
        date= new Text();
    }

    public PrcpDetails(int prcp, String date) {
        this.prcp = new IntWritable(prcp);
        this.date = new Text(date);
    }

    public PrcpDetails(IntWritable prcp, Text date) {
        this.prcp = prcp;
        this.date = date;
    }

    public IntWritable getPrcp() {
        return prcp;
    }

    public void setPrcp(IntWritable prcp) {
        this.prcp = prcp;
    }

    public Text getDate() {
        return date;
    }

    public void setDate(Text date) {
        this.date = date;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        prcp.write(dataOutput);
        date.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        prcp.readFields(dataInput);
        date.readFields(dataInput);
    }

    @Override
    public String toString() {
        return prcp + " " + date;
    }
}