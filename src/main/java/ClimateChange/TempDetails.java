package ClimateChange;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TempDetails implements Writable {

    private FloatWritable max;
    private FloatWritable min;
    private FloatWritable avg;
    private IntWritable prcp;
    private Text date;

    public TempDetails(){
        max= new FloatWritable();
        min= new FloatWritable();
        avg = new FloatWritable();
        date = new Text();
        prcp = new IntWritable();
    }

    public TempDetails(float max, float min, float avg, String date, int prcp) {
        this.max = new FloatWritable(max);
        this.min = new FloatWritable(min);
        this.avg = new FloatWritable(avg);
        this.date = new Text(date);
        this.prcp=new IntWritable(prcp);
    }

    public TempDetails(FloatWritable max, FloatWritable min, FloatWritable avg, Text date, IntWritable prcp) {
        this.max = max;
        this.min = min;
        this.avg = avg;
        this.date = date;
        this.prcp=prcp;
    }

    public FloatWritable getMax() {
        return max;
    }

    public void setMax(FloatWritable max) {
        this.max = max;
    }

    public FloatWritable getMin() {
        return min;
    }

    public void setMin(FloatWritable min) {
        this.min = min;
    }

    public FloatWritable getAvg() {
        return avg;
    }

    public void setAvg(FloatWritable avg) {
        this.avg = avg;
    }

    public Text getDate() {
        return date;
    }

    public void setDate(Text date) {
        this.date = date;
    }

    public IntWritable getPrcp() {
        return prcp;
    }

    public void setPrcp(IntWritable prcp) {
        this.prcp = prcp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        max.write(dataOutput);
        min.write(dataOutput);
        avg.write(dataOutput);
        date.write(dataOutput);
        prcp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        max.readFields(dataInput);
        min.readFields(dataInput);
        avg.readFields(dataInput);
        date.readFields(dataInput);
        prcp.readFields(dataInput);
    }

    @Override
    public String toString() {
        return max + " " + min + " " + avg + " " + date +" "+prcp;
    }
}
