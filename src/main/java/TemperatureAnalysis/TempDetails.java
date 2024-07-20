package TemperatureAnalysis;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TempDetails implements Writable {

    private FloatWritable max;
    private FloatWritable min;
    private FloatWritable avg;
    private Text date;

    public TempDetails(){
        max= new FloatWritable();
        min= new FloatWritable();
        avg = new FloatWritable();
        date = new Text();
    }

    public TempDetails(float max, float min, float avg, String date) {
        this.max = new FloatWritable(max);
        this.min = new FloatWritable(min);
        this.avg = new FloatWritable(avg);
        this.date = new Text(date);
    }

    public TempDetails(FloatWritable max, FloatWritable min, FloatWritable avg, Text date) {
        this.max = max;
        this.min = min;
        this.avg = avg;
        this.date = date;
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

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        max.write(dataOutput);
        min.write(dataOutput);
        avg.write(dataOutput);
        date.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        max.readFields(dataInput);
        min.readFields(dataInput);
        avg.readFields(dataInput);
        date.readFields(dataInput);
    }

    @Override
    public String toString() {
        return max + " " + min + " " + avg + " " + date;
    }
}