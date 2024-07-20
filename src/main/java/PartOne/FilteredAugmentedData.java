package PartOne;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FilteredAugmentedData implements Writable {

    private Text date;
    private FloatWritable tmax;
    private FloatWritable tmin;
    private FloatWritable tavg;
    private IntWritable prcp;
    private LocationWritable location;

    public FilteredAugmentedData() {
        this.date = new Text();
        this.tmax = new FloatWritable(-999);
        this.tmin = new FloatWritable(-999);
        this.tavg = new FloatWritable(-999);
        this.location = new LocationWritable();
        this.prcp=new IntWritable(-999);
    }

    public FilteredAugmentedData(String date, Float tmax, Float tmin, Float tavg, LocationWritable location, Integer prcp) {
        this.date = new Text(date);
        this.tmax = new FloatWritable(tmax);
        this.tmin = new FloatWritable(tmin);
        this.tavg = new FloatWritable(tavg);
        this.location = location;
        this.prcp = new IntWritable(prcp);
    }

    public FilteredAugmentedData(Text date, FloatWritable tmax, FloatWritable tmin, FloatWritable tavg, LocationWritable location, IntWritable prcp) {
        this.date = date;
        this.tmax = tmax;
        this.tmin = tmin;
        this.tavg = tavg;
        this.location = location;
        this.prcp = prcp;
    }

    public Text getDate() {
        return date;
    }

    public void setDate(Text date) {
        this.date = date;
    }

    public FloatWritable getTmax() {
        return tmax;
    }

    public void setTmax(FloatWritable tmax) {
        this.tmax = tmax;
    }

    public FloatWritable getTmin() {
        return tmin;
    }

    public void setTmin(FloatWritable tmin) {
        this.tmin = tmin;
    }

    public FloatWritable getTavg() {
        return tavg;
    }

    public void setTavg(FloatWritable tavg) {
        this.tavg = tavg;
    }

    public LocationWritable getLocation() {
        return location;
    }

    public void setLocation(LocationWritable location) {
        this.location = location;
    }

    public IntWritable getPrcp() {
        return prcp;
    }

    public void setPrcp(IntWritable prcp) {
        this.prcp = prcp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        date.write(dataOutput);
        tmax.write(dataOutput);
        tmin.write(dataOutput);
        tavg.write(dataOutput);
        location.write(dataOutput);
        prcp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        date.readFields(dataInput);
        tmax.readFields(dataInput);
        tmin.readFields(dataInput);
        tavg.readFields(dataInput);
        location.readFields(dataInput);
        prcp.readFields(dataInput);
    }

    @Override
    public String toString() {
        return date +
                "," + tmax +
                "," + tmin +
                "," + tavg +
                "," + prcp +
                "," + location;
    }
}
