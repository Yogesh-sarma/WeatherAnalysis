package PartOne;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocationWritable implements Writable {

    private Text location;
    private Text state;
    private Text country;

    public LocationWritable() {
        this.location = new Text();
        this.state = new Text();
        this.country = new Text();
    }

    public LocationWritable(Text location, Text state, Text country) {
        this.location = location;
        this.state = state;
        this.country = country;
    }

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public Text getState() {
        return state;
    }

    public void setState(Text state) {
        this.state = state;
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country = country;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        location.write(dataOutput);
        state.write(dataOutput);
        country.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        location.readFields(dataInput);
        state.readFields(dataInput);
        country.readFields(dataInput);
    }

    @Override
    public String toString() {
        return location +
                ", " + state +
                ", " + country;
    }
}
