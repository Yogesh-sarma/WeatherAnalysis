package PartOne;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TempDetails implements Writable {

    private Text date;
    private Text element;
    private Text value;

    public TempDetails() {
        this.date = new Text();
        this.element = new Text();
        this.value = new Text();
    }

    public TempDetails(String date, String element, String value) {
        this.date = new Text(date);
        this.element = new Text(element);
        this.value = new Text(value);
    }

    public TempDetails(Text date, Text element, Text value) {
        this.date = date;
        this.element = element;
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        date.write(dataOutput);
        element.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        date.readFields(dataInput);
        element.readFields(dataInput);
        value.readFields(dataInput);
    }

    public Text getDate() {
        return date;
    }

    public void setDate(Text date) {
        this.date = date;
    }

    public Text getElement() {
        return element;
    }

    public void setElement(Text element) {
        this.element = element;
    }

    public Text getValue() {
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return date +
                "," + element +
                "," + value;
    }
}
