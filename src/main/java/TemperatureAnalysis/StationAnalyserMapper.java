package TemperatureAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class StationAnalyserMapper extends Mapper<Object, Text, Text, TempDetails>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String[] values = value.toString().split("\\s+");
        String station = values[0];
        String date = values[1];
        float max = Float.parseFloat(values[2]);
        float min = Float.parseFloat(values[3]);
        float avg = Float.parseFloat(values[4]);
        TempDetails tempDetails = new TempDetails(max, min, avg, station);

        System.out.println("key-"+key.toString());
        context.write(new Text(date), tempDetails);
    }
}
