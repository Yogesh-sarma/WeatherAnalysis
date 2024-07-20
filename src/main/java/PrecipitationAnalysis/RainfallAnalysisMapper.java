package PrecipitationAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RainfallAnalysisMapper extends Mapper<Object, Text, Text, PrcpDetails> {

    public void map(Object key, Text value, Context context) throws
            IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String[] values = value.toString().split("\\s+");
        String station = values[0];
        String date = values[1];
        int totalPrcp = Integer.parseInt(values[2]);
        context.write(new Text(station+" "+date), new PrcpDetails(totalPrcp, ""));
    }
}
