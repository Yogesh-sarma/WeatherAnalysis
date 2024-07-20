package PartOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FilterAugmentMapper extends Mapper<Object, Text, Text, TempDetails> {
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            if((str.substring(0,2).equalsIgnoreCase("us")
                    || str.substring(0,2).equalsIgnoreCase("ca")
                    || str.substring(0,2).equalsIgnoreCase("mx")) && (str.toLowerCase().contains("prcp")
                    || str.toLowerCase().contains("tmax")
                    || str.toLowerCase().contains("tmin")
                    || str.toLowerCase().contains("tavg"))){
                String[] split = str.split(",");
                word.set(split[0]);
                TempDetails tempDetails = new TempDetails(split[1], split[2], split[3]);
                context.write(word, tempDetails);
            }
        }
    }
}
