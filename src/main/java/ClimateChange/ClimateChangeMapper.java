package ClimateChange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ClimateChangeMapper extends Mapper<Object, Text, Text, TempDetails> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            String[] split = str.split(",");
            String currStation = split[0];
            FloatWritable maxTemp = new FloatWritable(0);
            FloatWritable minTemp = new FloatWritable(0);
            FloatWritable avgTemp = new FloatWritable(0);
            IntWritable prcp = new IntWritable(0);
            String currDate = split[1];
            if (currStation.substring(0, 2).equalsIgnoreCase("us") || currStation.substring(0, 2).equalsIgnoreCase("ca") || currStation.substring(0, 2).equalsIgnoreCase("mx")) {
                word.set(currStation);
                if (split[2].equalsIgnoreCase("tmax")) {
                    maxTemp = new FloatWritable(Float.parseFloat(split[3]));
                } else if (split[2].equalsIgnoreCase("tmin")) {
                    minTemp = new FloatWritable(Float.parseFloat(split[3]));
                } else if (split[2].equalsIgnoreCase("tavg")) {
                    avgTemp = new FloatWritable(Float.parseFloat(split[3]));
                } else if (split[2].equalsIgnoreCase("prcp")) {
                    prcp = new IntWritable(Integer.parseInt(split[3]));
                }
                TempDetails tempDetails = new TempDetails(maxTemp, minTemp, avgTemp, new Text(currDate), prcp);
                System.out.println(tempDetails);
                context.write(word, tempDetails);
            }
        }
    }
}
