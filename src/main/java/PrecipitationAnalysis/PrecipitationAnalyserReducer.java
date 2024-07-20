package PrecipitationAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PrecipitationAnalyserReducer extends Reducer<Text, PrcpDetails, Text, PrcpDetails> {
    public void reduce(Text key, Iterable<PrcpDetails> values, Context context) throws IOException, InterruptedException {
        int sumPrcp = 0;
        int prevAgg = -1;
        int currAgg = -1;
        Date date = new Date();
        Configuration conf = context.getConfiguration();
        String aggregationUnit = conf.get("aggregationUnit");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        for (PrcpDetails tempDetail : values) {
            try {
                date = dateFormat.parse(tempDetail.getDate().toString());
            } catch (ParseException e) {
                System.err.println("parse exception in reducer");
            }
            if (aggregationUnit.equalsIgnoreCase("month")) {
                currAgg = date.getMonth();
                currAgg++;
            }
            if (aggregationUnit.equalsIgnoreCase("year")) {
                currAgg = date.getYear();
                currAgg += 1900;
            }
            System.out.println("curragg - " + currAgg);
            if (prevAgg == -1) {
                prevAgg = currAgg;
            }

            if(currAgg != prevAgg){
                PrcpDetails result = new PrcpDetails(new IntWritable(sumPrcp), new Text(""+prevAgg));

                context.write(new Text(key.toString()+" "+(prevAgg)), result);

                prevAgg = currAgg;
                sumPrcp=0;
            }

            sumPrcp += tempDetail.getPrcp().get();
        }
        PrcpDetails result = new PrcpDetails(new IntWritable(sumPrcp), new Text(""+prevAgg));
        context.write(new Text(key.toString()+" "+(prevAgg)), result);
    }
}
