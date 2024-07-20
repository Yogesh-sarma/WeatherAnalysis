package PrecipitationAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class RainfallAnalysisReducer extends Reducer<Text, PrcpDetails, Text, PrcpDetails> {
    public void reduce(Text key, Iterable<PrcpDetails> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String thresholdStr = conf.get("threshold");
        int threshold = Integer.parseInt(thresholdStr);

        Iterator<PrcpDetails> itr = values.iterator();
        PrcpDetails prcpDetail = itr.next();
        String result= prcpDetail.getPrcp().get() >= threshold ? "High rainfall" : "Low Rainfall";
        prcpDetail.setDate(new Text(result));
        context.write(key, prcpDetail);
    }
}