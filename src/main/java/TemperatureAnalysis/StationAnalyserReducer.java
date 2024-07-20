package TemperatureAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class StationAnalyserReducer extends Reducer<Text, TempDetails, Text, TempDetails> {
    public void reduce(Text key, Iterable<TempDetails> values, Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        float firstMax=0, firstMin=0, firstAvg=0;
        Iterator<TempDetails> itr = values.iterator();
        TempDetails temp1= itr.next();
        context.write(key, temp1);
        firstAvg = temp1.getAvg().get();
        firstMax = temp1.getMax().get();
        firstMin = temp1.getMin().get();

        TempDetails temp2 = itr.next();
        context.write(key, temp2);
        float secondAvg = temp2.getAvg().get();
        float secondMax = temp2.getMax().get();
        float secondMin = temp2.getMin().get();

        Text diffKey = new Text(conf.get("station1")+"v"+conf.get("station2"));
        context.write(diffKey, new TempDetails(firstMax-secondMax, firstMin-secondMin, firstAvg-secondAvg, ""));
    }
}
