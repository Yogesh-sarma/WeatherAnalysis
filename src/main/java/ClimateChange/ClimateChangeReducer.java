package ClimateChange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
public class ClimateChangeReducer extends  Reducer<Text, TempDetails, Text, TempDetails> {
    public void reduce(Text key, Iterable<TempDetails> values, Context context) throws IOException,
    InterruptedException {

        int maxCount=0, minCount=0, avgCount=0;
        int sumPrcp = 0;
        float sumMaxTemp=0, sumMinTemp=0, sumAvgTemp=0, avgMaxTemp, avgMinTemp, avgAvgTemp;
        int prevAgg=-1;
        int currAgg=-1;
        Date date = new Date();
        Configuration conf = context.getConfiguration();
        String aggregationUnit = conf.get("aggregationUnit");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        for (TempDetails tempDetail : values) {
            try {
                date = dateFormat.parse(tempDetail.getDate().toString());
            } catch (ParseException e) {
                System.err.println("parse exception in reducer");
            }
            if(aggregationUnit.equalsIgnoreCase("month")){
                currAgg = date.getMonth();
                currAgg++;
            }
            if(aggregationUnit.equalsIgnoreCase("year")){
                currAgg = date.getYear();
                currAgg+=1900;
            }
            System.out.println("curragg - " + currAgg);
            if(prevAgg ==-1){
                prevAgg=currAgg;
            }

            if(currAgg != prevAgg){
                System.out.println(sumMaxTemp+", count- "+maxCount);
                avgMaxTemp = sumMaxTemp /maxCount;
                avgMinTemp = sumMinTemp /minCount;
                avgAvgTemp = sumAvgTemp /avgCount;

                TempDetails result = new TempDetails(new FloatWritable(avgMaxTemp),
                new FloatWritable(avgMinTemp), new FloatWritable(avgAvgTemp), new Text(""+prevAgg), new IntWritable(sumPrcp));

                context.write(new Text(key.toString()+" "+(prevAgg)), result);

                prevAgg = currAgg;
                sumMaxTemp=0; sumMinTemp=0; sumAvgTemp=0; maxCount = 0; minCount = 0; avgCount=0;
            }


            float currMax = tempDetail.getMax().get();
            float currMin = tempDetail.getMin().get();
            float currAvg = tempDetail.getAvg().get();
            sumPrcp += tempDetail.getPrcp().get();

            if(currMax!=0.0f){
                maxCount++;
            }
            if(currMin!=0.0f){
                minCount++;
            }
            if(currAvg!=0.0f){
                avgCount++;
            }

            sumMaxTemp += currMax;
            sumMinTemp += currMin;
            sumAvgTemp += currAvg;
        }
        avgMaxTemp = sumMaxTemp /maxCount;
        avgMinTemp = sumMinTemp /minCount;
        avgAvgTemp = sumAvgTemp /avgCount;

        TempDetails result = new TempDetails(new FloatWritable(avgMaxTemp),
        new FloatWritable(avgMinTemp), new FloatWritable(avgAvgTemp), new Text(""+prevAgg), new IntWritable(sumPrcp));

        context.write(new Text(key.toString()+" "+prevAgg), result);
    }
}