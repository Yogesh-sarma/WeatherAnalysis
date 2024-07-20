package PrecipitationAnalysis;

import TemperatureAnalysis.TempDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class PrecipitationAnalyserMapper extends Mapper<Object, Text, Text, PrcpDetails> {
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws
            IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String reqStation = conf.get("station");

        String beginDate = conf.get("beginDate");
        String endDate = conf.get("endDate");

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            String[] split = str.split(",");
            String currStation = split[0];
            if(currStation.equalsIgnoreCase(reqStation)){
                IntWritable prcp = new IntWritable();
                String currDate = split[1];
                try {
                    if(inReqTimePeriod(currDate, beginDate, endDate)){
                        word.set(currStation);
                        if(split[2].equalsIgnoreCase("prcp")){
                            prcp = new IntWritable(Integer.parseInt(split[3]));
                        }
                        System.out.println(prcp);
                        PrcpDetails prcpDetails = new PrcpDetails(prcp, new Text(currDate));
                        context.write(word, prcpDetails);
                    }
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private boolean inReqTimePeriod(String currDate, String beginDate, String endDate) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Date curr = dateFormat.parse(currDate);
        Date begin = dateFormat.parse(beginDate);
        Date end = dateFormat.parse(endDate);

        return curr.compareTo(begin) >=0 && curr.compareTo(end) <=0;
    }
}
