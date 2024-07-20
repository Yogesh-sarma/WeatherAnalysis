package TemperatureAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class TemperatureAnalysisMapper extends Mapper<Object, Text, Text, TempDetails> {
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
                FloatWritable maxTemp = new FloatWritable(0);
                FloatWritable minTemp = new FloatWritable(0);
                FloatWritable avgTemp = new FloatWritable(0);
                String currDate = split[1];
                try {
                    if(inReqTimePeriod(currDate, beginDate, endDate)){
                        word.set(currStation);
                        if(split[2].equalsIgnoreCase("tmax")){
                            maxTemp = new FloatWritable(Float.parseFloat(split[3]));
                        } else if (split[2].equalsIgnoreCase("tmin")){
                            minTemp = new FloatWritable(Float.parseFloat(split[3]));
                        } else if (split[2].equalsIgnoreCase("tavg")){
                            avgTemp = new FloatWritable(Float.parseFloat(split[3]));
                        }
                        TempDetails tempDetails = new TempDetails(maxTemp, minTemp, avgTemp, new Text(currDate));
                        System.out.println(tempDetails);
                        context.write(word, tempDetails);
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
