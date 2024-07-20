package TemperatureAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SAMainDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length !=6) {
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.set("station1", args[2]);
        conf.set("station2", args[3]);
        conf.set("aggregation", args[4]);

        Job job = Job.getInstance(conf, "Station analysis");
        job.setJarByClass(SAMainDriver.class);
        job.setMapperClass(StationAnalyserMapper.class);
        job.setReducerClass(StationAnalyserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TempDetails.class);

        FileInputFormat.addInputPath(job, new Path(args[0]+"/"+args[2]+"_"+args[4]+"/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path(args[0]+"/"+args[3]+"_"+args[4]+"/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+args[2]+"v"+args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
