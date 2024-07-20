package PrecipitationAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RainfallMainDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length !=6) {
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.set("station", args[2]);
        conf.set("aggregationUnit", args[3]);
        conf.set("threshold", args[4]);

        Job job = Job.getInstance(conf, "Precipitation analysis");
        job.setJarByClass(RainfallMainDriver.class);
        job.setMapperClass(RainfallAnalysisMapper.class);
        job.setReducerClass(RainfallAnalysisReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PrcpDetails.class);

        FileInputFormat.addInputPath(job, new Path(args[0]+"/"+args[2]+"_PA_"+args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+args[2]+"_"+args[3]+"_rainfall"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
