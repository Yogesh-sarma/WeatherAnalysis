package PrecipitationAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PAMainDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length !=6) {
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.set("station", args[2]);
        conf.set("beginDate", args[3]);
        conf.set("endDate", args[4]);
        conf.set("aggregationUnit", args[5]);

        Job job = Job.getInstance(conf, "Precipitation analysis");
        job.setJarByClass(PAMainDriver.class);
        job.setMapperClass(PrecipitationAnalyserMapper.class);
        job.setReducerClass(PrecipitationAnalyserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PrcpDetails.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+args[2]+"_PA_"+args[5]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
