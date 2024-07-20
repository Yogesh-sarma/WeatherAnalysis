package ClimateChange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CCMainDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length !=6) {
            System.exit(0);
        }
        Configuration conf = new Configuration();
        conf.set("station", args[2]);
        conf.set("aggregationUnit", args[3]);

        Job job = Job.getInstance(conf, "Temperature analysis");
        job.setJarByClass(CCMainDriver.class);
        job.setMapperClass(ClimateChangeMapper.class);
        job.setReducerClass(ClimateChangeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TempDetails.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+args[2]+"_CC_"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
