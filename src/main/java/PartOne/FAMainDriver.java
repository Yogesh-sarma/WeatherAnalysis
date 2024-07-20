package PartOne;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class FAMainDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length !=3){
            System.exit(0);
        }
        Configuration conf = new Configuration();
        HashMap<String, LocationWritable> locationDetails= new HashMap<>();
        Gson gson = new Gson();
        conf.set("stationDetails", gson.toJson(locationDetails));
        Job job = Job.getInstance(conf, "Filtering and Augmenting data");

        Path cacheFile = new Path("hdfs://10.0.2.15:9000/stations/ghcnd-stations.txt");
        DistributedCache.addCacheFile(cacheFile.toUri(), conf);

        job.setJarByClass(FAMainDriver.class);
        job.setMapperClass(FilterAugmentMapper.class);
        job.setReducerClass(FilterAugmentReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TempDetails.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FilteredAugmentedData.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
