import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
public class ULDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ECommerceMapReduce");

        job.setJarByClass(ULDriver.class);
        job.setMapperClass(ULMapper.class);
        job.setReducerClass(ULReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    public static class ULMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                // Assuming your dataset is CSV and columns are separated by commas
                String[] columns = value.toString().split(",");

                // Extract relevant columns (modify as per your dataset)
                String customerId = columns[0];
                String gender = columns[1];
                String age = columns[2];
                String city = columns[3];
                String totalSpend = columns[5];
                String itemsPurchased = columns[6];
                String satisfactionLevel = columns[10];

                // Emit key-value pairs (customerId as key, selected columns as value)
                context.write(new Text(customerId),
                        new Text(gender + "," + age + "," + city + "," + totalSpend + "," + itemsPurchased + "," + satisfactionLevel));
            } catch (Exception e) {
                // Handle exceptions, e.g., malformed lines
            }
        }
    }

    public static class ULReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Since we're only selecting columns, the reducer can be a pass-through
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
