
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Date;
import java.util.*;
import java.text.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TonghaoYueduTop {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 5) {
            System.err.println("Usage: hadoop jar TonghaoYueduTop.jar <in> <out> <queue> <reduce num> <start date>");
            System.exit(5);
        }

        conf.set("mapred.max.map.failures.percent", "1");
        conf.set("mapred.job.queue.name", otherArgs[2]);
        conf.set("mapred.queue.name", otherArgs[2]);
        Job job = new Job(conf, "TonghaoYuedu.top3");
        job.setJarByClass(TonghaoYueduTop.class);
        job.setMapperClass(YueduMapper.class);
        job.setCombinerClass(YueduCombiner.class);
        job.setReducerClass(YueduReducer.class);
        job.setNumReduceTasks(Integer.valueOf(otherArgs[3]).intValue());

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // the reduce output is IntWritable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        ParsePosition pos = new ParsePosition(0);
        Date dt = formatter.parse(otherArgs[4], pos);
        Calendar cd = Calendar.getInstance();
        cd.setTime(dt);
        FileSystem fs = FileSystem.get(conf);
        for (int i = 0; i < 2; i++) {
            String tmpPath = otherArgs[0] + "/ds=" + formatter.format(cd.getTime());
            Path tPath = new Path(tmpPath);
            if (fs.exists(tPath)) {
                FileInputFormat.addInputPath(job, tPath);
                System.out.println("Exist " + tmpPath);
            } else {
                System.out.println("Not exist " + tmpPath);
            }
            cd.add(Calendar.DATE, -1);
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class YueduMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text urlValue = new Text("");
        private Pattern p;
        private Matcher m;

        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            p = Pattern.compile("^/a/201\\d[^_]+?\\.htm$");
        }

        @Override
        public void map(Object key, Text inValue, Context context
        ) throws IOException, InterruptedException {
            String[] fields = inValue.toString().split(",", 19);
            if (fields.length < 18) {
                return;
            }

            m = p.matcher(fields[5]);
            if (m.find() && fields[17].length() >= 5) {
                urlValue.set(fields[4] + fields[5]);
                context.write(urlValue, new IntWritable(1));
            }
        }
    }

    public static class YueduCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text url, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(url, new IntWritable(sum));
        }
    }

    public static class YueduReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text url, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > 10000) {
                context.write(url, new IntWritable(sum));
            }
        }
    }
}
