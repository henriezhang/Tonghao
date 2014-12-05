
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class TonghaoYueduRes {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 6) {
            System.err.println("Usage: hadoop jar TonghaoYueduRes.jar <in> <out> <queue> <reduce num> <topn> <view num>");
            System.exit(6);
        }

        conf.set("mapred.max.map.failures.percent", "1");
        conf.set("mapred.job.queue.name", otherArgs[2]);
        conf.set("mapred.queue.name", otherArgs[2]);
        conf.set("mapred.child.java.opts", "-Xmx2700m");
        conf.set("topn", otherArgs[4]);
        conf.set("viewn", otherArgs[5]);
        Job job = new Job(conf, "TonghaoYuedu.step_2");
        job.setJarByClass(TonghaoYueduRes.class);
        job.setMapperClass(YueduMapper.class);
        job.setReducerClass(YueduReducer.class);
        job.setNumReduceTasks(Integer.valueOf(otherArgs[3]).intValue());

        // the map output is IntWriteable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is IntWriteable, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (otherArgs.length == 7) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[6]));
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class YueduMapper
            extends Mapper<Object, Text, Text, Text> {
        private Map mapTop3;
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mapTop3 = new HashMap();
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                FSDataInputStream in = fs.open(new Path("tonghaotop3/top3.ini"));
                BufferedReader bufread = new BufferedReader(new InputStreamReader(in));
                String line;
                String[] list;
                while ((line = bufread.readLine()) != null) {
                    list = line.split("\\|");
                    if (list.length < 2) {
                        continue;
                    }
                    mapTop3.put(list[0], list[1]);
                }
                in.close();
            } catch (Exception e) {
                System.out.println("open fail");
            }
        }

        @Override
        public void map(Object key, Text inValue, Context context
        ) throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\\t", 3);
            if (fields.length != 2) {
                return;
            }

            Object url = mapTop3.get(fields[1]);
            if (url == null) {
                context.write(new Text(fields[0]), new Text(fields[1]));
            }
        }
    }

    public static class YueduReducer
            extends Reducer<Text, Text, Text, Text> {
        private HashMap mapUrl;

        @Override
        public void reduce(Text id, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            mapUrl = new HashMap();
            int pv = 0;
            for (Text val : values) {
                String url = val.toString();
                if (url.matches("[0-9]*")) { // url 对应的pv
                    pv = Integer.parseInt(url, 10);
                    continue;
                }
                if (!mapUrl.containsKey(url)) {
                    mapUrl.put(url, 1);
                } else {
                    Integer sum = (Integer) mapUrl.get(url);
                    sum++;
                    mapUrl.put(url, sum);
                }
            }

            ArrayList<Entry<String, Integer>> arrayList = new ArrayList<Entry<String, Integer>>(mapUrl.entrySet());
            Collections.sort(arrayList, new Comparator<Entry<String, Integer>>() {
                public int compare(Entry<String, Integer> e1,
                                   Entry<String, Integer> e2) {
                    return (e2.getValue()).compareTo(e1.getValue());
                }
            });

            int topn = Integer.parseInt(context.getConfiguration().get("topn"));
            int viewn = Integer.parseInt(context.getConfiguration().get("viewn"));
            for (Entry<String, Integer> entry : arrayList) {
                if (topn == 0 || entry.getValue() < viewn) {
                    break;
                }
                topn--;
                context.write(new Text(id + "|" + pv), new Text(entry.getKey() + "|" + entry.getValue()));
            }
        }
    }
}