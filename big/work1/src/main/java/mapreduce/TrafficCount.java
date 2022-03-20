package mapreduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


/**
 * @author Jonas Gao
 * @since 2022/3/12
 */
public class TrafficCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Traffic> {
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Traffic> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String[] strings = line.split("\t");
            String number = strings[1];
            String uploadStr = strings[8];
            String downloadStr = strings[9];
            int upstream = Integer.parseInt(uploadStr);
            int downstream = Integer.parseInt(downloadStr);
            outputCollector.collect(new Text(number), new Traffic(upstream, downstream));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Traffic, Text, Traffic> {
        @Override
        public void reduce(Text key, Iterator<Traffic> values, OutputCollector<Text, Traffic> output, Reporter reporter) throws IOException {
            int upstream = 0;
            int downstream = 0;
            while (values.hasNext()) {
                Traffic traffic = values.next();
                upstream += traffic.getUpstream();
                downstream += traffic.getDownstream();
            }
            output.collect(key, new Traffic(upstream, downstream));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(TrafficCount.class);
        conf.setJobName("lijiangyong");
        conf.setMapperClass(Map.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Traffic.class);

        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("/user/lijiangyong/HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(conf, new Path("/user/lijiangyong"));

        JobClient.runJob(conf);
    }
}
