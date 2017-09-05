package TopK;
//

/**
 * Created with IntelliJ IDEA.
 * User: Isaac Li
 * Date: 12/4/12
 * Time: 5:48 PM
 * To change this template use File | Settings | File Templates.
 */

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.conf.Configured;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.NullWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        import org.apache.hadoop.util.Tool;
        import org.apache.hadoop.util.ToolRunner;

        import java.io.IOException;
        import java.util.TreeMap;

//利用MapReduce求最大值海量数据中的K个数
public class Top_k_new extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, NullWritable, Text> {
        public static final int K = 1;
        private TreeMap<Integer, Text> fatcats = new TreeMap<Integer, Text>();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] str = value.toString().split(",", -2);
            int temp = Integer.parseInt(str[8]);
            fatcats.put(temp, value);
            if (fatcats.size() > K)
                fatcats.remove(fatcats.firstKey());
        }
        @Override
        protected void cleanup(Context context) throws IOException,  InterruptedException {
            for(Text text: fatcats.values()){
                context.write(NullWritable.get(), text);
            }
        }
    }

    public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
        public static final int K = 1;
        private TreeMap<Integer, Text> fatcats = new TreeMap<Integer, Text>();
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                String v[] = val.toString().split("\t");
                Integer weight = Integer.parseInt(v[1]);
                fatcats.put(weight, val);
                if (fatcats.size() > K)
                    fatcats.remove(fatcats.firstKey());
            }
            for (Text text: fatcats.values())
                context.write(NullWritable.get(), text);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "TopKNum");
        job.setJarByClass(Top_k_new.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapClass.class);
        // job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Top_k_new(), args);
        System.exit(res);
    }

}
