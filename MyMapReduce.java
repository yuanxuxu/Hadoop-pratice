package SingleTableJoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyMapReduce {

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String child = value.toString().split(" ")[0].replaceAll("\\s+", "");
            System.out.println(child);
            String parent = value.toString().split(" ")[1].replaceAll("\\s+", "");
            System.out.println(parent);
            //产生正序与逆序的key-value同时压入context

            context.write(new Text(parent), new Text("+" + child));
            context.write(new Text(child), new Text("-" + parent));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Text> grandparent = new ArrayList<Text>();
            ArrayList<Text> grandchild = new ArrayList<Text>();
            for (Text t : values) {//对各个values中的值进行处理
                String s = t.toString();
                System.out.println(key + " " + s);
                if (s.startsWith("-")) {
                    grandparent.add(new Text(s.substring(1)));
                } else {
                    grandchild.add(new Text(s.substring(1)));
                }
            }
            //再将grandparent与grandchild中的东西，一一对应输出。
            for (int i = 0; i < grandchild.size(); i++) {
                for (int j = 0; j < grandparent.size(); j++) {
                    context.write(grandchild.get(i), grandparent.get(j));
                }
            }
            System.out.println("-------------------------");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 判断output文件夹是否存在，如果存在则删除
        Path path = new Path(otherArgs[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
