package FindFriends;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Robin
 * qq好友推荐
 * Hadoop好友hello，hello好友是world...依次类推。
 * 那么hadoop和world有共同的好友hello，所以hadoop和world可能具有好友关系，
 * world就是hadoop的推荐好友,hadoop也是world的好友推荐。
 * 计算出qq文件内符合上述条件的推荐好友
 *
 * 好友文件 :friends.txt
 * hadoop hello
 * hdfs world
 * tom cat
 * cat dog
 * hello world
 * hello hdfs
 */
public class FindFriends {
    /*
     *  Mapping结果：
     * ﻿ hadoop hello
     *  hello hadoop
        hdfs world
        world hdfs
        tom cat
        cat tom
        cat dog
        dog cat
        hello world
        world hello
        hello hdfs
        hdfs hello
     */
    public static class FindFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String array[] = line.split("\\s+");
            context.write(new Text(array[0]), new Text(array[1]));
            context.write(new Text(array[1]), new Text(array[0]));
        }
    }

    /*
     *maping之后reduce之前，Shuffling进行洗牌，把相同key的整理在一起（默认的shuffling），结果如下：
     *
     * 洗牌结果(输入到reduce)：
     *hadoop hello
     *
     *hello hadoop
     *hello world
     *hello hdfs
     *
     *hdfs world
     *hdfs hello
     *
     *tom cat
     *
     *cat tom
     *cat dog
     *
     *dog cat
     */

    /**tom dog
     dog tom
     hello   world
     world   hello
     hdfs    world
     hdfs    ﻿hadoop
     world   hdfs
     world   ﻿hadoop
     ﻿hadoop hdfs
     ﻿hadoop world
     hello   hdfs
     hdfs    hello  */

    // Reducing进行笛卡尔乘积计算
    public static class FindFriendsReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // 去重
            Set<String> set = new HashSet<String>();
            for (Text v : values) {
                set.add(v.toString());
            }

            if (set.size() > 1) {
                for (Iterator<String> i = set.iterator(); i.hasNext();) {
                    String qqName = i.next();
                    for (Iterator<String> j = set.iterator(); j.hasNext();) {
                        String otherQqName = j.next();
                        if (!qqName.equals(otherQqName)) {
                            context.write(new Text(qqName), new Text(otherQqName));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        final String INPUT_PATH = "friends.txt";
        final String OUTPUT_PATH = "outfriends";
        /**
         * Configuration：map/reduce的j配置类，向hadoop框架描述map-reduce执行的工作
         */
        Configuration conf = new Configuration();

        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if(fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "FindFriends"); //设置一个用户定义的job名称
        job.setJarByClass(FindFriends.class);
        job.setMapperClass(FindFriendsMapper.class); //为job设置Mapper类
//      job.setCombinerClass(IntSumReducer.class);    //为job设置Combiner类
        job.setReducerClass(FindFriendsReduce.class); //为job设置Reducer类
        job.setOutputKeyClass(Text.class);        //为job的输出数据设置Key类
        job.setOutputValueClass(Text.class);    //为job输出设置value类

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        System.exit(job.waitForCompletion(true) ?0 : 1);        //运行job
    }

}
