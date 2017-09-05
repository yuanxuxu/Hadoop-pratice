package Join;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
    private final static String INPUT_PATH = "inputjoin";
    private final static String OUTPUT_PATH = "outputmapjoin";

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, EmpJoinDep>{
        private EmpJoinDep empJoinDep = new EmpJoinDep();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");
            if(values.length==4){
                empJoinDep.setName(values[0]);
                empJoinDep.setSex(values[1]);
                empJoinDep.setAge(Integer.parseInt(values[2]));
                empJoinDep.setDepNo(Integer.parseInt(values[3]));
                empJoinDep.setTable("EMP");
                context.write(new IntWritable(Integer.parseInt(values[3])), empJoinDep);
            }

            if(values.length==2){
                empJoinDep.setDepNo(Integer.parseInt(values[0]));
                empJoinDep.setDepName(values[1]);
                empJoinDep.setTable("DEP");
                context.write(new IntWritable(Integer.parseInt(values[0])), empJoinDep);
            }
        }
    }


    public static class MyReducer extends Reducer<IntWritable, EmpJoinDep, NullWritable, EmpJoinDep>{

        @Override
        protected void reduce(IntWritable key, Iterable<EmpJoinDep> values,
                              Context context)
                throws IOException, InterruptedException {
            String depName = "";
            List<EmpJoinDep> list = new LinkedList<EmpJoinDep>();
            //1  emp
            //1  dep
            for (EmpJoinDep val : values) {
                list.add(new EmpJoinDep(val));
                //如果是部门表，如果部门编号为1，则获取该部门的名字。
                if(val.getTable().equals("DEP")){
                    depName = val.getDepName();
                }
            }
            //如果上面部门编号是1，则这里也是1。
            for (EmpJoinDep listjoin : list) {
                //如果是员工表，则需要设置员工的所属部门。
                if(listjoin.getTable().equals("EMP")){
                    listjoin.setDepName(depName);
                    context.write(NullWritable.get(), listjoin);
                }

            }

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),conf);
        if(fileSystem.exists(new Path(OUTPUT_PATH)))
        {
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job = Job.getInstance(conf, "Reduce Join");

        job.setJarByClass(ReduceJoin.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(EmpJoinDep.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(EmpJoinDep.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}