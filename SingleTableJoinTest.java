package SingleTableJoin;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SingleTableJoinTest {

    public static int times = 0;

    public static class Map extends Mapper<Object, Text, Text, Text>{
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            String childName = new String();
            String parentName = new String();

            // value is one line input
            StringTokenizer iter = new StringTokenizer(value.toString());
            String[] values = new String[2];
            int i = 0;
            while (iter.hasMoreTokens()) {
                values[i] = iter.nextToken();
                i++;
            }
            if (i == 2 && values[0].compareTo("child") != 0) {
                childName = values[0];
                parentName = values[1];
                //System.out.println(values[0] + " " + values[1]);
                context.write(new Text(parentName), new Text(childName +"-"+ parentName));
                context.write(new Text(childName), new Text(childName +"-"+ parentName));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            if (times == 0) {
                context.write(new Text("grandChild"), new Text("grandParent"));
                times++;
            }

            Set<String> child = new HashSet<>();
            Set<String> parent = new HashSet<>();

            int i=0;
            for(Text val : values){
                String[] temp = new String[2];
                temp = val.toString().split("-");
                System.out.println(key +"--"+ temp[0]+"--"+temp[1]);
                if(!child.contains(temp[0]))
                    child.add(temp[0]);
                else child.remove(temp[0]);
                if(!parent.contains(temp[1]))
                    parent.add(temp[1]);
                else parent.remove(temp[1]);
                i++;
            }
            System.out.println(i);

            String[] chi = child.toArray(new String[child.size()]);
            String[] par = child.toArray(new String[parent.size()]);

            if(i==4){
            for(String c:chi){
                System.out.println(c);
                for(String p : par)
                {System.out.println(p);
                    context.write(new Text(c), new Text(p));}}}
        }
    }

    //main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Single table join <in>  <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "single table join");
        job.setJarByClass(SingleTableJoinTest.class);
        job.setMapperClass(SingleTableJoinTest.Map.class);
        job.setReducerClass(SingleTableJoinTest.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);


    }

}

