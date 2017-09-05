package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexTest {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException{
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }

        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            int splitIndex = key.toString().indexOf(":");
            info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            String fileList = new String();
            for( Text val : values){
                fileList += val.toString()+";";
            }
            result.set(fileList);
            context.write(key, result);
        }
    }

    public static void main(String[] args){
        try{
            Configuration conf = new Configuration();

            String[] otherArgs = new GenericOptionsParser(conf, args)
                    .getRemainingArgs();
            if(otherArgs.length != 2){
                System.err.println("Usage: InvertedIndexTest <in> <out>");
                System.exit(2);
            }


            Job job = Job.getInstance(conf, "InvertedIndexTest");
            job.setJarByClass(InvertedIndexTest.class);

            job.setMapperClass(InvertedIndexMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setReducerClass(InvertedIndexReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path path = new Path(otherArgs[1]);
            FileSystem fileSystem = path.getFileSystem(conf);
            if(fileSystem.exists(path)){
                fileSystem.delete(path, true);//delete even out not empty
            }

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        }catch (IllegalStateException e){
            e.printStackTrace();
        }catch (IllegalArgumentException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}

