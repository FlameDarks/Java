package com.zzuli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class day8 {
    class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
            String s = value.toString();
            byte[] bytes = s.getBytes();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            InputStreamReader reader = new InputStreamReader(inputStream);
            IKSegmenter ik = new IKSegmenter(reader,true);
            Lexeme txt = null;
            while ((txt = ik.next())!=null){
                context.write(new Text(txt.getLexemeText()),new IntWritable(1));
            }
        }
    }
    class Myreducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        protected void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
            int count = 0;
            for (IntWritable x:values){
                count=count+x.get();
            }
            context.write(key,new IntWritable(count));
        }
    }
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(Myreducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.setInputPaths(job,"input");
            FileOutputFormat.setOutputPath(job,new Path("out"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
