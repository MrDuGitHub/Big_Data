package org.apache.hadoop.examples;

import java.io.*;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans 
{
	public static int K=3;
	public static double[] X=new double[K];
	public static double[] Y=new double[K];
	public static int[] number=new int[K];
	public static boolean done=true;
	public static DecimalFormat df = new DecimalFormat("#.00");
	public static void init(String datafile)
	{
		try (
				FileReader reader = new FileReader(datafile); 
				BufferedReader br = new BufferedReader(reader)
			) 
		{
			String line; 
			for (int i=0;i<K;i++)
			{
				line=br.readLine();
				int pos=line.indexOf(" ");
				X[i]=Double.valueOf(line.substring(0, pos));
				Y[i]=Double.valueOf(line.substring(pos+1,line.length()));
				number[i]=0;
			}
//			while ((line = br.readLine()) != null) 
//			{
//				System.out.println(line); 
//			}
		} 
		catch (IOException e) { e.printStackTrace();}
	}
	
	public static class KMapper extends Mapper<Object, Text, IntWritable, Text>
	{
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
			String line=value.toString();
			int pos=line.indexOf(" ");
			int x=Integer.valueOf(line.substring(0, pos));
			int y=Integer.valueOf(line.substring(pos+1,line.length()));
			double min=80000;
			int index=0;
			for (int i=0;i<K;i++)
			{
				double d=(X[i]-x)*(X[i]-x)+(Y[i]-y)*(Y[i]-y);
				if (d<min) {min=d;index=i;}
			}
			context.write(new IntWritable(index),new Text(String.valueOf(x)+" "+String.valueOf(y)+" "+String.valueOf(1)));
	    }
	}
	
	public static class KCombiner extends Reducer<IntWritable,Text,IntWritable,Text>
	{
	    public void reduce(IntWritable key, Iterable<Text> values, Context context ) throws IOException, InterruptedException 
	    {
	        double x = 0,y=0;
	        int num = 0;
            for (Text val : values)
            {
            	StringTokenizer itr = new StringTokenizer(val.toString());
            	x+=Double.valueOf(itr.nextToken());
            	y+=Double.valueOf(itr.nextToken());
            	num +=Integer.valueOf(itr.nextToken());    
            }  
	        context.write(key,new Text(String.valueOf(x/num)+" "+String.valueOf(y/num)+" "+String.valueOf(num)));
	    }
	}
	
	public static class KReducer extends Reducer<IntWritable,Text,IntWritable,Text>
	{
	   public void reduce(IntWritable key, Iterable<Text> values, Context context ) throws IOException, InterruptedException 
	    {
		    double x = 0,y=0;
	        int num = 0;
            for (Text val : values)
            {
            	StringTokenizer itr = new StringTokenizer(val.toString());
            	double tem_x=Double.valueOf(itr.nextToken());
            	double tem_y=Double.valueOf(itr.nextToken());
            	int tem_num=Integer.valueOf(itr.nextToken());    
            	x+=tem_x*tem_num;
            	y+=tem_y*tem_num;
            	num+=tem_num;
            }  
            int index=key.get();
            if (number[index]!=num) done=false;
        	number[index]=num;	
        	X[index]=Double.valueOf(df.format(x/num));
        	Y[index]=Double.valueOf(df.format(y/num));
            context.write(key,new Text(String.valueOf(X[index])+" "+String.valueOf(Y[index])+" "+String.valueOf(num)));
	    }
	}
	
	public static void main(String[] args) throws Exception 
	{	
		init("/home/hadoop/hadoop/K-Means_python/data.txt");
//		for (int i=0;i<K;i++)
//		{
//			System.out.print(X[i]);
//			System.out.print(" ");
//			System.out.println(Y[i]);
//		}
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) 
	    {    System.err.println("Usage: K-means <in> <out>");
	         System.exit(2);
	    } 
	    int max_num=50;
	    int i=0;
	    while(i<max_num)
	    {
			Job job = Job.getInstance(conf, "K-means");
		    job.setJarByClass(KMeans.class);
		    job.setMapperClass(KMapper.class);
		    job.setCombinerClass(KReducer.class); 
		    job.setReducerClass(KReducer.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+i));
		    job.waitForCompletion(true);
		    if (done) break;
		    i++;
		    done=true;
	    }
	    System.exit(1);
	}
}
