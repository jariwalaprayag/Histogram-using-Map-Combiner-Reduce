import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.*;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;


/* single color intensity */
class Color implements WritableComparable <Color>{
    public int type;       /* red=1, green=2, blue=3 */
    public int intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */
    Color(int t, int i){
        this.type = t;
        this.intensity = i;
    }
    Color()
    {

    }
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(this.type);
        out.writeInt(this.intensity);
    }

    public void readFields ( DataInput in ) throws IOException {
        type = in.readInt();
        intensity = in.readInt();
    }

    public String toString () { return this.type+" "+this.intensity; }

    public int compareTo(Color key)
        {
        int thiscolor = this.type;
        int thisintensity = this.intensity;
        int thatcolor = key.type;
        int thatintensity = key.intensity;
        if (thiscolor == thatcolor){
                if(thisintensity > thatintensity)
                    return 1;
                else if(thisintensity == thatintensity)
                    return 0;
                else
                    return -1;
        }
        else{
            if(thiscolor > thatcolor)
                return 1;
            else{
                return -1;
            }
        }
    }

    public int hashcode(){
    	return this.type*4000+this.intensity;
    } 
    public boolean equals(Object a) {
        if(a == null)
            return false;
        else if(this == a)
            return true;
        else{
            Color that_a = (Color)a; 
            if(this.type == that_a.type)
            {
                if(this.intensity == that_a.intensity)
                    return true;
                else
                    return false;
            }
            else
                return false;
        }
    }
}

public class Histogram {
    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int red = s.nextInt();
            int green = s.nextInt();
            int blue = s.nextInt();
            context.write(new Color(1,red),new IntWritable(1));
            context.write(new Color(2,green),new IntWritable(1));
            context.write(new Color(3,blue),new IntWritable(1));
            s.close();
        }
    }

public static class HistogramInMapper extends Mapper<Object,Text,Color,IntWritable> {
        public Hashtable<Color, Integer> hash;
        protected void setup ( Context context ) throws IOException,InterruptedException
        {
            hash = new Hashtable<>();
        }
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int red = s.nextInt();
            int green = s.nextInt();
            int blue = s.nextInt();
            Color r= new Color(1,red);
            Color g=new Color(2,green);
            Color b =new Color(3,blue);
            if(hash.containsKey(r))
                hash.put(r,hash.get(r)+1);
            else
                hash.put(r,1);
            if(hash.containsKey(g))
                hash.put(g,hash.get(g)+1);
            else
                hash.put(g,1);
            if(hash.containsKey(b))
                hash.put(b,hash.get(b)+1);
            else
                hash.put(b,1);
            s.close();
        }
        protected void cleanup ( Context context ) throws IOException,InterruptedException
        {
            for (Color key : hash.keySet())
            {
                context. write (key,new IntWritable(hash.get(key)));
            }
            hash.clear();
        }
    }

    public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable> {
    @Override
public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
            int sum = 0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            context.write(key,new IntWritable(sum));
        }
}

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
            int sum = 0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            context.write(key,new LongWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
        Job joba = Job.getInstance();
        joba.setJobName("MyJoba");
        joba.setJarByClass(Histogram.class);
        joba.setOutputKeyClass(Color.class);
        joba.setOutputValueClass(IntWritable.class);
        joba.setMapOutputKeyClass(Color.class);
        joba.setMapOutputValueClass(IntWritable.class);
        joba.setMapperClass(HistogramMapper.class);
        joba.setCombinerClass(HistogramCombiner.class);
        joba.setReducerClass(HistogramReducer.class);
        joba.setInputFormatClass(TextInputFormat.class);
        joba.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(joba,new Path(args[0]));
        FileOutputFormat.setOutputPath(joba,new Path(args[1]));
        joba.waitForCompletion(true);

        Job jobb = Job.getInstance();
        jobb.setJobName("MyJobb");
        jobb.setJarByClass(Histogram.class);
        jobb.setOutputKeyClass(Color.class);
        jobb.setOutputValueClass(IntWritable.class);
        jobb.setMapOutputKeyClass(Color.class);
        jobb.setMapOutputValueClass(IntWritable.class);
        jobb.setMapperClass(HistogramInMapper.class);
        jobb.setReducerClass(HistogramReducer.class);
        jobb.setInputFormatClass(TextInputFormat.class);
        jobb.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobb,new Path(args[0]));
        FileOutputFormat.setOutputPath(jobb,new Path(args[1]+"2"));
        jobb.waitForCompletion(true);
   }
}