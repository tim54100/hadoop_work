import java.io.IOException; 
import java.util.*; 
import java.util.StringTokenizer; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

import java.text.DecimalFormat; 
import java.math.RoundingMode;

public class Cleardata {
 
 public static double checknumber(String s)
 {
    double number = 0;
    boolean interger_mode = true;
    int digit = 0;
    int ascii = (int) s.charAt(0) - 48;
    if(ascii<0 || ascii>9)
        return -1;
    for(int i=0;i<s.length();i++)
    {
        ascii = (int) s.charAt(i) - 48;
        if(0<=ascii && ascii<=9 && interger_mode)
        {
            number *= 10;
            number += ascii;
        }
        else if(ascii == -2)
        {
            digit = 1;
            interger_mode = false;
        }
        else if(!interger_mode && 0<=ascii && ascii<=9)
        {
            number += ascii * Math.pow(0.1, digit);
            ++digit;
        }        
            
    }
    return number;
 }
 public static String [] process_string(String s)
 {
    s = s.replaceAll(",,", ",NaN,");
    double [] x;
    x = new double[24];
    String [] s_x;
    s_x = new String[24];
    double [] indexs;
    indexs = new double[24];
    int count = 0;
    int index = 0;
    String [] key_value;
    key_value = new String[2];
    key_value[0] = "";
    key_value[1] = "";
    StringTokenizer st = new StringTokenizer(s, ",");
    while(st.hasMoreElements()){
	    
    	String temp = st.nextToken();
    	if (count<3)
            key_value[0] = key_value[0] + temp + ","; 
	else
	{	
	    if (temp.equals("NaN"))
	    {
                    // System.out.println("Token is null");
                indexs[index] = 1;
                    // x[index] = Double.parseDouble(temp);
            }
            else if (temp.equals("NR"))
                x[index] = 0;
            else
	    {
                    // System.out.println("Token:" + temp);
                    // x[index] = Double.parseDouble(temp);
                x[index] = checknumber(temp);
                if(x[index] == -1)
                    indexs[index] = 1;
            }
            ++index;
        }
    	++count;
    }
        // System.out.println(key_value[0]);
        // System.out.println(x[3]);
    double mean = 0;
    double std = 0;
    count = 0;
    for(int i=0;i<x.length;i++)
    {
        if (indexs[i]==0)
        {
            ++count;
            mean += x[i];
            std += Math.pow(x[i], 2);
        }
    }
    mean /= count;
    std /= count;
    std -= Math.pow(mean, 2);
    std = Math.sqrt(std);
    DecimalFormat df = new DecimalFormat("#.##");
    df.setRoundingMode(RoundingMode.CEILING);
    double lower_bound = mean - 3*std;
    String lower_boung_string = df.format(lower_bound);
    double upper_bound = mean + 3*std;
    String upper_boung_string = df.format(upper_bound);
    String mean_string = df.format(mean);
    String value = "";
    for(int i=0;i<x.length;i++)
    {
        if(indexs[i]==1)
            s_x[i] = mean_string;
        else if(x[i] < lower_bound)
            s_x[i] = lower_boung_string;
        else if(x[i] > upper_bound)
            s_x[i] = upper_boung_string;
        else
            s_x[i] = df.format(x[i]);
        key_value[1] = key_value[1] + s_x[i] + ',' ;
    }
    key_value[1] = key_value[1].substring(0, key_value[1].length()-1);
        // System.out.println(key_value[1]);
    return key_value;
 }
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
	String [] key_value_arr;
	key_value_arr = new String [2];
	key_value_arr = process_string(line);
        context.write(new Text(key_value_arr[0]), new Text(key_value_arr[1]));
    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "cleardata");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(Cleardata.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
