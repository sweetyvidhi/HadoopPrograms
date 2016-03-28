/**
*	The input of this program are lines of text of which each line contains 3 URI's. This program extracts the     
*   domain names from these URI's and find pairs of domains occuring in the same line. The program 
*	outputs the total count of each of these pairs of domains.
*	
* @author Vidya Lakshmi Rajagopalan
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class URIPairs {

  public static class URIMapper 
      extends Mapper<Object, Text, Text, IntWritable>{

    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {  

      StringTokenizer itr = new StringTokenizer(value.toString());
      String t=new String();
      ArrayList<String> domainlist=new ArrayList<String>();

      int i=0,j=0,ind,ind1;
      String  str = new String();
      String tempstr = new String();
      while (itr.hasMoreTokens())				//In this loop, each line of text is scanned word by word and 'domain' names are found
	{
		str=itr.nextToken();
		ind=str.indexOf("http://");				//Finding the start of the domain name
		if(ind>=0)						
		{
		tempstr=str.substring(ind,ind+7);
		if(str.length()>ind+7)
		{
			ind1=str.indexOf('/',ind+7);		//Finding if the url has further '/' which indicates we need to extract before the next /'
			if(ind1==-1)						//If further '/' is not found, the substring starting from position 'ind' to the end of the string is extracted to form the domain name
			    {
				t="<"+str.substring(ind,str.length())+">";
				domainlist.add(t);

			    }
			else								//If a subsequent '/' is found in the string, a substring starting from position 'ind' to the position just before '/' is extracted
			   {
				t="<"+str.substring(ind,ind1)+">";  
				domainlist.add(t);

			   }
			i++;
		}
		}
	}
	ind=i;
	String[] temp=new String[ind];
	for(i=0;i<ind;i++)
		temp[i]=domainlist.get(i);
	for(i=0;i<ind;i++)						// All possible domain pairs are formed in lexicographic order and outputted with a value of 'one'
	  for(j=i+1;j<ind;j++)
		{ if(temp[i].compareToIgnoreCase(temp[j])<0)
	              {   str=temp[i]+" "+temp[j];

			  word.set(str);
                          context.write(word, one);

                      }
		  else if (temp[i].compareToIgnoreCase(temp[j])>0)
		      {
			   str=temp[j]+" "+temp[i];

			   word.set(str);
                           context.write(word, one);

		      }
		      
                }
        }
   }

  public static class URIReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {		//The reducer finds the total number of domain pairs with their counts

    private IntWritable result = new IntWritable();


     public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
	result.set(sum);
	context.write(key,result);
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    
    Job job = new Job(conf, "uri pairs");
    job.setJarByClass(URIPairs.class);
    job.setMapperClass(URIMapper.class);
    job.setCombinerClass(URIReducer.class);
    job.setReducerClass(URIReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
