# HadoopPrograms

##Aim of the program
The input of this program are lines of text of which each line contains 3 URI's. This program extracts the domain names from these URI's and find pairs of domains occurring in the same line. The program outputs the total count of each of these pairs of domains.

##To execute this program
- Copy the inputdata to the hadoop filesystem by executing the command  
  bin/hadoop dfs -copyFromLocal inputdata input  
where inputdata is the input data directory
- Execute the program using the command  
bin/hadoop jar $FOLDER/uri.jar URIPairs input output  
where $FOLDER is the folder in which uri.jar is put  

##The structure of the program
The program uses TextInputFormat as the InputFormat. Hence, the hadoop system splits the given files into chunks of data by itself. Each line is fed in to the map task as input.

In the map task, String tokenizer class is used to retrieve each word.  Hence, each word is assumed to be one that ends in a space. Each word is checked for, if it contains ‘http://’. If so further operations are carried on to extract the domain name, by checking the position of the next ‘/’. If found, the string till that position, is taken as the domain name else, the string till the end of string is taken to be the domain uri.

 Once the required domain names are got, they are combined into string pair, taking into account the lexicographic order and also that duplicates are avoided and that the same domain name doesn’t repeat itself in the same combination.
 
   The output of the map task is the string pair and the count 1.
   
   Later in the reduce phase, the counts of each string pair are added and the result returned.
   
##Input dependency
   The program assumes the structure of the input to be :
-	Each line contains 3 different strings separated by space(But later during execution it was found that spaces are there in a particular string too). The program to an extent takes care of this too.
-	Each URI would contain the substring ‘http://’
-  The domain name of the URI would be between ‘http://’ and next ‘/’/ Hence, the program assumes that the characters that is between ‘http://’ and the next ‘/’ is a part of the domain name. This is how usually all URI’s behave. In case that there is no ‘/’ in the remaining part of the string, then the whole string, starting from ‘http://’ and the remaining characters would form the URI of the domain.

##Performance of the application: 
-	The application took around 7 minutes to complete using 16 nodes. When the nodes were changed there were almost a linear increase in the performance. 

    
      
      **No: of nodes**                         4           8         16
      
      **Execution time(in minutes)**          19          13          7

This shows that the performance increases almost linearly with the number of nodes. But as the number of nodes is increased, the number of map tasks and reduce tasks is increased, which causes an overhead time too. Hence, the performance is not clearly linear.


