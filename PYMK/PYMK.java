
// Java util packages
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// hadoop connfiguration jobs packages
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

//Hadoop in/out packages
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// Hadoop mapreduce packages
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// Hadoop io packages
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;




// main class for People You May Know (PYMK)
public class PYMK {

/* 
		Friends degree is sort of a touple (friend , degree).
		for each user, this object shows the type of friendship that the user has with another user.
		1 => first degree friendship : users are friend with each other directly
		2 => second degree friendship : users are both friends to a same user. 
		note that the ID and the degree can not be int because The key class of a mapper that maps text files is always LongWritable. 
		That is because it contains the byte offset of the current line and this could easily overflow an integer.
		The answer taken from StackOverflow 
		https://stackoverflow.com/questions/14922087/hadoop-longwritable-cannot-be-cast-to-org-apache-hadoop-io-intwritable
	*/


	static public class friendDegree implements Writable {
		public Long userID, degree;
		
		public void readFields(DataInput in) throws IOException {
			userID = in.readLong();
			degree = in.readLong();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeLong(userID);
			out.writeLong(degree);
		}
		
		public friendDegree(Long userID, Long degree) {
			this.userID = userID;
			this.degree = degree;
		}
		
		public friendDegree() {
			this(0L, 0L);
		}
	}

	
	//Map Class
	public static class PYMKMapper extends Mapper<LongWritable, Text, LongWritable, friendDegree> {
		/*
            for each friend in the friend list of each user, the main user is 
            the first degree friend and all the others are degree two friend.
        */

		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long User = Long.parseLong(line[0]); 					
			List<Long> UsersList = new ArrayList<Long>();
			
			
			
			if (line.length == 2) {
				StringTokenizer token = new StringTokenizer(line[1], ",");
				while (token.hasMoreTokens()) {
					Long pushTouser = Long.parseLong(token.nextToken());
					UsersList.add(pushTouser);
					ctx.write(new LongWritable(User), new friendDegree(pushTouser, -1L));
				}
				
				
				for (int a = 0; a < UsersList.size(); a++) {
					for (int b = a + 1; b < UsersList.size(); b++) {
						ctx.write(new LongWritable(UsersList.get(a)), new friendDegree((UsersList.get(b)), User));
						ctx.write(new LongWritable(UsersList.get(b)), new friendDegree((UsersList.get(a)), User));
					}
				}
			}
		}
	}
	
	//Reduce Class
	public static class PYMKReducer extends Reducer<LongWritable, friendDegree, LongWritable, Text> {
		/*
        for the reduce function, the input is a key which is the user and a list of pairs (user, 1) or (user,2)
        if the type is 1, we ignore it. second degree friends is what is important for us. 
        
        in the hash map, we count the number of secondary friends each user has with other users. 
		*/

		public void reduce(LongWritable key, Iterable<friendDegree> values, Context ctx)
				throws IOException, InterruptedException {
			final java.util.Map<Long, List<Long>> degree = new HashMap<Long, List<Long>>();
			for (friendDegree value : values) {
				final Boolean isFriend = (value.degree == -1);
				final Long pushTouser = value.userID;
				final Long mut_friends = value.degree;

				if (degree.containsKey(pushTouser)) {
					if (isFriend) {
						degree.put(pushTouser, null);
					} else if (degree.get(pushTouser) != null) {
						degree.get(pushTouser).add(mut_friends);
					}
				} else {
					if (!isFriend) {
						degree.put(pushTouser, new ArrayList<Long>() {
							{
								add(mut_friends);
							}
						});
					} else {
						degree.put(pushTouser, null);
					}
				}
			}

			// Sorting Common friends
			java.util.SortedMap<Long, List<Long>> friendsSort = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
				public int compare(Long k1, Long k2) {
					Integer v1 = degree.get(k1).size();
					Integer v2 = degree.get(k2).size();
					if (v1 > v2) {
						return -1;
					} else if (v1.equals(v2) && k1 < k2) {
						return -1;
					} else {
						return 1;
					}
				}
			});

			for (java.util.Map.Entry<Long, List<Long>> entry : degree.entrySet()) {
				if (entry.getValue() != null) {
					friendsSort.put(entry.getKey(), entry.getValue());
				}
			}

			Integer i = 0;
			String out = "";
			for (java.util.Map.Entry<Long, List<Long>> entry : friendsSort.entrySet()) {
				if (i == 0) {
					out = entry.getKey().toString();
				} else if (i < 10){
					out += "," + entry.getKey().toString();
				}
				++i;
			}
			ctx.write(key, new Text(out));
		}
	}


	// Main Function//
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PYMK");
		job.setJarByClass(PYMK.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(friendDegree.class);
		job.setMapperClass(PYMKMapper.class);
		job.setReducerClass(PYMKReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem writeToOut = new Path(args[1]).getFileSystem(conf);
		writeToOut.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
