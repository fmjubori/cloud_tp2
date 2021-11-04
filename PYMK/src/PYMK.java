/*** JAVA Imports  ***/
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*** Hadoop Imports  ***/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class PYMK {
	static public class friendCounts implements Writable {
		public Long userID, comFriends;
		
		public void readFields(DataInput in) throws IOException {
			userID = in.readLong();
			comFriends = in.readLong();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeLong(userID);
			out.writeLong(comFriends);
		}
		
		public friendCounts(Long userID, Long comFriends) {
			this.userID = userID;
			this.comFriends = comFriends;
		}
		
		public friendCounts() {
			this(-1L, -1L);
		}
	}

	
	//Mapper Class
	public static class PYMKMapper extends Mapper<LongWritable, Text, LongWritable, friendCounts> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long User = Long.parseLong(line[0]); 					//User ID
			List<Long> Users_List = new ArrayList<Long>(); 			//Users list (Friends)
			if (line.length == 2) {
				StringTokenizer token = new StringTokenizer(line[1], ",");
				while (token.hasMoreTokens()) {
					Long toUser = Long.parseLong(token.nextToken());
					Users_List.add(toUser);
					context.write(new LongWritable(User), new friendCounts(toUser, -1L));
				}
				
				
				for (int a = 0; a < Users_List.size(); a++) {
					for (int b = a + 1; b < Users_List.size(); b++) {
						context.write(new LongWritable(Users_List.get(a)), new friendCounts((Users_List.get(b)), User));
						context.write(new LongWritable(Users_List.get(b)), new friendCounts((Users_List.get(a)), User));
					}
				}
			}
		}
	}
	
	//Reducer Class
	/*Key-> Recommended Friend, Value-> List of Mutual Friend*/
	public static class PYMKReducer extends Reducer<LongWritable, friendCounts, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<friendCounts> values, Context context)
				throws IOException, InterruptedException {
			final java.util.Map<Long, List<Long>> comFriends = new HashMap<Long, List<Long>>();
			for (friendCounts value : values) {
				final Boolean isFriend = (value.comFriends == -1);
				final Long toUser = value.userID;
				final Long mutualFriend = value.comFriends;

				if (comFriends.containsKey(toUser)) {
					if (isFriend) {
						comFriends.put(toUser, null);
					} else if (comFriends.get(toUser) != null) {
						comFriends.get(toUser).add(mutualFriend);
					}
				} else {
					if (!isFriend) {
						comFriends.put(toUser, new ArrayList<Long>() {
							{
								add(mutualFriend);
							}
						});
					} else {
						comFriends.put(toUser, null);
					}
				}
			}

			// Sorting all the Mutual friends using Tree Map
			java.util.SortedMap<Long, List<Long>> sortFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
				public int compare(Long key1, Long key2) {
					Integer value1 = comFriends.get(key1).size();
					Integer value2 = comFriends.get(key2).size();
					if (value1 > value2) {
						return -1;
					} else if (value1.equals(value2) && key1 < key2) {
						return -1;
					} else {
						return 1;
					}
				}
			});

			for (java.util.Map.Entry<Long, List<Long>> entry : comFriends.entrySet()) {
				if (entry.getValue() != null) {
					sortFriends.put(entry.getKey(), entry.getValue());
				}
			}

			Integer i = 0;
			String output = "";
			for (java.util.Map.Entry<Long, List<Long>> entry : sortFriends.entrySet()) {
				if (i == 0) {
					output = entry.getKey().toString();
				} else if (i < 10){
					output += "," + entry.getKey().toString();
				}
				++i;
			}
			context.write(key, new Text(output));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PYMK");
		job.setJarByClass(PYMK.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(friendCounts.class);
		job.setMapperClass(PYMKMapper.class);
		job.setReducerClass(PYMKReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
