
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

	/* 
		Friends degree is sort of a touple (friend , degree).
		for each user, this object shows the type of friendship that the user has with another user.

		-1 => first degree friendship : users are friend with each other directly
		userId => second degree friendship : users are both friends to a same user. 

		note that the ID and the degree can not be int because The key class of a mapper that maps text files is always LongWritable. 
		That is because it contains the byte offset of the current line and this could easily overflow an integer.

		The answer taken from StackOverflow 
		https://stackoverflow.com/questions/14922087/hadoop-longwritable-cannot-be-cast-to-org-apache-hadoop-io-intwritable


	*/
	static public class friendDegree implements Writable {
		public Long userID;
		public Long degree;
		

		public void readFields(DataInput in ) throws IOException{
			userID = in.readLong();
			degree = in.readLong();
		}

		public void write(DataOutput out) throws IOException{
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

	
	//Mapper Class
	public static class PYMKMapper extends Mapper<LongWritable, Text, LongWritable, friendDegree> {

		/*
         *    for each friend in the friend list of each user, the main user is 
         *    the first degree friend and all the others are degree two friend.
         */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line[] = value.toString().split("\t");
			Long user = Long.parseLong(line[0]); 					
			List<Long> Users_List = new ArrayList<>();
			
			if (line.length == 2){

				String[] list = line[1].toString().split(",");

				/**
				 * Every user in the list of friends related to a specific user is a degree 1 friend with the user.
				 */
				for (String i : list) {
					Long curUser = Long.parseLong(i);
					Users_List.add(curUser);
					context.write(new LongWritable(user), new friendDegree(curUser, -1L));
				}


				/**
				 * all the users in the friends list related to a specific user are degree two friends with each other (two by two)
				 */

				for (Long element : Users_List){
					LongWritable suggestion = new LongWritable(element);

					for (int i = Users_List.indexOf(element) + 1; i < Users_List.size(); i++){
						LongWritable suggestion2 = new LongWritable(Users_List.get(i));

						context.write(suggestion, new friendDegree(Users_List.get(i) , user));
						context.write(suggestion2, new friendDegree(element , user));
					}

				}
			}
		}
	}
	
	
	/*
        for the reduce function, the input is a key which is the user and a list of pairs (user, 1) or (user,2)
        if the type is 1, we ignore it. second degree friends is what is important for us. 
        
        in the hash map, we count the number of secondary friends each user has with other users. 
    */
	public static class PYMKReducer extends Reducer<LongWritable, friendDegree, LongWritable, Text> {


		public void reduce(LongWritable key, Iterable<friendDegree> values, Context context) throws IOException, InterruptedException{

			HashMap <Long, Integer> friendsMap = new HashMap<Long, Integer>();
			for (friendDegree value : values) {
		
				Long curUser = value.userID;
				Long mutualFriend = value.degree;

				/**
				 * this is the actual reduce part. 
				 * we simply add the number of degree 2 friends that each user has
				 */

                if (value.degree == -1){
                    friendsMap.put(curUser, -1);

                }else{
					if (friendsMap.containsKey(curUser)) {
                        if (friendsMap.get(curUser) != -1){
                            int count = friendsMap.get(curUser);
                            friendsMap.remove(curUser);
						    friendsMap.put(curUser, count + 1);
                        } 
					}else{
						friendsMap.put(curUser, 1);
					}
				}
			}



			// Sorting all the Mutual friends using Tree Map
			SortedMap<Long, Integer> sortFriends = new TreeMap<Long, Integer>(new Comparator<Long>() {
				public int compare(Long key1, Long key2) {
					int value1 = friendsMap.get(key1);
					int value2 = friendsMap.get(key2);
					if (value1 > value2) {
						return -1;
					} else if (value2 == value1 && key1 < key2){
						return -1;
					}
					else {
						return 1;
					}
				}
			});

			for (java.util.Map.Entry<Long, Integer> entry : friendsMap.entrySet()) {
				if (entry.getValue() != -1) {
					sortFriends.put(entry.getKey(), entry.getValue());
				}
			}

			
			String result = "";
			int i = 0;
			for (java.util.Map.Entry<Long, Integer> entry : sortFriends.entrySet()){
				if (i == 0) {
					result = entry.getKey().toString();
				} else if (i < 10){
					result += "," + entry.getKey().toString();
				}
				i++;
			}
			context.write(key, new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {


        /**
         * setting up a mapreduce job and adding attributes
         */

		Configuration conf = new Configuration();

		Job job = new Job(conf, "PYMK");
		job.setJarByClass(PYMK.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(friendDegree.class);
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
