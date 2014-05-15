package hama.incremental.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * Incremental PageRank Algorithm
 * 按照迭代误差收敛
 * 
 * @author hama
 *
 */

public class IncrementalPageRank {

	public static class PageRankVertex extends
			Vertex<IntWritable, NullWritable, DoubleWritable> {
		
		private int numEdges;
		private double iterationError;
		
		@Override
		public void setup(Configuration conf) {
			String str = this.getConf().get("hama.pagerank.iteration.error", "0.00001");
			iterationError = Double.parseDouble(str);
			numEdges=getEdges().size();
		}

		@Override
		public void compute(Iterator<DoubleWritable> messages) throws IOException{
			if(getSuperstepCount()==0) {
				setValue(new DoubleWritable(0.15));
				sendMessageToNeighbors(new DoubleWritable(0.85*0.15/numEdges));
			} else {
				double delta=0.0;
				double newValue=0.0;
				
				while(messages.hasNext()) {
					DoubleWritable msg=messages.next();
					delta+=msg.get();
				}
				if(delta>iterationError) {
					newValue=getValue().get()+delta;
					setValue(new DoubleWritable(newValue));
					sendMessageToNeighbors(new DoubleWritable(0.85*delta/numEdges));
				} 
			}
			voteToHalt();
		}
	}

	public static class PagerankTextReader
			extends
			VertexInputReader<LongWritable, Text, IntWritable, NullWritable, DoubleWritable> {
		
		@Override
		public boolean parseVertex(LongWritable key, Text value,
				Vertex<IntWritable, NullWritable, DoubleWritable> vertex)
				throws Exception {
			String[] split = value.toString().split("\t");
			for (int i = 0; i < split.length; i++) {
				if (i == 0) {
					vertex.setVertexID(new IntWritable(Integer.parseInt(split[i])));
				} else {
					vertex.addEdge(new Edge<IntWritable, NullWritable>(new IntWritable(Integer.parseInt(split[i])), null));
				}
			}
			return true;
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length < 3) {
			System.err.println("Usage: <iterationError> <input> <output>");
			System.exit(-1);
		}
		
		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		GraphJob pageJob = new GraphJob(conf, IncrementalPageRank.class);
		pageJob.setJobName("Incremental Pagerank New");

		pageJob.setVertexClass(PageRankVertex.class);
		
		pageJob.set("hama.pagerank.iteration.error", args[0]);
		pageJob.setInputPath(new Path(args[1]));
		pageJob.setOutputPath(new Path(args[2]));

		pageJob.setMaxIteration(301);

		pageJob.setVertexIDClass(IntWritable.class);
		pageJob.setVertexValueClass(DoubleWritable.class);
		pageJob.setEdgeValueClass(NullWritable.class);

		pageJob.setInputKeyClass(LongWritable.class);
		pageJob.setInputValueClass(Text.class);
		pageJob.setInputFormat(TextInputFormat.class);
		pageJob.setVertexInputReaderClass(PagerankTextReader.class);
		pageJob.setPartitioner(HashPartitioner.class);
		pageJob.setOutputFormat(TextOutputFormat.class);
		pageJob.setOutputKeyClass(Text.class);
		pageJob.setOutputValueClass(DoubleWritable.class);
		pageJob.waitForCompletion(true);
	}
}
