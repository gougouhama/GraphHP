package hama.sssp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class SSSP {
	public static final String START_VERTEX = "shortest.paths.start.vertex.name";

	public static class ShortestPathVertex extends Vertex<IntWritable, IntWritable, IntWritable> {
		public static final Log LOG = LogFactory.getLog(ShortestPathVertex.class);

		public ShortestPathVertex() {
			this.setValue(new IntWritable(Integer.MAX_VALUE));
		}

		public boolean isStartVertex() {
			IntWritable startVertex = new IntWritable(Integer.parseInt(getConf().get(START_VERTEX)));
			return (this.getVertexID().equals(startVertex)) ? true : false;
		}

		@Override
		public void compute(Iterator<IntWritable> messages) throws IOException {
			int minDist = isStartVertex() ? 0 : Integer.MAX_VALUE;

			while (messages.hasNext()) {
				IntWritable msg = messages.next();
				if (msg.get() < minDist) {
					minDist = msg.get();
				}
			}
			if (minDist < this.getValue().get()) {
				this.setValue(new IntWritable(minDist));
				for (Edge<IntWritable, IntWritable> e : this.getEdges()) {
					sendMessage(e,new IntWritable(minDist + e.getValue().get()));
				}
			}
			voteToHalt();
		}
	}

	public static class MinIntCombiner extends Combiner<IntWritable> {

		/**
		 * 获取最小值
		 */
		@Override
		public IntWritable combine(Iterable<IntWritable> messages) {
			int minDist = Integer.MAX_VALUE;

			Iterator<IntWritable> it = messages.iterator();
			while (it.hasNext()) {
				int msgValue = it.next().get();
				if (minDist > msgValue)
					minDist = msgValue;
			}
			return new IntWritable(minDist);
		}
	}

	public static class SSSPTextReader extends
			VertexInputReader<LongWritable, Text, IntWritable, IntWritable, IntWritable> {
		@Override
		public boolean parseVertex(LongWritable key, Text value,
				Vertex<IntWritable, IntWritable, IntWritable> vertex) throws Exception {
			String[] split = value.toString().split("\t");
			for (int i = 0; i < split.length; i++) {
				if (i == 0) {
					vertex.setVertexID(new IntWritable(Integer.parseInt(split[i])));
				} else {
					String[] split2 = split[i].split(":");
					vertex.addEdge(new Edge<IntWritable, IntWritable>(new IntWritable(Integer.parseInt(split2[0])), 
						new IntWritable(Integer.parseInt(split2[1]))));
				}
			}
			return true;
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length < 3) {
			System.out.println("Usage: <startnode> <input> <output>");
			System.exit(-1);
		}
		// Graph job configuration
		HamaConfiguration conf = new HamaConfiguration();
		GraphJob ssspJob = new GraphJob(conf, SSSP.class);
		ssspJob.setJobName("Single Source Shortest Path");

		conf.set(START_VERTEX, args[0]);
		ssspJob.setInputPath(new Path(args[1]));
		ssspJob.setOutputPath(new Path(args[2]));
		
		ssspJob.setBounaryVertexParticipateLocalphase(true);

		ssspJob.setVertexClass(ShortestPathVertex.class);
		ssspJob.setCombinerClass(MinIntCombiner.class);
		ssspJob.setInputFormat(TextInputFormat.class);
		ssspJob.setInputKeyClass(LongWritable.class);
		ssspJob.setInputValueClass(Text.class);

		ssspJob.setPartitioner(HashPartitioner.class);
		ssspJob.setVertexInputReaderClass(SSSPTextReader.class);
		
		ssspJob.setOutputFormat(TextOutputFormat.class);
		ssspJob.setOutputKeyClass(IntWritable.class);
		ssspJob.setOutputValueClass(IntWritable.class);
		// Iterate until all the nodes have been reached.
		ssspJob.setMaxIteration(Integer.MAX_VALUE);

		ssspJob.setVertexIDClass(IntWritable.class);
		ssspJob.setVertexValueClass(IntWritable.class);
		ssspJob.setEdgeValueClass(IntWritable.class);
		
		ssspJob.waitForCompletion(true);
	}
}
