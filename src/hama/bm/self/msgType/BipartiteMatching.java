/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hama.bm.self.msgType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import com.google.common.base.Objects;

/**
 * Randomized Matching Algorithm for bipartite matching in the Pregel Model.
 * 
 */
public final class BipartiteMatching {

	private static final IntWritable UNMATCHED = new IntWritable(-1);
	public static final String SEED_CONFIGURATION_KEY = "bipartite.matching.seed";

	public static class BipartiteMatchingVertex extends
			Vertex<IntWritable, NullWritable, ValuePair<IntWritable,Text>> {

		// Components
		private final static Text LEFT = new Text("L");
		private final static Text RIGHT = new Text("R");

		// Needed because Vertex value and message sent have same types.
		private ValuePair<IntWritable,Text> requestMessage;
		private ValuePair<IntWritable,Text> grantMessage;
		private ValuePair<IntWritable,Text> denyMessage;
		private ValuePair<IntWritable,Text> acceptMessage;

		private Random random;

		@Override
		public void setup(Configuration conf) {
			requestMessage = new ValuePair<IntWritable,Text>(getVertexID(), new Text("request"));
			grantMessage = new ValuePair<IntWritable,Text>(getVertexID(), new Text("grant"));
			denyMessage = new ValuePair<IntWritable,Text>(getVertexID(), new Text("deny"));
			acceptMessage = new ValuePair<IntWritable,Text>(getVertexID(), new Text("accept"));
			
			random = new Random(Long.parseLong(getConf().get(
					SEED_CONFIGURATION_KEY, System.currentTimeMillis() + "")));
		}

		@Override
		public void compute(Iterator<ValuePair<IntWritable,Text>> messages) throws IOException {
			if (isMatched()) {
				voteToHalt();
				return;
			}
			if(getSuperstepCount()==0) {	//phase 0
				if (Objects.equal(getComponent(), LEFT)) {
					sendMessageToNeighbors(getRequestMessage());
					voteToHalt();
				}
			} else {
				if(messages.hasNext()) {
					ValuePair<IntWritable, Text> firstMsg = messages.next();
					String msgType = firstMsg.getSecond().toString();
					
					if (msgType.equals("request")) { // phase 1
						if (Objects.equal(getComponent(), RIGHT)) {
							List<ValuePair<IntWritable, Text>> buffer = new ArrayList<ValuePair<IntWritable, Text>>();
							buffer.add(firstMsg);
							while (messages.hasNext()) {
								buffer.add(messages.next());
							}
							if (buffer.size() > 0) {
								int luck = RandomUtils.nextInt(random,buffer.size());
								for (int i = 0; i < buffer.size(); i++) {
									ValuePair<IntWritable, Text> luckMsg = buffer.get(i);
									IntWritable sourceVertexID = getSourceVertex(luckMsg);
									if (i != luck) {
										sendMessage(sourceVertexID,getDenyMessage());
									} else {
										sendMessage(sourceVertexID,getGrantMessage());
									}
								}
							}
							voteToHalt();
						}
					} else if (msgType.equals("grant") || msgType.equals("deny")) { // phase 2.有可能同时收到deny和grant消息，且deny在前。
						if (Objects.equal(getComponent(), LEFT)) {
							List<ValuePair<IntWritable,Text>> buffer = new ArrayList<ValuePair<IntWritable,Text>>();
							//若该顶点只收到拒绝消息，则此Supertep只会由系统自动把它激活。
							//在compute并作不做任何操作，继续保持Active状态，直到下一循环的第0阶段
							if(msgType.equals("grant")) {
								buffer.add(firstMsg);
							}
							while (messages.hasNext()) {
								ValuePair<IntWritable,Text> msg=messages.next();
								if(msg.getSecond().toString().equals("grant")) {
									buffer.add(msg);
								}
							}
							if (buffer.size() > 0) {
								ValuePair<IntWritable,Text> luckyMsg = buffer.get(RandomUtils.nextInt(random, buffer.size()));
								IntWritable sourceVertexID = getSourceVertex(luckyMsg);
								setMatchVertex(sourceVertexID);
								sendMessage(sourceVertexID, getAcceptMessage());
								voteToHalt();  //收到同意消息的，会匹配成功，并且Inactive
							}
						}
					} else if (msgType.equals("accept")) { // phase 3
						if (Objects.equal(getComponent(), RIGHT)) {
							IntWritable sourceVertexID = getSourceVertex(firstMsg);
							setMatchVertex(sourceVertexID);
							voteToHalt();
						}
					}
				} else { // phase 0 ,active vertex(left) don't receive message
					if (Objects.equal(getComponent(), LEFT)) {
						sendMessageToNeighbors(getRequestMessage());
						voteToHalt();
					}
				}
			}
		}

		/**
		 * Finds the vertex from which "msg" came.
		 */
		private static IntWritable getSourceVertex(ValuePair<IntWritable,Text> msg) {
			return msg.getFirst();
		}

		/**
		 * @return the requestMessage
		 */
		public ValuePair<IntWritable, Text> getRequestMessage() {
			return requestMessage;
		}

		/**
		 * @return the grantMessage
		 */
		public ValuePair<IntWritable, Text> getGrantMessage() {
			return grantMessage;
		}

		/**
		 * @return the denyMessage
		 */
		public ValuePair<IntWritable, Text> getDenyMessage() {
			return denyMessage;
		}

		/**
		 * @return the acceptMessage
		 */
		public ValuePair<IntWritable, Text> getAcceptMessage() {
			return acceptMessage;
		}

		/**
		 * Pairs "this" vertex with the "matchVertex"
		 */
		private void setMatchVertex(IntWritable matchVertexID) {
			getValue().setFirst(matchVertexID);
		}
		
		

		/**
		 * Returns the component{LEFT/RIGHT} to which this vertex belongs.
		 */
		private Text getComponent() {
			return getValue().getSecond();
		}

		private boolean isMatched() {
			return !getValue().getFirst().equals(UNMATCHED);
		}

	}

	/**
	 * 
	 * Input graph is given as<br/>
	 * <Vertex> <component value>: <adjacent_vertex_1> <adjacent_vertex_2> ..<br/>
	 * A L:B D<br/>
	 * B R:A C<br/>
	 * C L:B D<br/>
	 * D R:A C<br/>
	 */
	public static class BipartiteMatchingVertexReader extends
			VertexInputReader<LongWritable, Text, IntWritable, NullWritable, ValuePair<IntWritable,Text>> {

		@Override
		public boolean parseVertex(LongWritable key, Text value,
				Vertex<IntWritable, NullWritable, ValuePair<IntWritable,Text>> vertex) throws Exception {

			String[] tokenArray = value.toString().split(":");
			String[] adjArray = tokenArray[1].trim().split(" ");
			String[] selfArray = tokenArray[0].trim().split(" ");

			vertex.setVertexID(new IntWritable(Integer.parseInt(selfArray[0])));
			vertex.setValue(new ValuePair<IntWritable,Text>(UNMATCHED, new Text(selfArray[1]))
					.setNames("MatchVertex", "Component"));
			// initially a node is unmatched, which is denoted by U.

			for (String adjNode : adjArray) {
				vertex.addEdge(new Edge<IntWritable, NullWritable>(new IntWritable(Integer.parseInt(adjNode)),
						null));
			}
			return true;
		}
	}

	public static void main(String... args) throws IOException,
			InterruptedException, ClassNotFoundException {

		if (args.length < 2) {
			System.err.println("Usage: <input> <output> ");
			System.exit(-1);
		}

		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		GraphJob job = new GraphJob(conf, BipartiteMatching.class);
		// set the defaults
		conf.set(SEED_CONFIGURATION_KEY, System.currentTimeMillis() + "");
		
		job.setJobName("BipartiteMatching");
		job.setInputPath(new Path(args[0]));
		job.setOutputPath(new Path(args[1]));

		job.setVertexClass(BipartiteMatchingVertex.class);
		job.setVertexIDClass(IntWritable.class);
		job.setVertexValueClass(ValuePair.class);
		job.setEdgeValueClass(NullWritable.class);

		job.setInputKeyClass(LongWritable.class);
		job.setInputValueClass(Text.class);
		job.setInputFormat(TextInputFormat.class);
		job.setVertexInputReaderClass(BipartiteMatchingVertexReader.class);
		job.setPartitioner(HashPartitioner.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ValuePair.class);

		job.waitForCompletion(true);
	}

}
