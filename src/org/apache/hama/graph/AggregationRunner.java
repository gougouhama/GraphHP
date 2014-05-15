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
package org.apache.hama.graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import com.google.common.base.Preconditions;

/**
 * Runner class to do the tasks that need to be done if aggregation was
 * configured.
 * 
 */
public final class AggregationRunner<V extends Writable, E extends Writable, M extends Writable> {

	// multiple aggregator arrays
	private Aggregator<M, Vertex<V, E, M>>[] aggregators;

	private Writable[] globalAggregatorResult;
	private IntWritable[] globalAggregatorIncrement;

	private boolean[] isAbstractAggregator;
	private String[] aggregatorClassNames;

	private Text[] aggregatorValueFlag;
	private Text[] aggregatorIncrementFlag;
	// aggregator on the master side
	private Aggregator<M, Vertex<V, E, M>>[] masterAggregator;

	private boolean enabled = false;
	private Configuration conf;

//	private static final Log LOG = LogFactory.getLog(AggregationRunner.class);

	@SuppressWarnings("unchecked")
	public void setupAggregators(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
		this.conf = peer.getConfiguration();
		String aggregatorClasses = peer.getConfiguration().get(
				GraphJob.AGGREGATOR_CLASS_ATTR);
		if (aggregatorClasses != null) {
			enabled = true;	// 为 true ,说明使用了聚集器

			aggregatorClassNames = aggregatorClasses.split(";");
//			LOG.info("hama.graph.aggregator.class 有:"
//					+ Arrays.toString(aggregatorClassNames));

			// init to the split size
			aggregators = new Aggregator[aggregatorClassNames.length];
			globalAggregatorResult = new Writable[aggregatorClassNames.length];
			globalAggregatorIncrement = new IntWritable[aggregatorClassNames.length];
			isAbstractAggregator = new boolean[aggregatorClassNames.length];
			aggregatorValueFlag = new Text[aggregatorClassNames.length];
			aggregatorIncrementFlag = new Text[aggregatorClassNames.length];

			/**
			 * 只有master peer上才创建 masterAggregator，只是数组，里面尚未定义 The master task is
			 * the first peer in the peer array.
			 */
			if (GraphJobRunner.isMasterTask(peer)) {
				masterAggregator = new Aggregator[aggregatorClassNames.length];
			}
			for (int i = 0; i < aggregatorClassNames.length; i++) {
				// 根据类名称创建类对象（反射机制），赋值给aggregators［i］
				aggregators[i] = getNewAggregator(aggregatorClassNames[i]);
				// 此处打log目的 : 为了和sendAggregatorValues里创建的aggregators[i]比较，是否一样
				// 结论：不一样 
				// 为何还要创建? 为了下一次迭代
				//LOG.info("aggregators["+i+"] ： "+aggregators[i].hashCode());

				aggregatorValueFlag[i] = new Text(
						GraphJobRunner.S_FLAG_AGGREGATOR_VALUE + ";" + i);
				aggregatorIncrementFlag[i] = new Text(
						GraphJobRunner.S_FLAG_AGGREGATOR_INCREMENT + ";" + i);

				if (aggregators[i] instanceof AbstractAggregator) {
					isAbstractAggregator[i] = true;
//					LOG.info("isAbstractAggregator[ " + i + " ]的内容 : "
//							+ isAbstractAggregator[i]);
				}
				if (GraphJobRunner.isMasterTask(peer)) {
					// 定义数组里面的对象,为 MasterPeer
					masterAggregator[i] = getNewAggregator(aggregatorClassNames[i]);
					//LOG.info("masterAggregator["+i+"] ： "+masterAggregator[i].hashCode());
				}
			}
		}
	}

	/**
	 * Runs the aggregators by sending their values to the master task.
	 * 每个peer把聚集器中的值发送给master peer，采用MapWritable结构，key为标记,value为要统计的值
	 */
	public void sendAggregatorValues(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
			int activeVertices) throws IOException {
		// send msgCounts to the master task
		MapWritable updatedCnt = new MapWritable();
		
		// 初始被doInitialSuperstep调用时，activeVertices参数的值为 1
		// 后续调用时，activeVertices表示每个peer上还活跃的数目
		updatedCnt.put(GraphJobRunner.FLAG_MESSAGE_COUNTS, new IntWritable(
				activeVertices));
		// also send aggregated values to the master
		if (aggregators != null) {
			for (int i = 0; i < this.aggregators.length; i++) {
				// 每个aggregators聚集操作后的值
				updatedCnt.put(aggregatorValueFlag[i],aggregators[i].getValue());
				if (isAbstractAggregator[i]) {
					// 每个aggregators聚集操作的次数
					updatedCnt.put(aggregatorIncrementFlag[i],
									((AbstractAggregator<M, Vertex<V, E, M>>) aggregators[i])
											.getTimesAggregated());
				}
			}
			for (int i = 0; i < aggregators.length; i++) {
				// now create new aggregators for the next iteration   
			    // 再生成对象，为了下一次迭代。aggregators[i]的值在 aggregateVertex()方法中使用
				aggregators[i] = getNewAggregator(aggregatorClassNames[i]);
				//LOG.info("aggregators["+i+"] ： "+aggregators[i].hashCode());
				if (GraphJobRunner.isMasterTask(peer)) {
					masterAggregator[i] = getNewAggregator(aggregatorClassNames[i]);
					//LOG.info("masterAggregator["+i+"] ： "+masterAggregator[i].hashCode());
				}
			}
		}
		/**
		 * 输出updatedCnt的内容
		 */
//		LOG.info("updatedCnt中的内容为: ");
//		for(Writable key:updatedCnt.keySet()) {
//		LOG.info("key = "+key +"   value = "+updatedCnt.get(key));
//		}
//		LOG.info("把各个task上的updatedCnt发送给master task");
		peer.send(GraphJobRunner.getMasterTask(peer), new GraphJobMessage(
				updatedCnt));
	}

	/**
	 * Aggregates the last value before computation and the value after the
	 * computation.
	 * 
	 * lastvalue 为compute前的值，vertex经过一次compute计算后
	 * 目的：修改聚集器的值（AggregationRunner.aggregators[]）
	 * @param lastValue
	 *            the value before compute().
	 * @param v
	 *            the vertex.
	 */
	public void aggregateVertex(M lastValue, Vertex<V, E, M> v) {
		if (isEnabled()) {
			for (int i = 0; i < this.aggregators.length; i++) {
				Aggregator<M, Vertex<V, E, M>> aggregator = this.aggregators[i];
				// 各task（除过master）聚集自己感兴趣的值
				aggregator.aggregate(v, v.getValue());
//				LOG.info("聚集器序号： " + i + "    VertexID : " + v.getVertexID()
//						+ "   聚集器值： " + aggregator.getValue());
				if (isAbstractAggregator[i]) {
					AbstractAggregator<M, Vertex<V, E, M>> intern = (AbstractAggregator<M, Vertex<V, E, M>>) aggregator;
					intern.aggregate(v, lastValue, v.getValue()); // lastValue不为空时，才把差值加到AbstractAggregator.absoluteDifference上.
					intern.aggregateInternal(); // 记录聚集器调用次数
//					LOG.info("聚集器序号： " + i + "    VertexID : "
//							+ v.getVertexID() + "   聚集器值： " + intern.getValue());
//					LOG.info("聚集器序号： " + i + "    VertexID : "
//							+ v.getVertexID() + "   聚集器值(Internal)： "
//							+ intern.getTimesAggregated());
				}
			}
		}
	}

	/**
	 * clear the aggregator value of inner iteration(not the final) 
	 */
	public void clearInnerAggretateValue() {
		if(isEnabled()) {
			for(int i=0;i<this.aggregators.length;i++) {
				Aggregator<M, Vertex<V, E, M>> aggregator = this.aggregators[i];
				aggregator.clearValue();
			}
		}
	}
	
	/**
	 * The method the master task does, it globally aggregates the values of
	 * each peer and updates the given map accordingly.
	 * 
	 * 把 AggregationRunner.masterAggregator[]值重新计算（需要的话，如计算平均值）
	 * 
	 * 没有给GraphJobRunner.FLAG_MESSAGE_COUNTS对应的项赋值,那么在receiveAggregatedValues中
	 * 取出的值count为null
	 * IntWritable count = (IntWritable) updatedValues
					.get(GraphJobRunner.FLAG_MESSAGE_COUNTS);
	 */
	public void doMasterAggregation(MapWritable updatedCnt) {
		if (isEnabled()) {
			// work through the master aggregators
			for (int i = 0; i < masterAggregator.length; i++) {
				Writable lastAggregatedValue = masterAggregator[i].getValue();
				
				if (isAbstractAggregator[i]) {
					final AbstractAggregator<M, Vertex<V, E, M>> intern = ((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[i]);
					final Writable finalizeAggregation = intern.finalizeAggregation();
					if (intern.finalizeAggregation() != null) {
						lastAggregatedValue = finalizeAggregation;
					}
					// this count is usually the times of active
					// vertices in the graph
					updatedCnt.put(aggregatorIncrementFlag[i],intern.getTimesAggregated());
				}
				updatedCnt.put(aggregatorValueFlag[i], lastAggregatedValue);
			}
		}
//		for(Entry<Writable,Writable> entry :updatedCnt.entrySet()) {
////			LOG.info("key = "+entry.getKey()+"      value = "+entry.getValue());
//		}
	}

	/**
	 * Receives aggregated values from a master task, by doing an additional
	 * barrier sync and parsing the messages.
	 * 
	 * 	每个Peer收到Master发送过来的聚集器值，存储于各自的globalAggregatorResult[]和
	 * globalAggregatorIncrement［］ 上
	 * 
	 * @return always true if no aggregators are defined, false if aggregators
	 *         say we haven't seen any messages anymore.
	 */
	public boolean receiveAggregatedValues(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
			long iteration) throws IOException, SyncException,
			InterruptedException {
		// if we have an aggregator defined, we must make an additional sync
		// to have the updated values available on all our peers.
		if (isEnabled() && iteration >= 1) {
			peer.sync();

			MapWritable updatedValues = peer.getCurrentMessage().getMap();
			
			for (int i = 0; i < aggregators.length; i++) {
				globalAggregatorResult[i] = updatedValues
						.get(aggregatorValueFlag[i]);
				globalAggregatorIncrement[i] = (IntWritable) updatedValues
						.get(aggregatorIncrementFlag[i]);
//				LOG.info("globalAggregatorResult[ "+i+" ] : "+globalAggregatorResult[i]);
//				LOG.info("globalAggregatorIncrement[ "+i+" ] : "+globalAggregatorIncrement[i]);
			}
			IntWritable count = (IntWritable) updatedValues
					.get(GraphJobRunner.FLAG_MESSAGE_COUNTS);
			
			/**
			 * 注：GraphJobRunner.globalUpdateCounts的值在GraphJobRunner.parseMessages方法中
			 *    汇聚过，表示所有BSPPeer上active的Vertices数.
			 * 
			 * 在GraphJobRunner.bsp()函数调用的GraphJobRunner.doMasterUpdates()函数中，
			    if (globalUpdateCounts == 0) {	// 全部BSPPeer上没有一个活跃顶点
				    updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(Integer.MIN_VALUE));
			    } else {
			       // 此函数没有填充GraphJobRunner.FLAG_MESSAGE_COUNTS，故下面获取其值应为null
				   aggregationRunner.doMasterAggregation(updatedCnt); 
			    }
			
			 */
//			LOG.info("count = "+count);
			// 所有节点都进入inactive时，终止外层大循环，计算停止，进入GraphJobRunner的cleanup()处理
			if (count != null && count.get() == Integer.MIN_VALUE) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @return true if aggregators were defined. Normally used by the internal
	 *         stateful methods, outside shouldn't use it too extensively.
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * Method to let the master read messages from peers and aggregate a value.
	 */
	public void masterReadAggregatedValue(Text textIndex, M value) {
		int index = Integer.parseInt(textIndex.toString().split(";")[1]);
		// master task时,vertex为空。
		// 注意masterAggregator和aggregators[]里面的聚集器里面是不同的对象！！！
		// 聚集函数中，定义的 +=  ,连加
		masterAggregator[index].aggregate(null, value);
//		LOG.info("masterAggregator[ "+index +" ]的值getValue() ： "+masterAggregator[index].getValue());
	}

	/**
	 * Method to let the master read messages from peers and aggregate the
	 * incremental value.
	 */
	public void masterReadAggregatedIncrementalValue(Text textIndex, M value) {
		int index = Integer.parseInt(textIndex.toString().split(";")[1]);
		if (isAbstractAggregator[index]) {
			((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[index])
					.addTimesAggregated(((IntWritable) value).get());
		}
//		LOG.info("masterAggregator[ "+index +" ]的值getTimesAggregated() ： "+((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[index]).getTimesAggregated());
	}

	@SuppressWarnings("unchecked")
	private Aggregator<M, Vertex<V, E, M>> getNewAggregator(String clsName) {
		try {
			return (Aggregator<M, Vertex<V, E, M>>) ReflectionUtils
					.newInstance(conf.getClassByName(clsName), conf);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		throw new IllegalArgumentException("Aggregator class " + clsName
				+ " could not be found or instantiated!");
	}

	public final Writable getLastAggregatedValue(int index) {
		return globalAggregatorResult[Preconditions.checkPositionIndex(index,
				globalAggregatorResult.length)];
	}

	public final IntWritable getNumLastAggregatedVertices(int index) {
		return globalAggregatorIncrement[Preconditions.checkPositionIndex(
				index, globalAggregatorIncrement.length)];
	}
}
