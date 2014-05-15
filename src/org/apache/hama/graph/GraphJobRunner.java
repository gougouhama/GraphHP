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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

/**
 * Fully generic graph job runner.
 * 
 * @param <V>
 *            the id type of a vertex.
 * @param <E>
 *            the value type of an edge.
 * @param <M>
 *            the value type of a vertex.
 */
/**
 * @author root
 *
 * @param <V>
 * @param <E>
 * @param <M>
 */
public final class GraphJobRunner<V extends Writable, E extends Writable, M extends Writable>
		extends BSP<Writable, Writable, Writable, Writable, GraphJobMessage> {

	public static enum GraphJobCounter {
		MULTISTEP_PARTITIONING, ITERATIONS, INPUT_VERTICES,BOUNDARY_VERTICES, 
		AGGREGATE_VERTICES,JOB_START_TIME,TOTAL_MESSAGES_SENT,TOTAL_MESSAGES_SENT_COMBINER,
		JOB_RUN_TIME_COMPUTE
	}

	private static final Log LOG = LogFactory.getLog(GraphJobRunner.class);

	// make sure that these values don't collide with the vertex names
	public static final String S_FLAG_MESSAGE_COUNTS = "hama.0";
	public static final String S_FLAG_AGGREGATOR_VALUE = "hama.1";
	public static final String S_FLAG_AGGREGATOR_INCREMENT = "hama.2";
	public static final Text FLAG_MESSAGE_COUNTS = new Text(
			S_FLAG_MESSAGE_COUNTS);
	
	public static final String MESSAGE_COMBINER_CLASS = "hama.vertex.message.combiner.class";
	public static final String GRAPH_REPAIR = "hama.graph.repair";

	private Configuration conf;
	private Combiner<M> combiner;
	private Partitioner<V, M> partitioner;

	private Map<V,Vertex<V, E, M>> verticesMap = new HashMap<V,Vertex<V, E, M>>();
	
	private boolean updated = true;
	private int globalUpdateCounts = 0;

	private long numberVertices = 0;
	private int maxIteration = -1;
	private long iteration;

	private Class<V> vertexIdClass;
	private Class<M> vertexValueClass;
	private Class<E> edgeValueClass;
	private Class<Vertex<V, E, M>> vertexClass;

	private AggregationRunner<V, E, M> aggregationRunner;

	private BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer;
	
	private Map<V,List<M>> localMessages;	
	private Map<V,List<M>> tempLocalMessages;	
	private Map<V,List<M>> remoteMessages;	
	private Map<V,List<M>> boundaryMessages;
	private Map<V,List<M>> tempBoundaryMessages;
	
	private int peerBoundaryVerticesNum = 0;
	private int totalBoundaryVerticesNum = 0;
	private long jobStartTime = 0;
	private boolean participateLocalphase=false;

	@Override
	public final void setup(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException, SyncException, InterruptedException {
		
		setupFields(peer);
		
		loadVertices(peer);
		
		countGlobalVertexCount(peer);
		// Record the start time on the master peer
		if (isMasterTask(peer)) {
			jobStartTime = System.currentTimeMillis();
			peer.getCounter(GraphJobCounter.JOB_START_TIME).increment(
					jobStartTime);
		}
		doInitialSuperstep(peer);
	}

	@Override
	public final void bsp(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException, SyncException, InterruptedException {
		
	    // we do supersteps while we still have updates and have not reached our maximum iterations yet
		while (updated && !((maxIteration > 0) && iteration > maxIteration)) {
			// reset the global update counter from our master in every superstep
			globalUpdateCounts = 0;
			int activeVertices=0;
			
			peer.sync();
			boundaryMessages=parseMessages(peer);
			tempBoundaryMessages=new HashMap<V,List<M>>();
			
			if(updated) {
				doMasterUpdates(peer); // master needs to update
			}
			
			// if aggregators say we don't have updates anymore, break
			if (!aggregationRunner.receiveAggregatedValues(peer, iteration)) {
				break;
			}
			
			if(updated) {
				activeVertices=doGlobalSuperstep(boundaryMessages,peer);
				while(tempLocalMessages.size()!=0 || activeVertices!=0) {
					localMessages=tempLocalMessages;
					tempLocalMessages=new HashMap<V, List<M>>();
					activeVertices=doPseudoSuperstep(localMessages, peer);
				}

				// After inner iteration completed,send messages between peers
				peerCommunication();	
				// send Inner iteration to Master Peer
				aggregationRunner.sendAggregatorValues(peer, remoteMessages.size()+tempBoundaryMessages.size());
				remoteMessages.clear();
				
				iteration++;
				if (isMasterTask(peer)) {
					peer.getCounter(GraphJobCounter.ITERATIONS).increment(1);
				}
			}
		} 
	}

	/**
	 * Just write <ID as Writable, Value as Writable> pair as a result. Note
	 * that this will also be executed when failure happened.
	 */
	@Override
	public final void cleanup(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException {
		for(Entry<V,Vertex<V,E,M>> entry:verticesMap.entrySet()) {
			peer.write(entry.getKey(), entry.getValue().getValue());
		}
	}

	
	/**
	 * After internal iterations completed, the peer send messages to other peers.
	 * 
	 * localFinalMsgs where messages are stored in a message sent to the other Peer,
	 * In the internal iteration, is closure down.
	 * 
	 * @throws IOException 
	 */
	public void peerCommunication() throws IOException {
		for (Entry<V, List<M>> entry : remoteMessages.entrySet()) {
			V key = entry.getKey();
			List<M> valueList = entry.getValue();

			// Using User-defined combiner, reducing the amount of messages transmitted
			if (combiner != null) {
				M combined = combiner.combine(valueList);
				valueList = new ArrayList<M>();
				valueList.add(combined);
				getPeer().getCounter(GraphJobRunner.GraphJobCounter.TOTAL_MESSAGES_SENT_COMBINER).increment(1);
			}

			int peerIndex = getPartitioner().getPartition(key,
					null, this.getPeer().getNumPeers());
			String destPeer = this.getPeer().getAllPeerNames()[peerIndex];
			// send message
			for (M value : valueList) {
				this.getPeer().send(destPeer, new GraphJobMessage(key, value));
			}
		}
	}

	/**
	 * The master task is going to check the number of updated vertices and do
	 * master aggregation. In case of no aggregators defined, we save a sync by
	 * reading multiple typed messages.
	 */
	private void doMasterUpdates(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException {
		if (isMasterTask(peer) && iteration >=1) {
			MapWritable updatedCnt = new MapWritable();
			//LOG.debug("GlobalUpdateCounts Value : "+globalUpdateCounts);
			
			if (globalUpdateCounts == 0) {
				updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(
						Integer.MIN_VALUE));
				peer.getCounter(GraphJobCounter.JOB_RUN_TIME_COMPUTE).increment(
						System.currentTimeMillis()-this.jobStartTime);
			} else {
				aggregationRunner.doMasterAggregation(updatedCnt);
			}
			// send the updates from the mater tasks back to the slaves
			if(updatedCnt.size()>=1) {
				for (String peerName : peer.getAllPeerNames()) {
					peer.send(peerName, new GraphJobMessage(updatedCnt));
				}
			}
		}
	}


	private int doGlobalSuperstep(
			Map<V, List<M>> boundaryMsgs,
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException {
		
		int activeBoundaryVertices = 0;
		
		for (V vertexID : boundaryMsgs.keySet()) {
			Vertex<V, E, M> vertex = verticesMap.get(vertexID);
			List<M> msgs = boundaryMsgs.get(vertexID);

			M lastValue = vertex.getValue();
			vertex.compute(msgs.iterator());
			if (!vertex.isHalted()) {
				activeBoundaryVertices++;
			}
			aggregationRunner.aggregateVertex(lastValue, vertex);
		}
		boundaryMsgs.clear();
		return activeBoundaryVertices;
	}
	

	private int doPseudoSuperstep(
			Map<V, List<M>> localMsgs,
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException {
		
		int activeLocalVertices = 0;

		Iterator<V> it=localMsgs.keySet().iterator(); 
		while(it.hasNext()){
			V vertexID=it.next();
			Vertex<V,E,M> vertex=verticesMap.get(vertexID);
			List<M> msgs = localMsgs.get(vertexID);
			
			M lastValue = vertex.getValue();
			vertex.compute(msgs.iterator());
			if (!vertex.isHalted()) {
				activeLocalVertices++;
			}
			aggregationRunner.aggregateVertex(lastValue, vertex);
			it.remove();
		}
		return activeLocalVertices;
	}

	/**
	 * Seed the vertices first with their own values in compute. This is the
	 * first superstep after the vertices have been loaded.
	 */
	private void doInitialSuperstep(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException {
		int activeVertices = 0;
		for(Entry<V,Vertex<V,E,M>> entry:verticesMap.entrySet()) {
			Vertex<V,E,M> vertex=entry.getValue();
			List<M> singletonList = Collections.singletonList(vertex.getValue());
			M lastValue = vertex.getValue();
			vertex.compute(singletonList.iterator());
			if(!vertex.isHalted()) {
				activeVertices++;
			}
			aggregationRunner.aggregateVertex(lastValue, vertex);
		}
		peerCommunication();	
		// send Inner iteration to Maste Peer
		remoteMessages.clear();
		aggregationRunner.sendAggregatorValues(peer,(activeVertices+1));
		
		iteration++;	
	}

	@SuppressWarnings("unchecked")
	private void setupFields(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
		this.peer = peer;
		this.conf = peer.getConfiguration();
		maxIteration = peer.getConfiguration().getInt(
				"hama.graph.max.iteration", -1);
		
		participateLocalphase = peer.getConfiguration().getBoolean(
				"hama.graph.boundary.vertex.participate.localphase", false);
		LOG.info("participateLocalphase: "+participateLocalphase);
		
		vertexIdClass = (Class<V>) conf.getClass(GraphJob.VERTEX_ID_CLASS_ATTR,
				Text.class, Writable.class);
		vertexValueClass = (Class<M>) conf.getClass(
				GraphJob.VERTEX_VALUE_CLASS_ATTR, IntWritable.class,
				Writable.class);
		edgeValueClass = (Class<E>) conf.getClass(
				GraphJob.VERTEX_EDGE_VALUE_CLASS_ATTR, IntWritable.class,
				Writable.class);
		vertexClass = (Class<Vertex<V, E, M>>) conf.getClass(
				"hama.graph.vertex.class", Vertex.class);

		// set the classes statically, so we can save memory per message
		GraphJobMessage.VERTEX_ID_CLASS = vertexIdClass;
		GraphJobMessage.VERTEX_VALUE_CLASS = vertexValueClass;
		GraphJobMessage.VERTEX_CLASS = vertexClass;
		GraphJobMessage.EDGE_VALUE_CLASS = edgeValueClass;

		partitioner = (Partitioner<V, M>) ReflectionUtils.newInstance(
				conf.getClass("bsp.input.partitioner.class",
						HashPartitioner.class), conf);

		if (!conf.getClass(MESSAGE_COMBINER_CLASS, Combiner.class).equals(
				Combiner.class)) {
			//LOG.debug("Combiner class: " + conf.get(MESSAGE_COMBINER_CLASS));
			combiner = (Combiner<M>) ReflectionUtils.newInstance(conf.getClass(
							"hama.vertex.message.combiner.class",Combiner.class), conf);
		}

		aggregationRunner = new AggregationRunner<V, E, M>();
		aggregationRunner.setupAggregators(peer);
		
		tempLocalMessages = new HashMap<V, List<M>>();
		localMessages = new HashMap<V, List<M>>();
		remoteMessages = new HashMap<V, List<M>>();
	    tempBoundaryMessages=new HashMap<V, List<M>>();;	
	}

	/**
	 * Loads vertices into memory of each peer. 
	 * TODO this needs to be simplified.
	 */
	@SuppressWarnings("unchecked")
	private void loadVertices(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException, SyncException, InterruptedException {

		/*
		 * Several partitioning constants begin
		 */
		final VertexInputReader<Writable, Writable, V, E, M> reader = (VertexInputReader<Writable, Writable, V, E, M>) ReflectionUtils
				.newInstance(conf.getClass(GraphJob.VERTEX_GRAPH_INPUT_READER,
						VertexInputReader.class), conf);

		final boolean repairNeeded = conf.getBoolean(GRAPH_REPAIR, false);
		
		final boolean runtimePartitioning = conf.getBoolean(
				GraphJob.VERTEX_GRAPH_RUNTIME_PARTIONING, true);
		//LOG.debug("The runtimePartitioning is : "+runtimePartitioning);
		
		final long splitSize = peer.getSplitSize();
		final int partitioningSteps = partitionMultiSteps(peer, splitSize);
		final long interval = splitSize / partitioningSteps;
		final boolean selfReference = conf.getBoolean("hama.graph.self.ref",false);

		/*
		 * Several partitioning constants end
		 */
		Vertex<V, E, M> vertex = newVertexInstance(vertexClass, conf);
		vertex.runner = this;
		long startPos = peer.getPos();
		if (startPos == 0)
			startPos = 1L;

		KeyValuePair<Writable, Writable> next = null;
		int steps = 1;
		while ((next = peer.readNext()) != null) {
			boolean vertexFinished = false;
			try {
				vertexFinished = reader.parseVertex(next.getKey(),next.getValue(), vertex);
			} catch (Exception e) {
				throw new IOException("exception occured during parsing vertex!"+ e.toString());
			}
			if (!vertexFinished) {
				continue;
			}
			if (vertex.getEdges() == null) {
				if (selfReference) {
					vertex.setEdges(Collections.singletonList(new Edge<V, E>(
							vertex.getVertexID(), null)));
				} else {
					vertex.setEdges(Collections.EMPTY_LIST);
				}
			}
			if (selfReference) {
				vertex.addEdge(new Edge<V, E>(vertex.getVertexID(), null));
			}
			if (runtimePartitioning) {
				int partition = partitioner.getPartition(vertex.getVertexID(),
						vertex.getValue(), peer.getNumPeers());
				peer.send(peer.getPeerName(partition), new GraphJobMessage(
						vertex));
			} else {
				vertex.setup(conf);	
				verticesMap.put(vertex.getVertexID(), vertex);
			}
			vertex = newVertexInstance(vertexClass, conf);
			vertex.runner = this;

			if (runtimePartitioning) {
				if (steps < partitioningSteps
						&& (peer.getPos() - startPos) >= interval) {
					peer.sync();
					steps++;
					GraphJobMessage msg = null;
					while ((msg = peer.getCurrentMessage()) != null) {
						Vertex<V, E, M> messagedVertex = (Vertex<V, E, M>) msg.getVertex();
						messagedVertex.runner = this;
						messagedVertex.setup(conf);
						verticesMap.put(messagedVertex.getVertexID(), messagedVertex);
					}
					startPos = peer.getPos();
				}
			}
		}	
		
		//load the partitioningStep block data
		if (runtimePartitioning) {
			peer.sync();

			GraphJobMessage msg = null;
			while ((msg = peer.getCurrentMessage()) != null) {
				Vertex<V, E, M> messagedVertex = (Vertex<V, E, M>) msg.getVertex();
				messagedVertex.runner = this;

				messagedVertex.setup(conf);
				verticesMap.put(messagedVertex.getVertexID(), messagedVertex);
			}
		}

		if (repairNeeded) {
			//LOG.debug("Starting repair of this graph!");
			repair(peer, partitioningSteps, selfReference);
		}

		LOG.info("number of vertices on this peer : " + verticesMap.size());

		doBoundaryVertices(peer,partitioningSteps);
	}

	/**
	 * @return the localMessages
	 */
	public Map<V, List<M>> getLocalMessages() {
		return localMessages;
	}
	
	/**
	 * @return the tempLocalMessages
	 */
	public Map<V, List<M>> getTempLocalMessages() {
		return tempLocalMessages;
	}

	/**
	 * @return the tempBoundaryMessages
	 */
	public Map<V, List<M>> getTempBoundaryMessages() {
		return tempBoundaryMessages;
	}

	/**
	 * @return the remoteMessages
	 */
	public Map<V, List<M>> getRemoteMessages() {
		return remoteMessages;
	}

	/**
	 * @return the verticesMap
	 */
	public Map<V, Vertex<V, E, M>> getVerticesMap() {
		return verticesMap;
	}

	/**
	 * @return the participateLocalphase
	 */
	public boolean isParticipateLocalphase() {
		return participateLocalphase;
	}

	/**
	 * @param participateLocalphase the participateLocalphase to set
	 */
	public void setParticipateLocalphase(boolean participateLocalphase) {
		this.participateLocalphase = participateLocalphase;
	}

	private void repair(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
			int partitioningSteps, boolean selfReference) throws IOException,
			SyncException, InterruptedException {
	}
	
	/**
	 * Compute the boundary vertices. Taking into account the amount of
	 * information transmitted is not large,so do only once synchronization,
	 * 
	 * @param peer
	 * @param partitioningSteps
	 * @throws IOException
	 * @throws SyncException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	private void doBoundaryVertices(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
			int partitioningSteps) throws IOException,
			SyncException, InterruptedException {
		
		int localPeerIndex = getPeer().getPeerIndex(); 
		// for storing the received vertex ID, each Peer will receive duplicate,so use Set to store
		Set<V> recBoundaryVertexIDSet=new HashSet<V>();	
		
		for (Entry<V, Vertex<V, E, M>> entry : verticesMap.entrySet()) {
			Vertex<V, E, M> vertex = entry.getValue();
			for (Edge<V, E> e : vertex.getEdges()) {
				int destPeerIndex = getPartitioner().getPartition(
						e.getDestinationVertexID(), null,
						getPeer().getNumPeers());
				// not on the same peer
				if (destPeerIndex != localPeerIndex) {
					// send the adjacent vertexID to adjacent vertex
					peer.send(vertex.getDestinationPeerName(e),
							new GraphJobMessage(e.getDestinationVertexID()));
				}
			}
		}

		peer.sync();
		GraphJobMessage msg = null;
		while ((msg = peer.getCurrentMessage()) != null) {
			V vertexName = (V)msg.getVertexId();
			recBoundaryVertexIDSet.add(vertexName);	
		}
		
		for(Entry<V,Vertex<V,E,M>> entry:verticesMap.entrySet()) {
			Vertex<V,E,M> vertex=entry.getValue();
			if(recBoundaryVertexIDSet.contains(vertex.getVertexID()) && !vertex.isBoundaryVertex()) {
				vertex.setBoundaryVertex(true);
			}
		}
		
		peerBoundaryVerticesNum=recBoundaryVertexIDSet.size();
		LOG.info("number of boundary vertices on this peer : "+peerBoundaryVerticesNum);
		// send to master peer to calculate the total number of boundary vertices
		peer.send(getMasterTask(peer), new GraphJobMessage(new IntWritable(peerBoundaryVerticesNum),1));
	
		peer.sync();
		// sum all the boundary vertices size
		if (isMasterTask(peer)) {
			GraphJobMessage msgBoundaryVertex = null;
			while ((msgBoundaryVertex = peer.getCurrentMessage()) != null) {
				if (msgBoundaryVertex.isBoundaryVertexSizeMessage()) {
					totalBoundaryVerticesNum += msgBoundaryVertex.getBoundaryVertexSize().get();
				}
			}
			LOG.info("Global Boundary Vertex Count : "+ totalBoundaryVerticesNum);
			peer.getCounter(GraphJobCounter.BOUNDARY_VERTICES).increment(
					totalBoundaryVerticesNum);
		}
	}
	
	
	
	/**
	 * Partitions our vertices through multiple supersteps to save memory.
	 */
	private int partitionMultiSteps(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
			long splitSize) throws IOException, SyncException,
			InterruptedException {
		int multiSteps = 1;

		MapWritable ssize = new MapWritable();
		ssize.put(new IntWritable(peer.getPeerIndex()), new LongWritable(splitSize));
		peer.send(getMasterTask(peer), new GraphJobMessage(ssize));
		ssize = null;
		peer.sync();

		if (isMasterTask(peer)) {
			long maxSplitSize = 0L;
			GraphJobMessage received = null;
			while ((received = peer.getCurrentMessage()) != null) {
				MapWritable x = received.getMap();
				for (Entry<Writable, Writable> e : x.entrySet()) {
					long curr = ((LongWritable) e.getValue()).get();
					if (maxSplitSize < curr) {
						maxSplitSize = curr;
					}
				}
			}
			int steps = (int) (maxSplitSize / conf.getLong(
					"hama.graph.multi.step.partitioning.interval", 20000000)) + 1;

			for (String peerName : peer.getAllPeerNames()) {
				MapWritable temp = new MapWritable();
				temp.put(new Text("max"), new IntWritable(steps));
				peer.send(peerName, new GraphJobMessage(temp));
			}
		}
		peer.sync();

		GraphJobMessage received = peer.getCurrentMessage();
		MapWritable x = received.getMap();
		for (Entry<Writable, Writable> e : x.entrySet()) {
			multiSteps = ((IntWritable) e.getValue()).get();
		}

		if (isMasterTask(peer)) {
			peer.getCounter(GraphJobCounter.MULTISTEP_PARTITIONING).increment(
					multiSteps);
		}

		return multiSteps;
	}
	

	/**
	 * Counts vertices globally by sending the count of vertices in the map to
	 * the other peers.
	 */
	private void countGlobalVertexCount(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException, SyncException, InterruptedException {
		for (String peerName : peer.getAllPeerNames()) {
			peer.send(peerName,
					new GraphJobMessage(new IntWritable(verticesMap.size())));
		}
		peer.sync();
		GraphJobMessage msg = null;
		while ((msg = peer.getCurrentMessage()) != null) {
			if (msg.isVerticesSizeMessage()) {
				numberVertices += msg.getVerticesSize().get();
			}
		}
        LOG.info("Global Vertex Count ： "+ numberVertices);
		if (isMasterTask(peer)) {
			peer.getCounter(GraphJobCounter.INPUT_VERTICES).increment(
					numberVertices);
		}
	}

	/**
	 * Parses the messages in every superstep and does actions according to
	 * flags in the messages.
	 * 
	 * @return a map that contains messages pro vertex.
	 */
	@SuppressWarnings("unchecked")
	private Map<V, List<M>> parseMessages(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
			throws IOException {
		GraphJobMessage msg = null;
		while ((msg = peer.getCurrentMessage()) != null) {
			// either this is a vertex message or a directive that must be read as map
			if (msg.isVertexMessage()) {
				final V vertexID = (V) msg.getVertexId();
				final M value = (M) msg.getVertexValue();
				List<M> msgs = tempBoundaryMessages.get(vertexID);
				if (msgs == null) {
					msgs = new ArrayList<M>();
					tempBoundaryMessages.put(vertexID, msgs);
				}
				msgs.add(value);
			} else if (msg.isMapMessage()) {
				for (Entry<Writable, Writable> e : msg.getMap().entrySet()) {
					Text vertexID = (Text) e.getKey();
					if (FLAG_MESSAGE_COUNTS.equals(vertexID)) {
						if (((IntWritable) e.getValue()).get() == Integer.MIN_VALUE) {
							updated = false;
						}else {
							globalUpdateCounts += ((IntWritable) e.getValue()).get();
						}
					} else if (aggregationRunner.isEnabled()
							&& vertexID.toString().startsWith(S_FLAG_AGGREGATOR_VALUE)) {
						aggregationRunner.masterReadAggregatedValue(vertexID,(M) e.getValue());
					} else if (aggregationRunner.isEnabled()
							&& vertexID.toString().startsWith(S_FLAG_AGGREGATOR_INCREMENT)) {
						aggregationRunner.masterReadAggregatedIncrementalValue(
								vertexID, (M) e.getValue());
					}
				}
			} else {
				throw new UnsupportedOperationException(
						"Unknown message type: " + msg);
			}
		}
		//LOG.debug("globalUpdateCounts的值 ： "+globalUpdateCounts);
		return tempBoundaryMessages;
	}

	/**
	 * @return the number of vertices, globally accumulated.
	 */
	public final long getNumberVertices() {
		return numberVertices;
	}

	/**
	 * @return the current number of iterations.
	 */
	public final long getNumberIterations() {
		return iteration;
	}

	/**
	 * @return the defined number of maximum iterations, -1 if not defined.
	 */
	public final int getMaxIteration() {
		return maxIteration;
	}

	/**
	 * @return the defined partitioner instance.
	 */
	public final Partitioner<V, M> getPartitioner() {
		return partitioner;
	}

	/**
	 * Gets the last aggregated value at the given index. The index is dependend
	 * on how the aggregators were configured during job setup phase.
	 * 
	 * @return the value of the aggregator, or null if none was defined.
	 */
	public final Writable getLastAggregatedValue(int index) {
		return aggregationRunner.getLastAggregatedValue(index);
	}

	/**
	 * Gets the last aggregated number of vertices at the given index. The index
	 * is dependend on how the aggregators were configured during job setup
	 * phase.
	 * 
	 * @return the value of the aggregator, or null if none was defined.
	 */
	public final IntWritable getNumLastAggregatedVertices(int index) {
		return aggregationRunner.getNumLastAggregatedVertices(index);
	}

	/**
	 * @return the peer instance.
	 */
	public final BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> getPeer() {
		return peer;
	}

	/**
	 * Checks if this is a master task. The master task is the first peer in the
	 * peer array.
	 */
	public static boolean isMasterTask(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
		return peer.getPeerName().equals(getMasterTask(peer));
	}

	/**
	 * @return the name of the master peer, the name at the first index of the
	 *         peers.
	 */
	public static String getMasterTask(
			BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
		return peer.getPeerName(0);
	}

	/**
	 * @return a new vertex instance
	 */
	public static <V extends Writable, E extends Writable, M extends Writable> Vertex<V, E, M> newVertexInstance(
			Class<?> vertexClass, Configuration conf) {
		@SuppressWarnings("unchecked")
		Vertex<V, E, M> vertex = (Vertex<V, E, M>) ReflectionUtils.newInstance(
				vertexClass, conf);
		return vertex;
	}
}
