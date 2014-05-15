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
package org.apache.hama.bsp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.Counters.Counter;
import org.apache.hama.bsp.ft.AsyncRcvdMsgCheckpointImpl;
import org.apache.hama.bsp.ft.BSPFaultTolerantService;
import org.apache.hama.bsp.ft.FaultTolerantPeerService;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.MessageManagerFactory;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.sync.PeerSyncClient;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.KeyValuePair;

/**
 * This class represents a BSP peer.
 */
public final class BSPPeerImpl<K1, V1, K2, V2, M extends Writable> implements
		BSPPeer<K1, V1, K2, V2, M> {

	private static final Log LOG = LogFactory.getLog(BSPPeerImpl.class);

	public static enum PeerCounter {
		COMPRESSED_MESSAGES, SUPERSTEP_SUM, TASK_INPUT_RECORDS, TASK_OUTPUT_RECORDS, IO_BYTES_READ, MESSAGE_BYTES_TRANSFERED, MESSAGE_BYTES_RECEIVED, TOTAL_MESSAGES_SENT, TOTAL_MESSAGES_RECEIVED, COMPRESSED_BYTES_SENT, COMPRESSED_BYTES_RECEIVED, TIME_IN_SYNC_MS
	}

	private final Configuration conf;
	private final FileSystem fs;
	private BSPJob bspJob;

	private TaskStatus currentTaskStatus;

	private TaskAttemptID taskId;
	private BSPPeerProtocol umbilical;

	private String[] allPeers;

	// SYNC
	private PeerSyncClient syncClient;
	private MessageManager<M> messenger;

	// IO
	private int partition;
	private String splitClass;
	private BytesWritable split;
	private OutputCollector<K2, V2> collector;
	private RecordReader<K1, V1> in;
	private RecordWriter<K2, V2> outWriter;
	private final KeyValuePair<K1, V1> cachedPair = new KeyValuePair<K1, V1>();

	private InetSocketAddress peerAddress;

	private Counters counters;
	private Combiner<M> combiner;

	private FaultTolerantPeerService<M> faultToleranceService;

	private long splitSize = 0L;

	/**
	 * Protected default constructor for LocalBSPRunner.
	 */
	protected BSPPeerImpl() {
		conf = null;
		fs = null;
	}

	/**
	 * For unit test.
	 * 
	 * @param conf
	 *            is the configuration file.
	 * @param dfs
	 *            is the Hadoop FileSystem.
	 */
	protected BSPPeerImpl(final Configuration conf, FileSystem dfs) {
		this.conf = conf;
		this.fs = dfs;
	}

	/**
	 * For unit test.
	 * 
	 * @param conf
	 *            is the configuration file.
	 * @param dfs
	 *            is the Hadoop FileSystem.
	 * @param counters
	 *            is the counters from outside.
	 */
	public BSPPeerImpl(final Configuration conf, FileSystem dfs,
			Counters counters) {
		this(conf, dfs);
		this.counters = counters;
	}

	public BSPPeerImpl(BSPJob job, Configuration conf, TaskAttemptID taskId,
			BSPPeerProtocol umbilical, int partition, String splitClass,
			BytesWritable split, Counters counters) throws Exception {
		this(job, conf, taskId, umbilical, partition, splitClass, split,
				counters, -1, TaskStatus.State.RUNNING);
	}

	/**
	 * BSPPeer Constructor.
	 * 
	 * BSPPeer acts on behalf of clients performing bsp() tasks.
	 * 
	 * @param conf
	 *            is the configuration file containing bsp peer host, port, etc.
	 * @param umbilical
	 *            is the bsp protocol used to contact its parent process.
	 * @param taskId
	 *            is the id that current process holds.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public BSPPeerImpl(BSPJob job, Configuration conf, TaskAttemptID taskId,
			BSPPeerProtocol umbilical, int partition, String splitClass,
			BytesWritable split, Counters counters, long superstep,
			TaskStatus.State state) throws Exception {
		this.conf = conf;
		this.taskId = taskId;
		this.umbilical = umbilical;
		this.bspJob = job;
		// IO
		this.partition = partition;
		this.splitClass = splitClass;
		this.split = split;
		this.counters = counters;

		this.fs = FileSystem.get(conf);

		String bindAddress = conf.get(Constants.PEER_HOST,
				Constants.DEFAULT_PEER_HOST);
//		LOG.info("bindAddress : "+bindAddress);
		int bindPort = conf.getInt(Constants.PEER_PORT,
				Constants.DEFAULT_PEER_PORT);
//		LOG.info("bindPort : "+bindPort);
		peerAddress = new InetSocketAddress(bindAddress, bindPort);
//		LOG.info("peerAddress (类型 InetSocketAddress): "+peerAddress);
		
		initializeIO();
		
//		LOG.debug("***** 初始化同步服务－ 开始...");
		initializeSyncService(superstep, state);
//		LOG.debug("***** 初始化同步服务－ 完成...");
		
		TaskStatus.Phase phase = TaskStatus.Phase.STARTING;
		String stateString = "running";
		if (state == TaskStatus.State.RECOVERING) {
			phase = TaskStatus.Phase.RECOVERING;
			stateString = "recovering";
		}

		setCurrentTaskStatus(new TaskStatus(taskId.getJobID(), taskId, 1.0f,
				state, stateString, peerAddress.getHostName(), phase, counters));

//		LOG.debug("***** 消息初始化－ 开始...");
		initilizeMessaging();
//		LOG.debug("***** 消息初始化－ 完成...");
		if (LOG.isDebugEnabled()) {
//			LOG.debug("Initialized Messaging service.");
		}

		final String combinerName = conf.get("bsp.combiner.class");
		//结果 :bsp.combiner.class =  null
//		LOG.info("bsp.combiner.class =  "+combinerName);
		
		if (combinerName != null) {
			combiner = (Combiner<M>) ReflectionUtils.newInstance(
					conf.getClassByName(combinerName), conf);
		}

		// 配置文件中，默认是关闭的,默认值为 false
		if (conf.getBoolean(Constants.FAULT_TOLERANCE_FLAG, false)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Fault tolerance enabled.");
			}
			if (superstep > 0)
				conf.setInt("attempt.superstep", (int) superstep);
			Class<?> ftClass = conf.getClass(Constants.FAULT_TOLERANCE_CLASS,
					AsyncRcvdMsgCheckpointImpl.class,
					BSPFaultTolerantService.class);
			if (ftClass != null) {
				if (superstep > 0) {
					counters.incrCounter(PeerCounter.SUPERSTEP_SUM, superstep);
				}

				this.faultToleranceService = ((BSPFaultTolerantService<M>) ReflectionUtils
						.newInstance(ftClass, null))
						.constructPeerFaultTolerance(job, this, syncClient,
								peerAddress, this.taskId, superstep, conf,
								messenger);
				TaskStatus.State newState = this.faultToleranceService
						.onPeerInitialized(state);

				if (state == TaskStatus.State.RECOVERING) {
					if (newState == TaskStatus.State.RUNNING) {
						phase = TaskStatus.Phase.STARTING;
						stateString = "running";
						state = newState;
					}

					setCurrentTaskStatus(new TaskStatus(taskId.getJobID(),
							taskId, 1.0f, state, stateString,
							peerAddress.getHostName(), phase, counters));
					if (LOG.isDebugEnabled()) {
						LOG.debug("State after FT service initialization - "
								+ newState.toString());
					}

				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Initialized fault tolerance service");
				}
			}
		}

		doFirstSync(superstep);

		if (LOG.isDebugEnabled()) {
			LOG.info(new StringBuffer("BSP Peer successfully initialized for ")
					.append(this.taskId.toString()).append(" ")
					.append(superstep).toString());
		}
	}

	/**
	 * Transfers DistributedCache files into the local cache files. Also creates
	 * symbolic links for URIs specified with a fragment if
	 * DistributedCache.getSymlinks() is true.
	 * 
	 * @throws IOException
	 *             If a DistributedCache file cannot be found.
	 */
	public final void moveCacheFiles() throws IOException {
		StringBuilder files = new StringBuilder();
		boolean first = true;
//		LOG.info("DistributedCache.getCacheFiles(conf) ： "+DistributedCache.getCacheFiles(conf));;
		if (DistributedCache.getCacheFiles(conf) != null) {
			for (URI uri : DistributedCache.getCacheFiles(conf)) {
				LOG.info("uri : "+uri);
				if (uri != null) {
					if (!first) {
						files.append(",");
					}
					if (null != uri.getFragment()
							&& DistributedCache.getSymlink(conf)) {

						FileUtil.symLink(uri.getPath(), uri.getFragment());
						files.append(uri.getFragment()).append(",");
					}
					FileSystem hdfs = FileSystem.get(conf);
					Path pathSrc = new Path(uri.getPath());
					if (hdfs.exists(pathSrc)) {
						LocalFileSystem local = FileSystem.getLocal(conf);
						Path pathDst = new Path(local.getWorkingDirectory(),
								pathSrc.getName());
						hdfs.copyToLocalFile(pathSrc, pathDst);
						files.append(pathDst.toUri().getPath());
					}
					first = false;
				}
			}
		}
		if (files.length() > 0) {
			DistributedCache.addLocalFiles(conf, files.toString());
		}
	}

	/**
	 * job.getConfiguration(), (FileSplit) split);
	 * 利用 bspJob和split创建 RecordReader对象 this.in 
	 * 表面返回的是TrackedRecordReader，而其里面包裹的是 LineRecordReader对象。
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public final void initInput() throws IOException {
		InputSplit inputSplit = null;
		// reinstantiate the split
		try {
		//	LOG.info("splitClass : "+splitClass);
			if (splitClass != null) {
				inputSplit = (InputSplit) ReflectionUtils.newInstance(
						getConfiguration().getClassByName(splitClass),
						getConfiguration());
//				LOG.info("创建 inputSplit 对象成功!");
			}
		} catch (ClassNotFoundException exp) {
//			LOG.info("创建 inputSplit 对象失败!");
			IOException wrap = new IOException("Split class " + splitClass
					+ " not found");
			wrap.initCause(exp);
			throw wrap;
		}

		//结果:  inputSplit(刚创建) : FileSplit内容 : filePath = null   start = 0   length = 0   hosts = null
//		LOG.info("inputSplit(刚创建) : "+inputSplit);
		
		if (inputSplit != null) {
			DataInputBuffer splitBuffer = new DataInputBuffer();
			splitBuffer.reset(split.getBytes(), 0, split.getLength());
			inputSplit.readFields(splitBuffer);
			
			//结果: inputSplit(用split赋值后) : FileSplit内容 : filePath = hdfs://hadoopx:9000/user/root/regular/data.txt   start = 0   length = 93   hosts = null
//			LOG.info("inputSplit(用split赋值后) : "+inputSplit);
			
//			LOG.info("in : "+in);	//in为null
			if (in != null) {
				in.close();
			}
			
			/**
			 * 下面的 bspJob 由 GroomServer.BSPPeerChild类中的main()方法创建，定义语句如下 ：
			 *  BSPJob job = new BSPJob(task.getJobID(), task.getJobFile());
			 *  其中两参数如下（举例）：
			 *    1. JobID : job_201304222340_0014
			 *    2. JobFile : /home/hadoop/hadooptmp/bsp/local/groomServer/attempt_201304222340_0014_000000_0/job.xml
			 *	
			 *  创建 BSPeerImpl实例的时候，把上面的job传递给 下面的 bspJob
			 */
			
			//结果 ： org.apache.hama.bsp.TextInputFormat@3cad7c （用户设置的）
			// 利用Java反射机制查来创建TextInputFormat 对象
//			LOG.info("bspJob.getInputFormat() : "+ bspJob.getInputFormat());
			
			/**
			 * 结果： org.apache.hama.bsp.LineRecordReader@1644c9 分析：
			 * TextInputFormat类中的定义如下：
			 * 
			 *  @Override 
			 *  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, 
			 *  	BSPJob job) throws IOException { 
			 *  	return new LineRecordReader(job.getConfiguration(), (FileSplit)split); 
			 *  }
			 *  其本质上创建的是 LineRecordReader对象.
			 * 
			 */
//			LOG.info("bspJob.getInputFormat().getRecordReader(inputSplit, bspJob) : "
//					+ bspJob.getInputFormat().getRecordReader(inputSplit,bspJob));
			
			/**
			 * TrackedRecordReader类和LineRecordReader类都实现了 RecordReader接口
			 * 创建完LineRecordReader对象后，存储到 TrackedRecordReader类中的rawIn（定义：RecordReader<K, V> rawIn;）变量。
			 * 
			 * 目前感觉：TrackedRecordReader是在外面的一层封装.
			 */
			in = new TrackedRecordReader<K1, V1>(bspJob.getInputFormat()
					.getRecordReader(inputSplit, bspJob),
					getCounter(BSPPeerImpl.PeerCounter.TASK_INPUT_RECORDS),
					getCounter(BSPPeerImpl.PeerCounter.IO_BYTES_READ));
			
			//结果： org.apache.hama.bsp.TrackedRecordReader@25a091
//			LOG.info("in : "+in);
			this.splitSize = inputSplit.getLength(); // 构造函数中未进行初始化
		}
	}

	/**
	 * @return the size of assigned split
	 */
	@Override
	public long getSplitSize() {
		return splitSize;
	}

	/**
	 * @return the conf
	 */
	public Configuration getConf() {
		return conf;
	}

	/**
	 * @return the position in the input stream.
	 */
	@Override
	public long getPos() throws IOException {
		return in.getPos();
	}

	public final void initilizeMessaging() throws ClassNotFoundException {
		messenger = MessageManagerFactory.getMessageManager(conf);
		//创建messenger对象（定义类型 MessageManager<M>）：
		// org.apache.hama.bsp.message.HadoopMessageManagerImpl@1b0a981
//		LOG.debug("创建messenger对象（定义类型 MessageManager<M>）： "+messenger);
		messenger.init(taskId, this, conf, peerAddress);
	}

	public final void initializeSyncService(long superstep,
			TaskStatus.State state) throws Exception {

		// 定义： private PeerSyncClient syncClient;
		syncClient = SyncServiceFactory.getPeerSyncClient(conf);
		//结果：  syncClient : org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl@fc6b62
//		LOG.debug("syncClient : "+syncClient);
		syncClient.init(conf, taskId.getJobID(), taskId);
		syncClient.register(taskId.getJobID(), taskId,
				peerAddress.getHostName(), peerAddress.getPort());
	}

	private void doFirstSync(long superstep) throws SyncException {
		if (superstep > 0)
			--superstep;
		syncClient.enterBarrier(taskId.getJobID(), taskId, superstep);
		syncClient.leaveBarrier(taskId.getJobID(), taskId, superstep);
	}

	@SuppressWarnings("unchecked")
	public final void initializeIO() throws Exception {

		initInput(); // 初始化 this.in 对象
		
		String outdir = null;
		if (conf.get("bsp.output.dir") != null) {
			//结果：用户写的输出目录， bsp.output.dir : regularOutput  （例）
//			LOG.info("bsp.output.dir : "+ conf.get("bsp.output.dir"));
			
			Path outputDir = new Path(conf.get("bsp.output.dir", "tmp-"
					+ System.currentTimeMillis()),
					Task.getOutputName(partition));
			outdir = outputDir.makeQualified(fs).toString();
			
			//结果： outdir : hdfs://hadoopx:9000/user/root/regularOutput/part-00000
			LOG.info("outdir : "+outdir);
		}
		
		//结果： org.apache.hama.bsp.TextOutputFormat@13c9bc ，用户可配置
//		LOG.info("bspJob.getOutputFormat() : " +bspJob.getOutputFormat());
		outWriter = bspJob.getOutputFormat()
				.getRecordWriter(fs, bspJob, outdir);
		
		//结果： outWriter : org.apache.hama.bsp.TextOutputFormat$LineRecordWriter@a080bf
//		LOG.info("outWriter : "+outWriter);
		
		final RecordWriter<K2, V2> finalOut = outWriter;	//匿名内部类中的参数必须是 final的

		//匿名内部类
		collector = new OutputCollector<K2, V2>() {
			@Override
			public void collect(K2 key, V2 value) throws IOException {
				finalOut.write(key, value);
			}
		};

		try {
			//如果没有使用分布式缓存的话，不用考虑此处
			moveCacheFiles();
		} catch (Exception e) {
			LOG.error(e);
		}

	}

	@Override
	public final M getCurrentMessage() throws IOException {
		return messenger.getCurrentMessage();
	}

	@Override
	public final void send(String peerName, M msg) throws IOException {
		//类型为：org.apache.hama.bsp.message.HadoopMessageManagerImpl
		//LOG.info("messenger的类型 : "+messenger);
		//LOG.info("发送信息......");
		messenger.send(peerName, msg);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hama.bsp.BSPPeerInterface#sync()
	 */
	@Override
	public final void sync() throws IOException, SyncException,
			InterruptedException {

		// normally all messages should been send now, finalizing the send phase
//		LOG.info("messenger对象 : "+messenger);
		messenger.finishSendPhase();
		Iterator<Entry<InetSocketAddress, MessageQueue<M>>> it = messenger
				.getMessageIterator();

		while (it.hasNext()) {
			Entry<InetSocketAddress, MessageQueue<M>> entry = it.next();
			final InetSocketAddress addr = entry.getKey();
			final Iterable<M> messages = entry.getValue();

			final BSPMessageBundle<M> bundle = combineMessages(messages);
			// remove this message during runtime to save a bit of memory
			it.remove();
			try {
				messenger.transfer(addr, bundle);
			} catch (Exception e) {
				LOG.error("Error while sending messages", e);
			}
		}

		if (this.faultToleranceService != null) {
			try {
				this.faultToleranceService.beforeBarrier();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		long startBarrier = System.currentTimeMillis();
		enterBarrier();

		if (this.faultToleranceService != null) {
			try {
				this.faultToleranceService.duringBarrier();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		// Clear outgoing queues.
		messenger.clearOutgoingQueues();

		leaveBarrier();

		incrementCounter(PeerCounter.TIME_IN_SYNC_MS,
				(System.currentTimeMillis() - startBarrier));
		incrementCounter(PeerCounter.SUPERSTEP_SUM, 1L);

		currentTaskStatus.setCounters(counters);

		if (this.faultToleranceService != null) {
			try {
				this.faultToleranceService.afterBarrier();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		umbilical.statusUpdate(taskId, currentTaskStatus);

	}

	private final BSPMessageBundle<M> combineMessages(Iterable<M> messages) {
		BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();
		if (combiner != null) {
			bundle.addMessage(combiner.combine(messages));
		} else {
			for (M message : messages) {
				bundle.addMessage(message);
			}
		}
		return bundle;
	}

	protected final void enterBarrier() throws SyncException {
		syncClient.enterBarrier(taskId.getJobID(), taskId,
				currentTaskStatus.getSuperstepCount());
	}

	protected final void leaveBarrier() throws SyncException {
		syncClient.leaveBarrier(taskId.getJobID(), taskId,
				currentTaskStatus.getSuperstepCount());
	}

	/**
	 * Delete files from the local cache
	 * 
	 * @throws IOException
	 *             If a DistributedCache file cannot be found.
	 */
	public void deleteLocalFiles() throws IOException {
		if (DistributedCache.getLocalCacheFiles(conf) != null) {
			for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
				if (path != null) {
					LocalFileSystem local = FileSystem.getLocal(conf);
					if (local.exists(path)) {
						local.delete(path, true); // recursive true
					}
				}
			}
		}
		DistributedCache.setLocalFiles(conf, "");
	}

	public final void close() {
		// there are many catches, because we want to close always every
		// component
		// even if the one before failed.
		if (in != null) {
			try {
				in.close();
			} catch (Exception e) {
				LOG.error(e);
			}
		}
		if (outWriter != null) {
			try {
				outWriter.close();
			} catch (Exception e) {
				LOG.error(e);
			}
		}
		this.clear();
		try {
			syncClient.close();
		} catch (Exception e) {
			LOG.error(e);
		}
		try {
			messenger.close();
		} catch (Exception e) {
			LOG.error(e);
		}
		// Delete files from the local cache
		try {
			deleteLocalFiles();
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	@Override
	public final void clear() {
		messenger.clearOutgoingQueues();
	}

	/**
	 * @return the string as host:port of this Peer
	 */
	@Override
	public final String getPeerName() {
		return peerAddress.getHostName() + ":" + peerAddress.getPort();
	}

	@Override
	public final String[] getAllPeerNames() {
		initPeerNames();
		return allPeers;
	}

	@Override
	public final String getPeerName(int index) {
		initPeerNames();
		return allPeers[index];
	}

	@Override
	public int getPeerIndex() {
		return this.taskId.getTaskID().getId();
	}

	@Override
	public final int getNumPeers() {
		initPeerNames();
		return allPeers.length;
	}

	private final void initPeerNames() {
		if (allPeers == null) {
			allPeers = syncClient.getAllPeerNames(taskId);
		}
	}

	/**
	 * @return the number of messages
	 */
	@Override
	public final int getNumCurrentMessages() {
		return messenger.getNumCurrentMessages();
	}

	/**
	 * Sets the current status
	 * 
	 * @param currentTaskStatus
	 *            the new task status to set
	 */
	public final void setCurrentTaskStatus(TaskStatus currentTaskStatus) {
		this.currentTaskStatus = currentTaskStatus;
	}

	/**
	 * @return the count of current super-step
	 */
	@Override
	public final long getSuperstepCount() {
		return currentTaskStatus.getSuperstepCount();
	}

	/**
	 * Gets the job configuration.
	 * 
	 * @return the conf
	 */
	@Override
	public final Configuration getConfiguration() {
		return conf;
	}

	/*
	 * IO STUFF
	 */

	@Override
	public final void write(K2 key, V2 value) throws IOException {
		incrementCounter(PeerCounter.TASK_OUTPUT_RECORDS, 1);
		collector.collect(key, value);
	}

	@Override
	public final boolean readNext(K1 key, V1 value) throws IOException {
		return in.next(key, value);
	}

	@Override
	public final KeyValuePair<K1, V1> readNext() throws IOException {
		K1 k = in.createKey();
		V1 v = in.createValue();
		if (in.next(k, v)) {
			cachedPair.clear();
			cachedPair.setKey(k);
			cachedPair.setValue(v);
			return cachedPair;
		} else {
			return null;
		}
	}

	@Override
	public final void reopenInput() throws IOException {
		initInput();
	}

	@Override
	public final Counter getCounter(Enum<?> name) {
		return counters == null ? null : counters.findCounter(name);
	}

	@Override
	public final Counter getCounter(String group, String name) {
		Counters.Counter counter = null;
		if (counters != null) {
			counter = counters.findCounter(group, name);
		}
		return counter;
	}

	public Counters getCounters() {
		return counters;
	}

	@Override
	public final void incrementCounter(Enum<?> key, long amount) {
		if (counters != null) {
			counters.incrCounter(key, amount);
		}
	}

	@Override
	public final void incrementCounter(String group, String counter, long amount) {
		if (counters != null) {
			counters.incrCounter(group, counter, amount);
		}
	}

	@Override
	public TaskAttemptID getTaskId() {
		return taskId;
	}

}
