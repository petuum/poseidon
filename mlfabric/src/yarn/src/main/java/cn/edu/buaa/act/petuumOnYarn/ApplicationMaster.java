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

package cn.edu.buaa.act.petuumOnYarn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;

/**
 @author Haoyue Wang {wanghy11@act.buaa.edu.cn}
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	private static long startTime;
	private static long currentTime;

	// Configuration
	private List<Container> allAllocatedContainers;
	private Configuration conf;
	private static String hostfileHDFSPath = "";
	private int numNodes = 1;
	private int startPort = 9999;
	private List<NodeReport> avaliableNodeList;
	// Location of script ( obtained from info set in env )
	// Script path in fs
	private String scriptHDFSPath = "";
	private String petuumHDFSPathPrefix = "petuum/";
	
	
	// Handle to communicate with the Resource Manager
	@SuppressWarnings("rawtypes")
	private AMRMClientAsync amRMClient;

	// In both secure and non-secure modes, this points to the job-submitter.
	private UserGroupInformation appSubmitterUgi;

	// Handle to communicate with the Node Manager
	private NMClientAsync nmClientAsync;
	// Listen to process the response from the Node Manager
	private NMCallbackHandler containerListener;

	// Application Attempt Id ( combination of attemptId and fail count )
	@VisibleForTesting
	protected ApplicationAttemptId appAttemptID;

	// TODO
	// For status update for clients - yet to be implemented
	// Hostname of the container
	private String appMasterHostname = "";
	// Port on which the app master listens for status updates from clients
	private int appMasterRpcPort = -1;
	// Tracking url to which app master publishes info for clients to monitor
	private String appMasterTrackingUrl = "";

	// App Master configuration
	// No. of containers to run the workers
	@VisibleForTesting
	protected int numTotalContainers = 1;
	// Memory to request for the container on which the worker will run
	private int containerMemory = 10;
	// VirtualCores to request for the container on which the worker will
	// run
	private int containerVirtualCores = 1;
	// Priority of the request
	private int requestPriority;

	// Counter for completed containers ( complete denotes successful or failed
	// )
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	// Allocated container count so that we know how many containers has the RM
	// allocated to us
	@VisibleForTesting
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	// Count of failed containers
	private AtomicInteger numFailedContainers = new AtomicInteger();
	// Count of containers already requested from the RM
	// Needed as once requested, we should not request for containers again.
	// Only request for more if the original requirement changes.
	@VisibleForTesting
	protected AtomicInteger numRequestedContainers = new AtomicInteger();

	// Shell command to be executed
	private String shellCommand = "";
	// Args to be passed to the shell command
	private String shellArgs = "";
	// Env variables to be setup for the shell command
	private Map<String, String> shellEnv = new HashMap<String, String>();	

	// Hardcoded path to custom log_properties
	private static final String log4jPath = "log4j.properties";
	private static final String launchfileIdentifier = "launch.py";	
	private static final String hostfileIdentifier = "hostfile";	

	private volatile boolean done;
	
	private ByteBuffer allTokens;

	// Launch threads
	private List<Thread> launchThreads = new ArrayList<Thread>();

	private final String python_prefix = "python";
	/**
	 * @param args
	 *            Command line args
	 */
	public static void main(String[] args) {
		startTime = System.currentTimeMillis();
		boolean result = false;
		try {
			ApplicationMaster appMaster = new ApplicationMaster();
			LOG.info("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			appMaster.run();
			result = appMaster.finish();
		} catch (Throwable t) {
			LOG.fatal("Error running ApplicationMaster", t);
			LogManager.shutdown();
			ExitUtil.terminate(1, t);
		}
		if (result) {
			LOG.info("Application Master completed successfully. exiting");
			System.exit(0);
		} else {
			LOG.info("Application Master failed. exiting");
			System.exit(2);
		}
	}

	/**
	 * Dump out contents of $CWD and the environment to stdout for debugging
	 */
	private void dumpOutDebugInfo() {

		LOG.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			LOG.info("System env: key=" + env.getKey() + ", val="
					+ env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val="
					+ env.getValue());
		}

		BufferedReader buf = null;
		try {
			String lines = Shell.execCommand("ls", "-al");
			buf = new BufferedReader(new StringReader(lines));
			String line = "";
			while ((line = buf.readLine()) != null) {
				LOG.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.cleanup(LOG, buf);
		}
	}

	public ApplicationMaster() {
		// Set up the configuration
		conf = new YarnConfiguration();
	}

	/**
	 * Parse command line options
	 *
	 * @param args
	 *            Command line args
	 * @return Whether init successful and run should be invoked
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean init(String[] args) throws ParseException, IOException {
		Options opts = new Options();
		opts.addOption(
				"script_hdfs_path",
				true,
				"User's launch script path on HDFS");
		opts.addOption("hdfs_path_prefix", true, "petuum dir path prefix on HDFS. default /petuum/");
		opts.addOption("app_attempt_id", true,
				"App Attempt ID. Not to be used unless for testing purposes");
		opts.addOption("shell_env", true,
				"Environment for script. Specified as env_key=env_val pairs");
		opts.addOption("container_memory", true,
				"Amount of memory in MB to be requested to run the Petuum worker");
		opts.addOption("container_vcores", true,
				"Amount of virtual cores to be requested to run the Petuum worker");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("debug", false, "Dump out debug information");

		opts.addOption("help", false, "Print usage");
		opts.addOption("num_nodes", true, "Required number of nodes");
		opts.addOption("start_port", true, "Start port of each machine");

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException(
					"No args specified for application master to initialize");
		}

		// Check whether customer log4j.properties file exists
		if (new File(log4jPath).exists()) {
			try {
				Log4jPropertyHelper.updateLog4jConfiguration(
						ApplicationMaster.class, log4jPath);
			} catch (Exception e) {
				LOG.warn("Can not set up custom log4j properties. " + e);
			}
		}

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}

		Map<String, String> envs = System.getenv();
		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser
						.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException(
						"Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs
					.get(Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name()
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name()
					+ " not set in the environment");
		}

		LOG.info("Application master for app" + ", appId="
				+ appAttemptID.getApplicationId().getId()
				+ ", clustertimestamp="
				+ appAttemptID.getApplicationId().getClusterTimestamp()
				+ ", attemptId=" + appAttemptID.getAttemptId());
		
		if (cliParser.hasOption("shell_env")) {
			String shellEnvs[] = cliParser.getOptionValues("shell_env");
			for (String env : shellEnvs) {
				env = env.trim();
				int index = env.indexOf('=');
				if (index == -1) {
					shellEnv.put(env, "");
					continue;
				}
				String key = env.substring(0, index);
				String val = "";
				if (index < (env.length() - 1)) {
					val = env.substring(index + 1);
				}
				shellEnv.put(key, val);
			}
		}
		containerMemory = Integer.parseInt(cliParser.getOptionValue(
				"container_memory", "1000"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
				"container_vcores", "2"));
		numNodes = Integer.parseInt(cliParser.getOptionValue("num_nodes", "1"));
		startPort = Integer.parseInt(cliParser.getOptionValue("start_port", "9999"));
		petuumHDFSPathPrefix = cliParser.getOptionValue("hdfs_path_prefix", "petuum/");
		scriptHDFSPath = cliParser.getOptionValue("script_hdfs_path", "");
		
		if(scriptHDFSPath.trim().equals("")){
			LOG.fatal("Launch script path is empty!");
			return false;
		}
		if(getAvaliableNodes() == false)
			return false;
		numTotalContainers = numNodes;
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException(
					"Cannot run with no containers");
		}
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority",
				"10"));

		allAllocatedContainers = new ArrayList<Container>();
		allAllocatedContainers = java.util.Collections
				.synchronizedList(allAllocatedContainers);

		return true;
	}
	
	private boolean getAvaliableNodes(){
		List<NodeReport> clusterNodeReports;
		try {
			YarnClient yarnClient = YarnClient.createYarnClient();
			yarnClient.init(conf);
			yarnClient.start();

			clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
			for (NodeReport node : clusterNodeReports) {
				LOG.info("node infos:" + node.getHttpAddress());
			}
			
			avaliableNodeList = new ArrayList<NodeReport>();
			if (numNodes <= clusterNodeReports.size()) {
				for (NodeReport node : clusterNodeReports) {
					if (node.getCapability().getMemory() >= containerMemory
							&& node.getCapability().getVirtualCores() >= containerVirtualCores) {
						avaliableNodeList.add(node);
					}
				}
				if (avaliableNodeList.size() >= numNodes)
					numTotalContainers = numNodes;
				else {
					LOG.error("Resource isn't enough");
					return false;
				}
			} else {
				LOG.error("cluster nodes isn't enough");
				return false;
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace());
			return false;
		}
		return true;
	}

	/**
	 * Helper function to print usage
	 *
	 * @param opts
	 *            Parsed command line options
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	/**
	 * Main run function for the application master
	 *
	 * @throws YarnException
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked" })
	public void run() throws YarnException, IOException {
		LOG.info("Starting ApplicationMaster");

		// Note: Credentials, Token, UserGroupInformation, DataOutputBuffer
		// class
		// are marked as LimitedPrivate
		Credentials credentials = UserGroupInformation.getCurrentUser()
				.getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		LOG.info("Executing with tokens:");
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			LOG.info(token);
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		// Create appSubmitterUgi and add original tokens to it
		String appSubmitterUserName = System
				.getenv(ApplicationConstants.Environment.USER.name());
		appSubmitterUgi = UserGroupInformation
				.createRemoteUser(appSubmitterUserName);
		appSubmitterUgi.addCredentials(credentials);

		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(conf);
		amRMClient.start();

		containerListener = createNMCallbackHandler();
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		// Setup local RPC Server to accept status requests directly from
		// clients
		// TODO need to setup a protocol for client to be able to communicate to
		// the RPC server
		// TODO use the rpc port info to register with the RM for the client to
		// send requests to this app master

		// Register self with ResourceManager
		// This will start heartbeating to the RM
		appMasterHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRMClient
				.registerApplicationMaster(appMasterHostname, appMasterRpcPort,
						appMasterTrackingUrl);
		// Dump out information about cluster capability as seen by the
		// resource manager
		int maxMem = response.getMaximumResourceCapability().getMemory();
		LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

		int maxVCores = response.getMaximumResourceCapability()
				.getVirtualCores();
		LOG.info("Max vcores capabililty of resources in this cluster "
				+ maxVCores);

		// A resource ask cannot exceed the max.
		if (containerMemory > maxMem) {
			LOG.info("Container memory specified above max threshold of cluster."
					+ " Using max value."
					+ ", specified="
					+ containerMemory
					+ ", max=" + maxMem);
			containerMemory = maxMem;
		}

		if (containerVirtualCores > maxVCores) {
			LOG.info("Container virtual cores specified above max threshold of cluster."
					+ " Using max value."
					+ ", specified="
					+ containerVirtualCores + ", max=" + maxVCores);
			containerVirtualCores = maxVCores;
		}

		List<Container> previousAMRunningContainers = response
				.getContainersFromPreviousAttempts();
		LOG.info(appAttemptID + " received "
				+ previousAMRunningContainers.size()
				+ " previous attempts' running containers on AM registration.");
		numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

		int numTotalContainersToRequest = numTotalContainers;
		// Setup ask for containers from RM
		// Send request for containers to RM
		// Until we get our fully allocated quota, we keep on polling RM for
		// containers
		// Keep looping until all the containers are launched and script
		// executed on them ( regardless of success/failure).
		for (int i = 0; i < numTotalContainersToRequest; ++i) {
			ContainerRequest containerAsk = setupContainerAskForRM();
			amRMClient.addContainerRequest(containerAsk);
		}
		numRequestedContainers.set(numTotalContainers);

	}

	@VisibleForTesting
	NMCallbackHandler createNMCallbackHandler() {
		return new NMCallbackHandler(this);
	}

	@VisibleForTesting
	protected boolean finish() {
		// wait for completion.
		while (!done && (numCompletedContainers.get() != numTotalContainers)) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException ex) {
			}
		}

		// Join all launched threads
		// needed for when we time out
		// and we need to release containers
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				LOG.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}

		// When the application completes, it should stop all running containers
		LOG.info("Application completed. Stopping running containers");
		nmClientAsync.stop();

		// When the application completes, it should send a finish application
		// signal to the RM
		LOG.info("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		boolean success = true;
		if (numFailedContainers.get() == 0
				&& numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers
					+ ", completed=" + numCompletedContainers.get()
					+ ", allocated=" + numAllocatedContainers.get()
					+ ", failed=" + numFailedContainers.get();
			LOG.info(appMessage);
			success = false;
		}
		try {
			amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (YarnException ex) {
			LOG.error("Failed to unregister application", ex);
		} catch (IOException e) {
			LOG.error("Failed to unregister application", e);
		}

		amRMClient.stop();

		return success;
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
		@SuppressWarnings("unchecked")
		@Override
		public void onContainersCompleted(
				List<ContainerStatus> completedContainers) {
			LOG.info("Got response from RM for container ask, completedCnt="
					+ completedContainers.size());
			for (ContainerStatus containerStatus : completedContainers) {
				LOG.info(appAttemptID
						+ " got container status for containerID="
						+ containerStatus.getContainerId() + ", state="
						+ containerStatus.getState() + ", exitStatus="
						+ containerStatus.getExitStatus() + ", diagnostics="
						+ containerStatus.getDiagnostics());

				// non complete containers should not be here
				assert (containerStatus.getState() == ContainerState.COMPLETE);

				// increment counters for completed/failed containers
				int exitStatus = containerStatus.getExitStatus();
				if (0 != exitStatus) {
					// container failed
					if (ContainerExitStatus.ABORTED != exitStatus) {
						// script failed
						// counts as completed
						numCompletedContainers.incrementAndGet();
						numFailedContainers.incrementAndGet();
					} else {
						// container was killed by framework, possibly preempted
						// we should re-try as the container was lost for some
						// reason
						numAllocatedContainers.decrementAndGet();
						numRequestedContainers.decrementAndGet();
						// we do not need to release the container as it would
						// be done by the RM
					}
					
				} else {
					// nothing to do
					// container completed successfully
					numCompletedContainers.incrementAndGet();
					LOG.info("Container completed successfully."
							+ ", containerId="
							+ containerStatus.getContainerId());
					LOG.info("Strads application completed, kill all containers");
					numCompletedContainers.set(numAllocatedContainers.intValue());
					done = true;
				}
			}
			
			// ask for more containers if any failed
			int askCount = numTotalContainers - numRequestedContainers.get();
			numRequestedContainers.addAndGet(askCount);

			if (askCount > 0) {
				for (int i = 0; i < askCount; ++i) {
					ContainerRequest containerAsk = setupContainerAskForRM();
					amRMClient.addContainerRequest(containerAsk);
				}
			}

			if (numCompletedContainers.get() == numTotalContainers) {
				done = true;
			}
		}

		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
			LOG.info("Got response from RM for container ask, allocatedCnt="
					+ allocatedContainers.size());
			LOG.info("before add" + numAllocatedContainers.get());
			numAllocatedContainers.addAndGet(allocatedContainers.size());
			LOG.info("totalAllocated:" + numAllocatedContainers.get());
			LOG.info("totalRequested:" + numTotalContainers);
			for (Container allocated : allocatedContainers) {
				LOG.info("containerId=" + allocated.getId()
						+ ", containerNode=" + allocated.getNodeId().getHost()
						+ ":" + allocated.getNodeId().getPort()
						+ ", containerNodeURI="
						+ allocated.getNodeHttpAddress()
						+ ", containerResourceMemory"
						+ allocated.getResource().getMemory()
						+ ", containerResourceVirtualCores"
						+ allocated.getResource().getVirtualCores());
				allAllocatedContainers.add(allocated);
			}

			if (numAllocatedContainers.intValue() == numTotalContainers) {
				try {
					InetAddress ia;
					List<String> allocatedIpList = new ArrayList<String>();
					for(int i = 0; i < allocatedContainers.size(); i++){
						Container allocatedContainer = allocatedContainers.get(i);
						LOG.info("Launching worker on a new container."
								+ ", containerId=" + allocatedContainer.getId()
								+ ", containerNode=" + allocatedContainer.getNodeId().getHost()
								+ ":" + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
								+ allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory"
								+ allocatedContainer.getResource().getMemory() + ", containerResourceVirtualCores"
								+ allocatedContainer.getResource().getVirtualCores());
						String hostName = allocatedContainer.getNodeId().getHost();
						ia = InetAddress.getByName(hostName);
						String hostIp = ia.getHostAddress();
						allocatedIpList.add(hostIp);
					}
					LOG.info("allocated ip list:" + allocatedIpList);
					processMachineFile(allocatedIpList);
					for(int i = 0; i < allocatedContainers.size(); i++){
						Container allocatedContainer = allocatedContainers.get(i);
						LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
								allocatedContainer, containerListener, i + "");
						Thread launchThread = new Thread(
								runnableLaunchContainer);
						launchThreads.add(launchThread);
						launchThread.start();
						if(i == 0)
							Thread.sleep(3000);
					}
				} catch (Exception e) {
					LOG.error(e.getMessage());
					LOG.error(e.getStackTrace());
				}
			}
			currentTime = System.currentTimeMillis();
			LOG.info("start up App in " + (currentTime - startTime) + "ms");
		}

		@Override
		public void onShutdownRequest() {
			done = true;
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
		}

		@Override
		public float getProgress() {
			// set progress to deliver to RM on next heartbeat
			float progress = (float) numCompletedContainers.get()
					/ numTotalContainers;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
				done = true;
				amRMClient.stop();
		}
	}

	
	public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
		private final ApplicationMaster applicationMaster;

		public NMCallbackHandler(ApplicationMaster applicationMaster) {
			this.applicationMaster = applicationMaster;
		}

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Succeeded to stop Container " + containerId);
			}
			containers.remove(containerId);
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId,
				ContainerStatus containerStatus) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Container Status: id=" + containerId + ", status="
						+ containerStatus);
			}
		}

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Succeeded to start Container " + containerId);
			}
			Container container = containers.get(containerId);
			if (container != null) {
				applicationMaster.nmClientAsync.getContainerStatusAsync(
						containerId, container.getNodeId());
			}
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to start Container " + containerId);
			containers.remove(containerId);
			applicationMaster.numCompletedContainers.incrementAndGet();
			applicationMaster.numFailedContainers.incrementAndGet();
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId,
				Throwable t) {
			LOG.error("Failed to query the status of Container " + containerId);
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to stop Container " + containerId);
			containers.remove(containerId);
		}
	}

	/**
	 * Thread to connect to the {@link ContainerManagementProtocol} and launch
	 * the container that will execute the worker.
	 */
	private class LaunchContainerRunnable implements Runnable {

		// Allocated container
		Container container;

		NMCallbackHandler containerListener;

		String clientId;

		/**
		 * @param lcontainer
		 *            Allocated container
		 * @param containerListener
		 *            Callback handler of the container
		 */
		public LaunchContainerRunnable(Container lcontainer,
				NMCallbackHandler containerListener, String cliendId) {
			this.container = lcontainer;
			this.containerListener = containerListener;
			this.clientId = cliendId;
		}

		@Override
		/**
		 * Connects to CM, sets up container launch context 
		 * for worker and eventually dispatches the container 
		 * start request to the CM. 
		 */
		public void run() {
			LOG.info("Setting up container launch container for containerid="
					+ container.getId());

			// Set the local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			
			FileSystem fs = null;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                LOG.fatal("Get filesystem fail");
                LOG.fatal(e.getMessage());
                numCompletedContainers.incrementAndGet();
                numFailedContainers.incrementAndGet();
                return;
            }
			
			try {
                // Add machine files and launch files to local resources.
				YarnUtil.addToLocalResources(fs, launchfileIdentifier, scriptHDFSPath, localResources);
                YarnUtil.addToLocalResources(fs, hostfileIdentifier, hostfileHDFSPath, localResources);
            } catch (IOException e) {
                LOG.fatal("Add host file to local fails");
                LOG.fatal(e.getMessage());
                numCompletedContainers.incrementAndGet();
                numFailedContainers.incrementAndGet();
                return;
            }

			// Set the necessary command to execute on the allocated container
			Vector<CharSequence> vargs = new Vector<CharSequence>(5);

			// Set executable command
			vargs.add(shellCommand);
			// Set script path
			vargs.add("./" + launchfileIdentifier);
			
			
			vargs.add(clientId);			
			vargs.add("./" + hostfileIdentifier);
			// Set args for the shell command if any
			vargs.add(shellArgs);
			// Add log redirect params
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
					+ "/stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
					+ "/stderr");

			// Get final commmand
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}

			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());
			LOG.info("Commands:" + commands.toString());
			
			ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
					localResources, shellEnv, commands, null,
					allTokens.duplicate(), null);
			containerListener.addContainer(container.getId(), container);
			nmClientAsync.startContainerAsync(container, ctx);
		}
	}

	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 *
	 * @return the setup ResourceRequest to be sent to RM
	 */
	private ContainerRequest setupContainerAskForRM() {
		// setup requirements for hosts
		// set the priority for the request
		Priority pri = Priority.newInstance(requestPriority);

		// Set up resource type requirements
		// For now, memory and CPU are supported so we set memory and cpu
		// requirements
		Resource capability = Resource.newInstance(containerMemory,
				containerVirtualCores);
		String[] nodes = null;
		if (!avaliableNodeList.isEmpty()) {
			nodes = new String[1];
			nodes[0] = (String) avaliableNodeList.get(0).getNodeId().getHost();
			avaliableNodeList.remove(0);
		}
		//String[] racks = {"/default-rack"};
		ContainerRequest request = new ContainerRequest(capability, nodes,
				null, pri, false);
		LOG.info("Requested container ask: " + request.toString() + ", nodes: "
				+ request.getNodes());
		return request;
	}
	
	private void processMachineFile(List<String> allocatedIpList) {
		try {
			String text = "";
			String lineTxt = "";
			for (int i = 0; i < allocatedIpList.size(); i++) {
				lineTxt = i + " " + allocatedIpList.get(i) + " " + startPort;
				text = text + lineTxt + "\n";
			}
			LOG.info("server text:" + text.trim());
			FileSystem fs = FileSystem.get(conf);
	        if (petuumHDFSPathPrefix.equals("")) {
	            hostfileHDFSPath = new Path(fs.getHomeDirectory(),
	                    hostfileIdentifier).toUri().toString();
	        } else {
	        	hostfileHDFSPath = new Path(fs.getHomeDirectory(), petuumHDFSPathPrefix + hostfileIdentifier)
	                    .toUri().toString();
	        }
	        LOG.info("Hostfile being writen to " + hostfileHDFSPath);
	        YarnUtil.writeFileHDFS(fs, hostfileHDFSPath, text.trim());
		} catch (Exception e) {
			System.out.println("read file error");
			e.printStackTrace();
		}
	}	
}
