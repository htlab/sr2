package soxrecorderv2.recorder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import soxrecorderv2.common.SoxRecorderClient;
import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.common.model.RecordTask;
import soxrecorderv2.common.model.SoxLoginInfo;
import soxrecorderv2.finder.Finder;
import soxrecorderv2.logging.SR2LogItem;
import soxrecorderv2.logging.SR2Logger;
import soxrecorderv2.logging.SR2PostgresLogWriter;
import soxrecorderv2.util.ThreadUtil;
import sun.misc.Signal;
import sun.misc.SignalHandler;


/**
 * runs as a daemon, subscribe sox nodes with a single connection,
 * and just waiting for data arrival, save data to PostgreSQL directly when data is received.
 * 
 * (recording process skips calling Controlller API to reduce overhead and gain some performance advantage.)
 * 
 * @author tomotaka
 *
 */
public class Recorder implements Runnable {
	
	public static final String[] FIXED_TARGET_SERVERS = {
		"sox.ht.sfc.keio.ac.jp",
		"soxfujisawa.ht.sfc.keio.ac.jp"
	};
	
	private LinkedBlockingQueue<RecordTask> taskQueue;
	private LinkedBlockingQueue<SR2LogItem> logItemQueue;
	private SR2PostgresLogWriter logWriter;
	private List<DBWriterProcess> writers;
	@SuppressWarnings("unused")
	private List<String> targetServers;
	private List<Finder> finders = new ArrayList<>();
	private Map<String, Finder> server2finder = new HashMap<>();
	private List<ServerRecordProcess> serverRecorders = new ArrayList<>();
	private Map<String, ServerRecordProcess> server2recorder = new HashMap<>();
	
	private Thread logWriterThread;
	private List<Thread> finderThreads;
	private List<Thread> recordThreads;
	private List<Thread> writerThreads;
	
	@SuppressWarnings("unused")
	private SoxRecorderClient client;
	private String configFile;
	private String endpoint;
	private String apiKey;
	private Properties config;
	
	public Recorder(final String configFile) {
		this.configFile = configFile;
		loadConfig();
		
		endpoint = config.getProperty("endpoint");
		apiKey = config.getProperty("apikey");
		System.out.println("[Recorder] endpoint=" + endpoint);
		System.out.println("[Recorder] apiKey=" + apiKey);
		
		client = new SoxRecorderClient(endpoint, apiKey);
	}
	
	private void loadConfig() {
		try (FileInputStream fin = new FileInputStream(configFile)) {
			config = new Properties();
			config.load(fin);
			System.out.println("[Recorder] loaded properties(config) from file=" + configFile);
		} catch (FileNotFoundException e) {
			// FIXME Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// FIXME Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		try {
			Class.forName("org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (args.length < 1) {
			System.err.println("missing config file");
			System.exit(-1);
			return;
		}
		
		String configFile = args[0];
		
		final Recorder recorder = new Recorder(configFile);
		System.out.println("[Recorder] recorder instance created");
		final Thread recorderThread = new Thread(recorder);
		recorderThread.start();
		System.out.println("[Recorder] recorder thread started");
		
		// SIGTERMやSIGINTが来たときにシステムをシャットダウンするようにする
		Signal sigterm = new Signal("TERM");
		Signal sigint = new Signal("INT");
		Signal[] handleSignals = { sigterm, sigint };
		SignalHandler sigHandler = new SignalHandler() {
			@Override
			public void handle(Signal arg0) {
				System.out.println("[Record-main] got signal 1");
				recorder.stopSoxRecorder();
				System.out.println("[Record-main] got signal 2");
//				ThreadUtil.join(recorderThread);
				System.out.println("[Record-main] got signal 3 finished");
				System.exit(0);
			}
		};
		for (Signal sig : handleSignals) {
			Signal.handle(sig, sigHandler);
		}
		
		Runtime runtime = Runtime.getRuntime();
		runtime.addShutdownHook(new Thread() {
			@Override public void run() {
				System.out.println("[Record-main][shutdownhook] 1");
				if (recorderThread.isAlive()) {
					System.out.println("[Record-main][shutdownhook] 2.1");
					recorder.stopSoxRecorder();
					System.out.println("[Record-main][shutdownhook] 2.2");
//					ThreadUtil.join(recorderThread);
				}
				System.out.println("[Record-main][shutdownhook] 2.2 finished");
			}
		});
	}
	
	@Override
	public void run() {
		// DBとの接続管理オブジェクトを用意する
		System.out.println("[Recorder][run][0] going to boot");
//		connManager = new PGConnectionManager(this);
//		System.out.println("[Recorder][run][1] created connManager");
		
		System.out.println("[Recorder][run][0.5] going to prepare log system");
		logItemQueue = new LinkedBlockingQueue<>(1000);
		logWriter = new SR2PostgresLogWriter(this, 500);
		logWriterThread = new Thread(logWriter);
		logWriterThread.start();

		// 対象のサーバーリストを取得する
//		try {
//			targetServers = client.getSoxServerList();
//		} catch (SR2CommunicationException | SR2InvalidApiKeyException e) {
//			e.printStackTrace();  // FIXME auto generated
//		}
		System.out.println("[Recorder][run][1] fetched sox server list");
		
		// soxとsoxfujisawaを追加する => 重複をなくす
//		Set<String> unifiedTargets = new HashSet<>(targetServers);
		Set<String> unifiedTargets = new HashSet<>();
		for (String fixedServer : FIXED_TARGET_SERVERS) {
			unifiedTargets.add(fixedServer);
		}
		System.out.println("[Recorder][run][2] added FIXED_TARGET_SERVERS");
		
		// xmpp subscriptionの状態と, データベースの状態をシンクロさせる
		for (String soxServer : unifiedTargets) {
			SoxLoginInfo soxLoginInfo = getDefaultRecorderLoginInfo(soxServer);
			SubStateSynchronizer synchronizer = new SubStateSynchronizer(this, soxLoginInfo);
			synchronizer.run();
			synchronizer.shutdownSubProcess();
		}
		
		// 受信プロセスからDB書き込みプロセスへタスクを渡すキューを準備する
		taskQueue = new LinkedBlockingQueue<>(100);
		System.out.println("[Recorder][run][3] prepared taskQueue");
		
		// 受信したデータをDBに書き込むプロセスを開始する
		writers = new ArrayList<>();
		writerThreads = new ArrayList<>();
		int nWriters = 1;  // FIXME: multi thread?
		for (int i = 0; i < nWriters; i++) {
			DBWriterProcess writer = new DBWriterProcess(this, taskQueue);
			Thread writerThread = new Thread(writer);
			writerThread.start();
			writers.add(writer);
			writerThreads.add(writerThread);
		}
		System.out.println("[Recorder][run][4] started writerThread");
		
		// それぞれのXMPPサーバについてレコーディングプロセスを開始する
		serverRecorders = new ArrayList<>();
		recordThreads = new ArrayList<>();
		for (String soxServer : unifiedTargets) {
			ServerRecordProcess srvRecorder = new ServerRecordProcess(this, taskQueue, soxServer);
			Thread srvRecorderThread = new Thread(srvRecorder);
			srvRecorderThread.start();
			serverRecorders.add(srvRecorder);
			server2recorder.put(soxServer, srvRecorder);
			recordThreads.add(srvRecorderThread);
			System.out.println("[Recorder][run][5] started ServerRecordProcess for sox_server=" + soxServer);
		}
		
		// 新しいノードを見つけたらDB登録+subscribeを行うfinderをxmppサーバごとに走らせる
		finders = new ArrayList<>();
		finderThreads = new ArrayList<>();
		for (String soxServer : unifiedTargets) {
			Finder finder = new Finder(this, soxServer);
			Thread finderThread = new Thread(finder);
			finderThread.start();
			finders.add(finder);
			server2finder.put(soxServer, finder);
			finderThreads.add(finderThread);
			System.out.println("[Recorder][run][6] started Finder for sox_server=" + soxServer);
		}
		
		// just wait
	}
	
	public Properties getConfig() {
		return config;
	}
	
	public void subscribe(NodeIdentifier nodeId) {
		ServerRecordProcess recorder = server2recorder.get(nodeId.getServer());
		if (recorder == null) {
			// FIXME logging?
			return;
		}
		recorder.subscribe(nodeId);
	}
	
	public SoxLoginInfo getDefaultRecorderLoginInfo(String soxServer) {
		return new SoxLoginInfo(soxServer, "soxrecorder", "!htmiro1"); // FIXME
	}
	
	public void stopSoxRecorder() {
		for (RecorderSubProcess subProcess : getSubProcesses()) {
			subProcess.shutdownSubProcess();
		}
		
		// join all threads
		ThreadUtil.joinAll(getSubProcessThreads());

		for (RecorderSubProcess subProcess : getSubProcesses()) {
			subProcess.getConnManager().close();
		}
	}

	/**
	 * 終了時の処理のため, シャットダウンが可能になっているサブコンポーネントを列挙する
	 * @return
	 */
	public Collection<RecorderSubProcess> getSubProcesses() {
		List<RecorderSubProcess> subProcesses = new ArrayList<>();
		subProcesses.addAll(finders);
		subProcesses.addAll(serverRecorders);
		subProcesses.addAll(writers);
		subProcesses.add(logWriter);  // logWriterは最後
		return subProcesses;
	}
	
	/**
	 * 終了時の処理のため, 終了を待つためのスレッドの一覧を取得する
	 * @return
	 */
	public Collection<Thread> getSubProcessThreads() {
		List<Thread> ret = new ArrayList<>();
		ret.addAll(finderThreads);
		ret.addAll(recordThreads);
		ret.addAll(writerThreads);
		ret.add(logWriterThread);  // logWriterは最後
		return ret;
	}
	
	public LinkedBlockingQueue<SR2LogItem> getLogItemQueue() {
		return logItemQueue;
	}
	
	public SR2Logger createLogger(String componentName) {
		return new SR2Logger(componentName, logItemQueue);
	}
	
}
