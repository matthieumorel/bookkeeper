package org.apache.hedwig.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

public class TestPubSubServerWithConfig {

	private static Logger logger = Logger.getLogger(TestPubSubServerWithConfig.class);

	/**
	 * start-up zookeeper + pubsubserver reading from a config URL.
	 * Then stop and cleanup
	 * 
	 * loop over that.
	 * 
	 * if the pubsub server does not wait for its zookeeper client to be
	 * connected, the pubsub server will fail at startup.
	 * 
	 * 
	 * 
	 */
	@Test
	public void testPubSubServerInstantiationWithConfig() throws Exception {
		for (int i = 0; i < 10; i++) {
			logger.info("iteration " + i);
			instantiateAndDestroyPubSubServer();
		}
	}

	private void instantiateAndDestroyPubSubServer() throws IOException, InterruptedException, ConfigurationException,
	        MalformedURLException, Exception {
		String hedwigParams = "default_server_host=localhost:4080\n" + "zookeeper_connection_string=localhost:2181\n"
		        + "zookeeper_connection_timeout=120000\n";

		File hedwigConfigFile = new File(System.getProperty("java.io.tmpdir") + "/hedwig.cfg");
		writeStringToFile(hedwigParams, hedwigConfigFile);

		ClientBase.setupTestEnv();
		File zkTmpDir = File.createTempFile("zookeeper", "test");
		zkTmpDir.delete();
		zkTmpDir.mkdir();

		ZooKeeperServer zks = new ZooKeeperServer(zkTmpDir, zkTmpDir, 2181);

		NIOServerCnxnFactory serverFactory = new NIOServerCnxnFactory();
		serverFactory.configure(new InetSocketAddress(2181), 100);
		serverFactory.startup(zks);

		boolean b = ClientBase.waitForServerUp("127.0.0.1:2181", 5000);
		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI().toURL());

		logger.info("Zookeeper server up and running!");

		ZooKeeper zkc = new ZooKeeper("127.0.0.1", 2181, null);

		// initialize the zk client with (fake) values 
		zkc.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zkc.create("/ledgers/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		zkc.close();
		PubSubServer hedwigServer = null;
		try {
			logger.info("starting hedwig broker!");
			hedwigServer = new PubSubServer(serverConf);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("failed to instantiate hedwig pub sub server");
		}

		hedwigServer.shutdown();
		serverFactory.shutdown();

		zks.shutdown();

		zkTmpDir.delete();

		ClientBase.waitForServerDown("localhost:2181", 10000);

	}

	public static void writeStringToFile(String string, File f) throws IOException {
		if (f.exists()) {
			if (!f.delete()) {
				throw new RuntimeException("cannot create file " + f.getAbsolutePath());
			}
		}
		if (!f.createNewFile()) {
			throw new RuntimeException("cannot create new file " + f.getAbsolutePath());
		}

		FileWriter fw = new FileWriter(f);
		fw.write(string);
		fw.close();
	}
}
