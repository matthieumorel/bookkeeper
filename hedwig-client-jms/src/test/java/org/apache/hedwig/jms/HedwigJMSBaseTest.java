package org.apache.hedwig.jms;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Before;

public class HedwigJMSBaseTest {

    protected PubSubServer hedwigServer;
    private NIOServerCnxnFactory serverFactory;
    private ZooKeeper zkc;
    private File ZkTmpDir;
    private ZooKeeperServer zks;
    private int ZooKeeperDefaultPort = 2181;
    private String HOSTPORT = "127.0.0.1:2181";
    private List<BookieServer> bs = new ArrayList<BookieServer>();
    private BookKeeper bkc;
    protected File hedwigConfigFile;
    List<File> tmpDirs = new ArrayList<File>();
    private String hedwigParams = "default_server_host=localhost:4080\n"
            + "zookeeper_connection_string=localhost:2181\n" + "zookeeper_connection_timeout=120000\n"
            + "max_outstanding_messages=" + 10000;

    @Before
    public void prepare() throws Exception, InterruptedException, KeeperException {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        // LOG.debug("Running ZK server");
        // ServerStats.registerAsConcrete();

        hedwigConfigFile = new File(System.getProperty("java.io.tmpdir") + "/hedwig.cfg");
        TestUtils.writeStringToFile(hedwigParams, hedwigConfigFile);

        TestUtils.cleanupTmpDirs();
        ClientBase.setupTestEnv();
        ZkTmpDir = File.createTempFile("zookeeper", "test");
        ZkTmpDir.delete();
        ZkTmpDir.mkdir();

        zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);

        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(new InetSocketAddress(ZooKeeperDefaultPort), 100);
        serverFactory.startup(zks);

        ClientBase.waitForServerUp(HOSTPORT, 100000);

        System.out.println("Zookeeper server up and running!");
        // LOG.debug("Server up: " + b);

        // create a zookeeper client
        // LOG.debug("Instantiate ZK Client");
        zkc = new ZooKeeper("127.0.0.1", ZooKeeperDefaultPort, null);

        // initialize the zk client with values
        zkc.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/ledgers/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < 3; i++) {
            File f = File.createTempFile("bookie", "test");
            tmpDirs.add(f);
            f.delete();
            f.mkdir();

            BookieServer server = new BookieServer(new org.apache.bookkeeper.conf.ServerConfiguration()
                    .setBookiePort(5000 + i).setZkServers(HOSTPORT).setJournalDirName(f.getAbsolutePath())
                    .setLedgerDirNames(new String[] { f.getAbsolutePath() }));
            server.start();
            bs.add(server);
        }
        zkc.close();
        bkc = new BookKeeper("127.0.0.1");
        Thread.sleep(4000);
        System.out.println("added bookies!");

    }

    @After
    public void tearDown() throws Exception {
        if (hedwigServer != null) {
            hedwigServer.shutdown();
        }
        serverFactory.shutdown();
        for (BookieServer bookie : bs) {
            bookie.shutdown();
        }
        zks.shutdown();
    }

}
