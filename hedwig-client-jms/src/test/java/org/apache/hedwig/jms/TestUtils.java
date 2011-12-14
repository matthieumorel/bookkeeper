package org.apache.hedwig.jms;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactoryAccessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.JMXEnv;

public class TestUtils {

    public static final int ZK_PORT = 2181;
    public static final int INITIAL_BOOKIE_PORT = 5000;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(System.getProperty("user.dir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
            + "storage");
    static List<BookieServer> bs = new ArrayList<BookieServer>();
    public static ServerCnxnFactory serverFactory;
    private static LinkedList<BookieServer> bookiesList;

    public static void startServer() throws Exception {
        final File zkDataDir = new File(System.getProperty("user.dir") + File.separator + "tmp" + File.separator
                + "zookeeper" + File.separator + "data");
        if (zkDataDir.exists()) {
            TestUtils.deleteDirectoryContents(zkDataDir);
        } else {
            zkDataDir.mkdirs();
        }
        serverFactory = createNewServerInstance(zkDataDir, null, String.valueOf(ZK_PORT), 10);
        // ensure that only hedwigServer and data bean are registered
        JMXEnv.ensureOnly("InMemoryDataTree", "StandaloneServer_port");
    }

    static ServerCnxnFactory createNewServerInstance(File dataDir, ServerCnxnFactory factory, String hostPort,
            int maxCnxns) throws IOException, InterruptedException {
        ZooKeeperServer zks = new ZooKeeperServer(dataDir, dataDir, 3000);
        if (factory == null) {
            factory = ServerCnxnFactory.createFactory(ZK_PORT, maxCnxns);
        }
        factory.startup(zks);
        org.junit.Assert.assertTrue("waiting for hedwigServer up",
                ClientBase.waitForServerUp("127.0.0.1:" + ZK_PORT, 4000));

        return factory;
    }

    public static void deleteDirectoryContents(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                deleteDirectoryContents(file);
            }
            if (!file.delete()) {
                throw new RuntimeException("could not delete : " + file);
            }
        }
    }

    protected void stopServer() throws Exception {
        shutdownServerInstance(serverFactory, "localhost:" + ZK_PORT);
        serverFactory = null;
        // ensure no beans are leftover
        JMXEnv.ensureOnly();
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

    static void shutdownServerInstance(ServerCnxnFactory factory, String hostPort) {
        if (factory != null) {
            ZKDatabase zkDb;
            {
                ZooKeeperServer zs = getServer(factory);

                zkDb = zs.getZKDatabase();
            }
            factory.shutdown();
            try {
                zkDb.close();
            } catch (IOException ie) {
                ie.printStackTrace();
            }

            org.junit.Assert.assertTrue("waiting for hedwigServer down",
                    ClientBase.waitForServerDown("127.0.0.1:" + ZK_PORT, 4000));
        }
    }

    protected static ZooKeeperServer getServer(ServerCnxnFactory fac) {
        ZooKeeperServer zs = ServerCnxnFactoryAccessor.getZkServer(fac);

        return zs;
    }

    public static void initializeBKBookiesAndLedgers(final ZooKeeper zk) throws KeeperException, InterruptedException,
            IOException {
        try {
            zk.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/ledgers/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Create Bookie Servers
        bs = new LinkedList<BookieServer>();

        for (int i = 0; i < 3; i++) {
            File f = new File(DEFAULT_STORAGE_DIR + "/bookie_test_" + i);
            f.delete();
            f.mkdir();
            BookieServer server = new BookieServer(new org.apache.bookkeeper.conf.ServerConfiguration()
                    .setBookiePort(INITIAL_BOOKIE_PORT + i).setZkServers("localhost:" + ZK_PORT)
                    .setJournalDirName(f.getAbsolutePath()).setLedgerDirNames(new String[] { f.getAbsolutePath() }));
            server.start();
            bs.add(server);
        }

    }

    public static void stopBKBookies() throws Exception {
        if (bs != null) {
            for (BookieServer bookie : bs) {
                bookie.shutdown();
            }
        }
    }

    public static void cleanupTmpDirs() {
        if (DEFAULT_TEST_OUTPUT_DIR.exists()) {
            deleteDirectoryContents(DEFAULT_TEST_OUTPUT_DIR);
        }
        DEFAULT_STORAGE_DIR.mkdirs();
    }

}
