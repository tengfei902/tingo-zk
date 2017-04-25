package com.tingo.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * Created by user on 17/4/24.
 */
public class CuratorTest extends BaseTestCase {

    private static final String url = "http://127.0.0.1:2181";
    private static final int connectTimeOut = 10*1000;
    private static final int sessionTimeOut = 100*1000;

    private static final String nameSpace = "zk-base";

    @Test
    public void testConnectToZk() {
        String nameSpace = "zk-base";

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(url,sessionTimeOut,connectTimeOut,retryPolicy);
        client.start();

        //2
        CuratorFramework client2 = CuratorFrameworkFactory.builder().connectString(url).connectionTimeoutMs(connectTimeOut).sessionTimeoutMs(sessionTimeOut).retryPolicy(retryPolicy).namespace(nameSpace).build();
        client2.start();
    }

    private CuratorFramework getClient(String dir) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(url).connectionTimeoutMs(connectTimeOut).sessionTimeoutMs(sessionTimeOut).retryPolicy(retryPolicy).namespace(dir).build();
        client.start();
        return client;
    }

    @Test
    public void testCreateNode() throws Exception {
        CuratorFramework client = getClient("/zk-test-create");
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/test1","test".getBytes());
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/test1","test".getBytes());
    }

    @Test
    public void testDelete() throws Exception {
        String path = "zk-test-create";
        CuratorFramework client = getClient(nameSpace);
        client.delete().forPath(path);
        client.delete().withVersion(1).forPath(nameSpace);
        client.delete().deletingChildrenIfNeeded().forPath(nameSpace);
    }

    @Test
    public void testReadData() throws Exception {
        CuratorFramework client = getClient(nameSpace);
        String path = "zk-test-create";
        client.getData().forPath(path);

        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath(path);
    }

    @Test
    public void testUpdateData() throws Exception {
        CuratorFramework client = getClient(nameSpace);
        String path = "zk-test-create";
        client.setData().withVersion(1).forPath(path).getVersion();
    }

    @Test
    public void testGetMasterNode() {
        CuratorFramework client = getClient(url);
        String master_path = "/mater_path";
        LeaderSelector leaderSelector = new LeaderSelector(client, master_path, new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                System.out.println("Selected to be master");
                Thread.sleep(30000);
                System.out.println("Finished");
            }
        });

        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    @Test
    public void testDistributedLock() throws Exception {
        CuratorFramework client = getClient(url);
        String lock_url = "/lock_url";
        InterProcessMutex lock = new InterProcessMutex(client,lock_url);
        lock.acquire();

        try {
            //do something
            System.out.println("In a lock");
        } finally {
            lock.release();
        }
    }
}
