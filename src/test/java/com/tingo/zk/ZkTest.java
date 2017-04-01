package com.tingo.zk;

import com.google.gson.Gson;
import com.tingo.zk.base.ZkClient;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by tengfei on 2017/3/26.
 */
public class ZkTest extends BaseTestCase {

    private static final String url = "127.0.0.1:2181";
    private static final int sessionTimeout = 30000;

    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            System.out.println("Zk session created");
            countDownLatch.countDown();
        }
    };


    @Test
    public void testZkMain() {

    }

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 创建zk连接
     * connectString:zk服务器列表 host:port[rootPath]
     * sessionTimeout:会话超时时间 毫秒
     * watcher:事件通知回调
     * canReadOnly:在zk集群中，一个机器如果和集群中过半及以上机器失去连接，那么这个机器不再处理客户端请求，但是某些场景下，当zk服务器发生
     * 此类故障时，我们还是希望zk能提供读服务
     * sessionId & sessionPasswd 会话ID和会话密钥，
     * @throws IOException
     */
    @Test
    public void testCreateZkClient() throws IOException,InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper(url,sessionTimeout,watcher);
        countDownLatch.await();
        System.out.println("Zk session established");
    }

    /**
     * 创建节点
     * path:不支持递归创建，即不支持父节点不存在的情况下创建一个子节点；如果一个节点已经存在，创建同名节点时会NodeExistException
     * data:字节数组
     * acl:权限控制，Ids.OPEN_ACL_UNSAFE 全部权限
     * createMode: PERSISTENT|PERSISTENT_SEQUENTIAL|EPHEMERAL|EPHEMERAL_SEQUENTIAL
     * StringCallBack:
     * context:
     */
    @Test
    public void testCreateNode() throws IOException {
        String path = "/zk-book/java";
        String value = "java";

        ZooKeeper zooKeeper = new ZooKeeper(url, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Zk session created");
            }
        });
        //同步api
        try {
            zooKeeper.create(path,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
        }

        path = "/zk-book/python";
        value = "python";
        //异步api
        try {
            zooKeeper.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, String name) {
                    //rc:响应码 0(ok)
                    System.out.println("String call back:"+String.valueOf(ctx));
                }
            },"I am a Context");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点
     * path
     * version
     * cb
     * ctx
     */
    @Test
    public void testDeleteNode() throws IOException {
        String path = "/zk-book/java";
        ZooKeeper zooKeeper = new ZooKeeper(url, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Zk session created");
            }
        });

        try {
            zooKeeper.delete(path,1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            zooKeeper.delete(path, 1, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    System.out.println("delete path:"+path+",result:"+rc);
                }
            },"I am a ctx");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReadNodeData() throws IOException {

        String path = "/zk-book";
        ZooKeeper zooKeeper = new ZooKeeper(url, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Zk session created");
            }
        });
        //getChildren
        try {
            List<String> children = zooKeeper.getChildren(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println(" changed");
                }
            });

            System.out.println(children.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //use default watcher
            List<String> children = zooKeeper.getChildren(path,true);
            System.out.println(children.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        zooKeeper.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("path changed");
            }
        }, new AsyncCallback.Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                System.out.println(children.toString());
            }
        },"ctx");

        zooKeeper.getChildren(path, false, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                System.out.println(children.toString());
            }
        },"ctx");

        path = "/zk-book/java";

        Stat stat = new Stat();
        //getData
        try {
            byte[] result = zooKeeper.getData(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("path changed");
                }
            },stat);

            System.out.println(new Gson().toJson(stat));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            byte[] result = zooKeeper.getData(path,false,stat);
            System.out.println(new Gson().toJson(stat));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        zooKeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("data changed");
            }
        }, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println(rc);
            }
        },stat);

        zooKeeper.getData(path, false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println(rc);
            }
        },stat);
    }

    /**
     * 同步接口，同步返回更新后的Stat
     * 异步接口，调用回调函数
     */
    @Test
    public void testSetData() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(url,sessionTimeout,watcher);

        String path = "/zk-book/groovy";
        String result = zooKeeper.create(path,"test".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Stat stat = zooKeeper.setData(result,"test1".getBytes(),1);
        System.out.println(new Gson().toJson(stat));

        zooKeeper.setData(path, "test2".getBytes(), stat.getVersion(), new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                System.out.println(new Gson().toJson(stat));
            }
        },"ctx");
    }

    /**
     * 检测节点是否存在
     * 同步检测，异步检测
     */
    public void  testCheckExist() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(url,sessionTimeout,watcher);
        String path = "/zk-book/c#";
        zooKeeper.create(path,"test".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Stat stat = zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("node data changed");
            }
        });

        System.out.println(new Gson().toJson(stat));

        stat = zooKeeper.exists(path,false);
        System.out.println(new Gson().toJson(stat));

        zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("data changed");
            }
        }, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                System.out.println(new Gson().toJson(stat));
            }
        },"ctx");

        zooKeeper.exists(path, false, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                System.out.println(new Gson().toJson(stat));
            }
        },"ctx");
    }

    /**
     * 权限控制
     * schema:分为world,auth,digest,ip,super
     */
    @Test
    public void testAclManage() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(url,sessionTimeout,watcher);
        zooKeeper.addAuthInfo("digest","foo:true".getBytes());
        String path = "/zk-book/acl-test";
        zooKeeper.create(path,"init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL,CreateMode.EPHEMERAL);
    }
}
