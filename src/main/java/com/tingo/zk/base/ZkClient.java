package com.tingo.zk.base;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Created by tengfei on 2017/3/28.
 */
public class ZkClient implements Watcher {

    private String url;
    private int sessionTimeout;
    private boolean canBeReadOnly;
    private long sessionId;
    private byte[] sessionPasswd;
    private Watcher watcher;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public boolean isCanBeReadOnly() {
        return canBeReadOnly;
    }

    public void setCanBeReadOnly(boolean canBeReadOnly) {
        this.canBeReadOnly = canBeReadOnly;
    }



    @Override
    public void process(WatchedEvent event) {
        if(Event.KeeperState.SyncConnected == event.getState()) {

        }
    }
}
