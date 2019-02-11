package cn.medemede.main;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZKClientTest {
    //客户端
    private ZooKeeper zooKeeper=null;

    //创建客户端
    @Before
    public void initZK() throws IOException {
        // zk地址及端口号
        String connectStr = "hadoop101:2181,hadoop102:2181,hadoop103:2181,hadoop104:2181";
        // 会话超时时间 毫秒
        int sessionTimeout = 2000;

        //创建客户端
        zooKeeper = new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                //监听发生后触发的事件
                System.out.println(watchedEvent.getType()+"--"+watchedEvent.getPath());
            }
        });
    }

    //创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        //节点路径，节点存储的数据，创建节点后的权限，节点类型
        zooKeeper.create("/xcp/test","xcp".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //获取子节点数
    @Test
    public void getNode() throws KeeperException, InterruptedException {
        //获取节点的路径，是否监听
        List<String> children = zooKeeper.getChildren("/xcp", false);
        for (String node:children){
            System.out.println(node);
        }
    }

    //判断节点是否存在
    @Test
    public void isExist() throws KeeperException, InterruptedException {
        //路径，是否监听
        Stat exists = zooKeeper.exists("/xcp/test", true);
        System.out.println(exists!=null?"Exist":"Not Exist");

        //测试Watch  进程结束前可以一直监听到
        Thread.sleep(Long.MAX_VALUE);
    }
}
