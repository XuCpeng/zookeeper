package cn.medemede.zkcs;

import org.apache.zookeeper.*;

import java.io.IOException;

public class ZKServer {
    private ZooKeeper zooKeeper=null;
    private String parentNode="/servers";  //需要事先在zk上创建此节点
    // 1 获取链接
    public void getConnect() throws IOException {
        // zk地址及端口号
        String connectStr = "hadoop101:2181,hadoop102:2181,hadoop103:2181,hadoop104:2181";
        // 会话超时时间 毫秒
        int sessionTimeout = 2000;

        zooKeeper=new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    // 2 注册(创建节点)
    public void regist(String hostname) throws KeeperException, InterruptedException {

        String creatZK = zooKeeper.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+" is online "+creatZK);
    }

    // 3 具体业务
    public void business() {
        System.out.println("Server 注册成功");
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //获取连接
        ZKServer zkServer=new ZKServer();
        zkServer.getConnect();

        //注册节点
        zkServer.regist(args[0]);

        //业务逻辑
        zkServer.business();

    }
}
