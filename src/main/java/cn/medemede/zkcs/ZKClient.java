package cn.medemede.zkcs;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZKClient {
    //客户端
    private ZooKeeper zooKeeper=null;
    private String parentNode="/servers";

    //创建客户端
    public void getConnect() throws IOException {
        // zk地址及端口号
        String connectStr = "hadoop101:2181,hadoop102:2181,hadoop103:2181,hadoop104:2181";
        // 会话超时时间 毫秒
        int sessionTimeout = 2000;

        //创建客户端
        zooKeeper = new ZooKeeper(connectStr, sessionTimeout, new Watcher() {

            public void process(WatchedEvent watchedEvent) {
                //监听发生后触发的事件
                System.out.println(watchedEvent.getType()+"--"+watchedEvent.getPath());

                //重复监听,监听事件发生后执行getServers
                try {
                    getServers();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //监听节点变化
    public void getServers() throws KeeperException, InterruptedException {

        //监听parentNode下的节点变化
        List<String> children = zooKeeper.getChildren(parentNode, true);

        ArrayList<String> servers=new ArrayList<String>();
        for (String child:children){
            //读取节点数据
            byte[] data = zooKeeper.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        System.out.println(servers);
    }

    //具体业务
    public void business() throws InterruptedException {
        System.out.println("ss 监听");
        Thread.sleep(Long.MAX_VALUE);//测试Watch  进程结束前可以一直监听到
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZKClient zkClient=new ZKClient();
        zkClient.getConnect();

        zkClient.business();
    }

}
