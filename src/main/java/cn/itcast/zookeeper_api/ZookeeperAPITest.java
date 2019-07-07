package cn.itcast.zookeeper_api;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class ZookeeperAPITest {
    /*
    节点的watch机制
     */

    @Test
    public void watchZnode() throws Exception {
        //1:定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 1);

        //2:获取客户端
        String conectionStr = "node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(conectionStr, 8000, 8000, retryPolicy);

        //3:启动客户端
        client.start();

        //4:创建一个TreeCache对象，指定要监控的节点路径
        TreeCache treeCache = new TreeCache(client, "/hello8");

        //5:自定义一个监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {

                ChildData data = treeCacheEvent.getData();
                if(data != null){
                    switch (treeCacheEvent.getType()){
                        case NODE_ADDED:
                            System.out.println("监控到有新增节点!");
                            break;
                        case NODE_REMOVED:
                            System.out.println("监控到有节点被移除!");
                            break;
                        case NODE_UPDATED:
                            System.out.println("监控到节点被更新!");
                            break;
                        default:
                            break;
                    }
                }
            }
        });

        //开始监听
        treeCache.start();

        Thread.sleep(1000000);
    }
    /*
     获取节点数据
     */
    @Test
    public void getZnodeData() throws Exception {
        //1:定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        //2:获取客户端
        String conectionStr = "node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(conectionStr, 8000, 8000, retryPolicy);

        //3:启动客户端
        client.start();
        //4:获取节点数据
        byte[] bytes = client.getData().forPath("/hello");
        System.out.println(new String(bytes));

        //5:关闭客户端
        client.close();

    }
    /*
      设置节点数据
     */
    @Test
    public void setZnodeData() throws Exception {
        //1:定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        //2:获取客户端
        String conectionStr = "node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(conectionStr, 8000, 8000, retryPolicy);
        //3:启动客户端
        client.start();
        //4:修改节点数据
        client.setData().forPath("/hello", "zookeeper".getBytes());
        //5:关闭客户端
        client.close();

    }
    /*
    创建临时节点
     */
    @Test
    public void createTmpZnode() throws Exception {
        //1:定制一个重试策略
        /*
            param1: 重试的间隔时间
            param2:重试的最大次数
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,1);
        //2:获取一个客户端对象
        /*
           param1:要连接的Zookeeper服务器列表
           param2:会话的超时时间
           param3:链接超时时间
           param4:重试策略
         */
        String connectionStr = "node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);

        //3:开启客户端
        client.start();
        //4:创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello4","world".getBytes());

        Thread.sleep(5000);
        //5:关闭客户端
        client.close();

    }
    /*
        创建永久节点

     */
    @Test
    public void createZnode() throws Exception {
        //1:定制一个重试策略
        /*
            param1: 重试的间隔时间
            param2:重试的最大次数
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,1);
        //2:获取一个客户端对象
        /*
           param1:要连接的Zookeeper服务器列表
           param2:会话的超时时间
           param3:链接超时时间
           param4:重试策略
         */
        String connectionStr = "node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);

        //3:开启客户端
        client.start();
        //4:创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello2","world".getBytes());
        //5:关闭客户端
        client.close();
    }
}
