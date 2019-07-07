package cn.itcast.zookeeper_api;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLCreateModeBackgroundPathAndBytesable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class JAVA_API_demo {
    /*
    *
    * 节点watch机制
    *
    * */
    @Test
    public void watchZnode() throws Exception {
        //1定制一个重试策略
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        //2获得一个人客户端对象
        String connectionStr="node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //3开启客户端
        client.start();
        //4创建一个TreeCach对象,指定要监控的节点路径
        TreeCache treeCache = new TreeCache(client, "/hello10");
        //5自定义监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if(data!=null){
                    switch (treeCacheEvent.getType()){
                        case NODE_ADDED:
                            System.out.println("监听到新增节点");
                            break;
                        case NODE_UPDATED:
                            System.out.println("监听到更新节点");
                            break;
                        case NODE_REMOVED:
                            System.out.println("监听到删除节点");
                            break;
                    }
                }

            }
        });
      //5开始监听
        treeCache.start();
        //延时
        Thread.sleep(800000000);
        //5关闭客户端
        client.close();
    }



    /*
     * 创建临时有序节点
     *
     * */
    @Test
    public void createNode3() throws Exception {
        //1定制一个重试策略
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        //2获得一个人客户端对象
        String connectionStr="node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //3开启客户端
        client.start();
        //4创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/tmp1", "world".getBytes());

        Thread.sleep(8000);
        //5关闭客户端
        client.close();
    }
    /*
     * 创建永久有序节点
     *
     * */
    @Test
    public void createNode2() throws Exception {
        //1定制一个重试策略
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        //2获得一个人客户端对象
        String connectionStr="node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //3开启客户端
        client.start();
        //4创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/hello11", "world".getBytes());
        //5关闭客户端
        client.close();
    }
    /*
    * 创建临时节点
    *
    * */
    @Test
    public void createNode1() throws Exception {
        //1定制一个重试策略
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        //2获得一个人客户端对象
        String connectionStr="node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //3开启客户端
        client.start();
        //4创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/tmp1", "world".getBytes());

        Thread.sleep(8000);
        //5关闭客户端
        client.close();
    }
    /*
    * 创建永久节点
    *
    * */
    @Test
    public void createNode() throws Exception {
        //1定制一个重试策略
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        //2获得一个人客户端对象
        String connectionStr="node01:2181,node02:2181,node03:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //3开启客户端
        client.start();
        //4创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello10", "world".getBytes());
        //5关闭客户端
        client.close();
    }
}
