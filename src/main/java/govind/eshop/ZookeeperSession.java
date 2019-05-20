package govind.eshop;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

/**
 * 单例模式
 */
@Slf4j
public class ZookeeperSession implements Watcher {
	/**
	 * CountDownLatch是Java多线程同步的工具类
	 */
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private ZooKeeper zkClient;
	private static final String CONNECTION_STR = "node128:2181,node129:2181";
	private static final int SESSION_TIMEOUT = 50000;
	private static final int WAIT_TIME = 200;
	private static final int RETRIES = Integer.MAX_VALUE;

	private ZookeeperSession() {
		try {
			/** 创建会话是异步进行的，所以需要监听器判断什么时候完成会话建立 */
			this.zkClient = new ZooKeeper(CONNECTION_STR, SESSION_TIMEOUT, this);
			//因为上面是异步操作，所以这里状态一般是：CONNECTING
			log.info("ZK状态：{}", zkClient.getState());
			//等待，直到连接建立成功
			connectedSemaphore.await();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		log.info("成功建立ZK连接：{}", zkClient);
	}

	/**
	 * 处理ZK监听事件
	 *
	 * @param event
	 */
	@Override
	public void process(WatchedEvent event) {
		log.info("监听到事件：{}", event);
		if (event.getState() == SyncConnected) {
			connectedSemaphore.countDown();
		}
	}

	public void acquireLock() {
		String path = "/taskid-list-lock";
		try {
			zkClient.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			log.info("成功获取分布式锁:taskid-list-lock");
		} catch (Exception e) {
			/** 如果创建失败，则获取锁失败会抛出NodeExistException，这里等待 */
			log.info("获取锁失败，尝试等待");
			int retryTimes = 0;
			while (retryTimes < RETRIES) {
				try {
					Thread.sleep(WAIT_TIME);
					zkClient.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				} catch (Exception ex) {
					log.info("第{}次尝试获取分布式锁{}", retryTimes, path);
					retryTimes++;
					continue;
				}
				log.info("第{}次获取锁成功", retryTimes);
				break;
			}
		}
	}

	public void releaseLock() {
		String path = "/taskid-list-lock";
		try {
			//-1标识匹配任何version的节点
			zkClient.delete(path, -1);
			log.info("成功释放分布式锁：{}", path);
		} catch (InterruptedException | KeeperException e) {
			log.info("释放锁异常：{}", e.getMessage());
		}
	}

	public String getZnodeData() {
		String data = "";
		try {
			String path = "/taskid-list";
			Stat exists = zkClient.exists(path, false);
			if (exists == null) {
				zkClient.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} else {
				data = new String(zkClient.getData(path, false, new Stat()));
			}
		} catch (KeeperException | InterruptedException e) {
			log.info("忽略错误：{}", e.getMessage());
		}
		return data;
	}

	public void setZnodeData(String data) {
		try {
			String path = "/taskid-list";
			Stat exists = zkClient.exists(path, false);
			if (exists == null) {
				zkClient.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} else {
				zkClient.setData(path, data.getBytes(), -1);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	public void setZnodeData(String path, String data) {
		try {
			Stat exists = zkClient.exists(path, false);
			if (exists == null) {
				zkClient.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} else {
				zkClient.setData(path, data.getBytes(), -1);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	/*
	 * =====================
	 *   静态内部类实现单例模式
	 * =====================
	 */
	public static ZookeeperSession getInstance() {
		return Singleton.instace;
	}

	private static class Singleton {
		private static ZookeeperSession instace;
		static {
			instace = new ZookeeperSession();
		}
	}
}
