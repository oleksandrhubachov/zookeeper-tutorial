package com.hubachov;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class LeaderElection implements Watcher {

	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final String ELECTION_NAMESPACE = "/election";
	private static final String TARGET_ZNODE = "/target_znode";
	private static final int SESSION_TIMEOUT = 3000;
	private ZooKeeper zooKeeper;
	private String currentZnodeName;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		final LeaderElection leaderElection = new LeaderElection();
		leaderElection.connectToZookeeper();
		leaderElection.volunteerForLeadership();
		leaderElection.reelectLeader();
//		leaderElection.watchTargetZnode();
		leaderElection.run();
		leaderElection.close();
		System.out.println("Disconnected from ZooKeeper, exiting application");
	}

	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		final String znodePrefix = ELECTION_NAMESPACE + "/c_";
		final String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("znode name " + znodeFullPath);
		this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
	}

	public void reelectLeader() throws KeeperException, InterruptedException {
		Stat predecessorStat = null;
		String predecessorZnodeName = "";
		while (predecessorStat == null) {
			final List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
			Collections.sort(children);
			final String smallestChild = children.get(0);
			if (smallestChild.equals(currentZnodeName)) {
				System.out.println("I'm the leader");
				return;
			} else {
				System.out.println("I am not a leader, " + smallestChild + " is the leader");
				int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
				predecessorZnodeName = children.get(predecessorIndex);
				predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
			}
		}
		System.out.println("Watching znode " + predecessorZnodeName);
		System.out.println();
	}

	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}

	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	public void watchTargetZnode() throws KeeperException, InterruptedException {
		final Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
		if (stat == null) {
			return;
		}
		final byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
		final List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
		System.out.println("Data: " + new String(data) + " children: " + children);
	}

	@Override
	public void process(final WatchedEvent event) {
		switch (event.getType()) {
			case None:
				if (event.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("Successfully connected to ZooKeeper");
				} else {
					synchronized (zooKeeper) {
						System.out.println("Disconnect from ZooKeeper event");
						zooKeeper.notifyAll();
					}
				}
			case NodeCreated:
				System.out.println(TARGET_ZNODE + " was created");
				break;
			case NodeDeleted:
				System.out.println(TARGET_ZNODE + " was deleted");
				try {
					reelectLeader();
				} catch (KeeperException | InterruptedException e) {}
				break;
			case NodeDataChanged:
				System.out.println(TARGET_ZNODE + " data changed");
				break;
			case NodeChildrenChanged:
				System.out.println(TARGET_ZNODE + " children changed");
				break;
		}
		try {
			watchTargetZnode();
		} catch (KeeperException | InterruptedException e) {}
	}

}
