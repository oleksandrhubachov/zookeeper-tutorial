package com.hubachov;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class LeaderElection implements Watcher {

	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final String ELECTION_NAMESPACE = "/election";
	private static final int SESSION_TIMEOUT = 3000;
	private ZooKeeper zooKeeper;
	private String currentZnodeName;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		final LeaderElection leaderElection = new LeaderElection();
		leaderElection.connectToZookeeper();
		leaderElection.volunteerForLeadership();
		leaderElection.electLeader();
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

	public void electLeader() throws KeeperException, InterruptedException {
		final List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
		Collections.sort(children);
		final String smallestChild = children.get(0);
		if (smallestChild.equals(currentZnodeName)) {
			System.out.println("I'm the leader");
			return;
		}
		System.out.println("I am not a leader, " + smallestChild + " is the leader");
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

	@Override
	public void process(final WatchedEvent event) {
		switch (event.getType()) {
			case None:if (event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("Successfully connected to ZooKeeper");
			} else {
				synchronized (zooKeeper) {
					System.out.println("Disconnect from ZooKeeper event");
					zooKeeper.notifyAll();
				}
			}
		}
	}

}
