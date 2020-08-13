package com.microsoft.cse.redis.api.helper;

public class ClusterEndpoints
{
	private static volatile ClusterEndpoints INSTANCE = null;

	public static ClusterEndpoints clusterEndpoints(String[] hosts, int[] ports)
	{
		if (hosts.length != ports.length)
		{
			System.err.println("The number of hosts do not mathch the number of ports");
			System.exit(-1);
		}

		CustomLogger.debug("Creating cluster endpoints");
		if (INSTANCE == null)
		{
			synchronized (ClusterEndpoints.class)
			{
				if (INSTANCE == null)
				{
					INSTANCE = new ClusterEndpoints(hosts, ports);

				}
			}
		}
		return INSTANCE;
	}

// Represents the RedisCluster of list.
	public class RedisCluster
	{
		private final String host;
		private final int port;
		RedisCluster next;

		public RedisCluster(String host, int port)
		{
			this.host = host;
			this.port = port;
		}

		public String getHost()
		{
			return host;
		}

		public int getPort()
		{
			return port;
		}

	}

	// Declaring head and tail pointer as null.
	private RedisCluster head = null;
	private RedisCluster tail = null;
	private RedisCluster current = head;

	private ClusterEndpoints(String[] hosts, int[] ports)
	{
		for (int i = 0; i < hosts.length; i++)
		{
			add(hosts[i], ports[i]);
		}

	}

	// This function will add the new RedisCluster at the end of the list.
	public void add(String host, int port)
	{
		// Create new RedisCluster
		RedisCluster newRedisCluster = new RedisCluster(host, port);
		// Checks if the list is empty.
		if (head == null)
		{
			// If list is empty, both head and tail would point to new RedisCluster.
			head = newRedisCluster;
			tail = newRedisCluster;
			newRedisCluster.next = head;
			current = head;
		} else
		{
			// tail will point to new RedisCluster.
			tail.next = newRedisCluster;
			// New RedisCluster will become new tail.
			tail = newRedisCluster;
			// Since, it is circular linked list tail will point to head.
			tail.next = head;
		}
	}

	public RedisCluster next()
	{
		if (head == null)
		{
			return null;
		}
		RedisCluster temp = current;
		current = current.next;

		return temp;

	}

	public void clear()
	{
		head = null;
		tail = null;
		current = null;

	}
	/**
	 * This could be the unit test
	 *
	 * public static void main(String[] args) { // Adds port to the list
	 *
	 * String[] s = { "a", "b", "c", "d" };
	 *
	 * int[] j = { 1, 2, 3, 4 };
	 *
	 * ClusterEndpoints cl = new ClusterEndpoints(s, j);
	 *
	 * // Displays all the RedisClusters present in the list for (int i = 0; i < 10;
	 * i++) { System.out.println(cl.next().getHost()); }
	 *
	 * cl.clear();
	 *
	 * cl.add("e", 11); cl.add("f", 12); cl.add("g", 13); cl.add("h", 14); //
	 * Displays all the RedisClusters present in the list for (int i = 0; i < 10;
	 * i++) { System.out.println(cl.next().getHost()); }
	 *
	 * }
	 */
}
