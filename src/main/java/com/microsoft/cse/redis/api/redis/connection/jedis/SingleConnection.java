package com.microsoft.cse.redis.api.redis.connection.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.microsoft.cse.redis.api.helper.ClusterEndpoints;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.helper.ClusterEndpoints.RedisCluster;
import com.microsoft.cse.redis.api.redis.connection.ConnectionInterface;

import redis.clients.jedis.Jedis;

public class SingleConnection extends ConnectionInterface
{
	// The one and only connection.

	private final SingleConnectionJedisPool pools;

	public SingleConnection(ClusterEndpoints clusters, String password, int numberofpools, int timeout)
	{
		super(clusters, password, timeout);

		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxIdle(1);
		config.setMaxTotal(numberofpools);

		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		RedisCluster rc = clusters.next();

		pools = new SingleConnectionJedisPool(config, rc.getHost(), rc.getPort(), password);

	}

	/**
	 *
	 * /** renewLock only sets the TTL of the lock using a new connection from the
	 * connection pool and closing it afterwards.
	 */
	// Return the same connection each time.
	// TODO: What happens if two threads use the same connection at the same time?
	@Override
	public Jedis getConnection()
	{
		CustomLogger.debug("In getConnection - getting a connection from pool");

		Jedis conn = null;
		conn = pools.getResource();
		conn.connect();
		return conn;
	}

	public void resetPool(Jedis conn)
	{

		conn.disconnect();

	}

	public void returnConn(Jedis conn)
	{

		if (conn == null)
		{
			return;
		}
		pools.returnResource(conn);
	}

	@Override
	public void shutdown()
	{
		pools.close();

	}

}
