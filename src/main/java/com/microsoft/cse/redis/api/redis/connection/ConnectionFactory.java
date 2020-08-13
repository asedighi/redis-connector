package com.microsoft.cse.redis.api.redis.connection;

import com.microsoft.cse.redis.api.helper.ClusterEndpoints;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.redis.connection.jedis.PooledConnection;
import com.microsoft.cse.redis.api.redis.connection.jedis.SingleConnection;
import com.microsoft.cse.redis.api.redis.connection.spring.SpringConnection;

/*
This is where the magic happens and depending on the type of Redis SDK requested, the appropriate connection
is created. Jedis has a choice of Pooled and Single instance. Lettuce and Spring are pooled by default.
*/
public class ConnectionFactory
{

	public enum conn_type
	{
		JEDIS, JEDISPOOL, SPRING, JEDISPIPELINED
	}

	private static volatile ConnectionFactory INSTANCE = null;

	private final conn_type selected;

	private SpringConnection springConnection;

	private PooledConnection pooledConnection;

	private SingleConnection jConnection;

	private final String password;

	private final int poolsize;

	private final int numberofpools;

	private final int timeoutmsec;

	private final ClusterEndpoints clusters;

	public static ConnectionFactory getJedisInstance(String[] host, int[] port, String password, int numberofpools,
			int timeoutmsec)
	{

		CustomLogger.debug("Requesting a Jedis connections");
		if (INSTANCE == null)
		{
			synchronized (ConnectionFactory.class)
			{
				if (INSTANCE == null)
				{
					INSTANCE = new ConnectionFactory(conn_type.JEDIS, host, port, password, 1, numberofpools,
							timeoutmsec);
				}
			}
		}
		return INSTANCE;
	}

	public static ConnectionFactory getJedisPooledInstance(String[] host, int[] port, String password, int poolsize,
			int numberofpools, int timeoutmsec)
	{
		CustomLogger.debug("Requesting a Jedis-pooled connections");

		if (INSTANCE == null)
		{
			synchronized (ConnectionFactory.class)
			{
				if (INSTANCE == null)
				{
					INSTANCE = new ConnectionFactory(conn_type.JEDISPOOL, host, port, password, poolsize, numberofpools,
							timeoutmsec);
				}
			}
		}
		return INSTANCE;
	}

	public static ConnectionFactory getPipelinedJedisPooledInstance(String[] host, int[] port, String password,
			int poolsize, int numberofpools, int timeoutmsec)
	{
		CustomLogger.debug("Requesting a Jedis-pooled connections");

		if (INSTANCE == null)
		{
			synchronized (ConnectionFactory.class)
			{
				if (INSTANCE == null)
				{
					INSTANCE = new ConnectionFactory(conn_type.JEDISPIPELINED, host, port, password, poolsize,
							numberofpools, timeoutmsec);
				}
			}
		}
		return INSTANCE;
	}

	public static ConnectionFactory getSpringInstance(String[] host, int[] port, String password, int timeoutmsec)
	{
		CustomLogger.debug("Requesting a Spring connections");

		if (INSTANCE == null)
		{
			synchronized (ConnectionFactory.class)
			{
				if (INSTANCE == null)
				{
					INSTANCE = new ConnectionFactory(conn_type.SPRING, host, port, password, 10, 0, timeoutmsec);
				}
			}
		}
		return INSTANCE;
	}

	public static ConnectionFactory getConnection()
	{
		if (INSTANCE != null)
		{
			return ConnectionFactory.INSTANCE;
		} else
		{
			CustomLogger.debug("All connections are NULL");
			return null;
		}
	}

	private ConnectionFactory(conn_type t, String[] host, int[] port, String password, int poolsize, int numberofpools,
			int timeoutmsec)
	{
		this.password = password;
		this.poolsize = poolsize;
		this.timeoutmsec = timeoutmsec;
		this.numberofpools = numberofpools;
		selected = t;

		clusters = ClusterEndpoints.clusterEndpoints(host, port);

		CustomLogger.debug("Requesting a connection of type: " + t);
		CustomLogger.debug("Requesting a connection to: " + host);
		CustomLogger.debug("Requesting a connection to port: " + port);
		CustomLogger.debug("Requesting a connection with pool size: " + poolsize);
		CustomLogger.debug("Requesting a connection with timeout of: " + timeoutmsec);

		switch (selected) {
		case JEDIS:
			jConnection = new SingleConnection(clusters, password, numberofpools, timeoutmsec);
			break;
		case JEDISPOOL:
		case JEDISPIPELINED:
			pooledConnection = new PooledConnection(clusters, poolsize, numberofpools, password, timeoutmsec);
			break;
		case SPRING:
			springConnection = new SpringConnection(clusters, poolsize, password, timeoutmsec);
			break;
		}
	}

	public conn_type getSelectedType()
	{

		return selected;

	}

	public void resetConnection()
	{
		CustomLogger.error("Resetting ALL connections");

		if (pooledConnection != null)
		{
			// pooledConnection.resetPool();
		}

		if (springConnection != null)
		{
			springConnection = null;
		}

	}

	public SingleConnection getJedisConnection()
	{
		if (jConnection == null)
		{
			CustomLogger.debug("Jedis connection NULL, creating a new one");

			jConnection = new SingleConnection(clusters, password, numberofpools, timeoutmsec);
		}
		return jConnection;
	}

	public PooledConnection getJedisPooledConnection()
	{
		CustomLogger.debug("Jedis-pooled connection NULL, creating a new one");

		if (pooledConnection == null)
		{
			pooledConnection = new PooledConnection(clusters, poolsize, numberofpools, password, timeoutmsec);
		}
		return pooledConnection;
	}

	public SpringConnection getSpringConnection()
	{
		CustomLogger.debug("Spring connection NULL, creating a new one");

		if (springConnection == null)
		{
			springConnection = new SpringConnection(clusters, poolsize, password, timeoutmsec);
		}

		return springConnection;
	}

}
