package com.microsoft.cse.redis.api.redis.connection.jedis;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import com.microsoft.cse.redis.api.helper.ClusterEndpoints;
import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.helper.RetryValues;
import com.microsoft.cse.redis.api.helper.ClusterEndpoints.RedisCluster;
import com.microsoft.cse.redis.api.redis.connection.ConnectionInterface;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class PooledConnection extends ConnectionInterface
{
	// Private member that contains the actual pool
	private JedisPool currentPool;
	private final ConcurrentLinkedQueue<JedisPool> pools;

	private final JedisPoolConfig config;
	private final int numPools;

	private static ScheduledExecutorService executor;

	private final Object lockObj = new Object();

	// Pool is created during the constructor phase.

	/**
	 * JedisPool does not create a Jedis connection in the connection pool when it
	 * defines the maximum number of resources and the minimum number of idle
	 * resources. When there is no idle connection in the pool, a new Jedis
	 * connection will be created. This connection will be released to the pool
	 * after it is used. However, creating a new connection and releasing it every
	 * time is a time-consuming process.
	 *
	 * @param host
	 * @param port
	 * @param numberOfConnection
	 * @param password
	 * @param timeout
	 */

	public PooledConnection(ClusterEndpoints clusters, int numberOfConnection, int numberofpool, String password,
			int timeout)
	{
		super(clusters, password, timeout);

		numPools = numberofpool;

		config = new JedisPoolConfig();
		config.setTestWhileIdle(true);

		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		config.setNumTestsPerEvictionRun(-1);
		config.setMaxTotal(numberOfConnection);
		config.setMaxIdle(numberOfConnection);
		config.setMinIdle(numberOfConnection / 2);
		config.setBlockWhenExhausted(true);

		pools = new ConcurrentLinkedQueue<>();
		for (int i = 0; i < numPools; i++)
		{
			RedisCluster rc = clusters.next();
			if (password == null)
			{
				pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, super.serverSsl));
			} else
			{
				pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, password, super.serverSsl));
			}
		}
		currentPool = pools.remove();

		if (executor == null)
		{
			executor = Executors.newScheduledThreadPool(2);

			executor.scheduleWithFixedDelay(() -> checkPool(), 0, ConfigurationManager.getConnectionCheckSec(),
					TimeUnit.MILLISECONDS);

			executor.scheduleWithFixedDelay(() -> resetPool(), 0, ConfigurationManager.getConnectionCheckSec(),
					TimeUnit.MILLISECONDS);

		}
	}

	/**
	 * renewLock only sets the TTL of the lock using a new connection from the
	 * connection pool and closing it afterwards.
	 */

	private void checkPool()
	{
		CustomLogger.debug("Checking to see if pools are ok");
		try
		{
			if (pools.isEmpty() && (currentPool != null))
			{
				for (int i = pools.size(); i < (numPools - 1); i++)
				{
					if (password == null)
					{
						synchronized (lockObj)
						{
							RedisCluster rc = clusters.next();

							pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, super.serverSsl));
						}

					} else
					{
						synchronized (lockObj)
						{
							RedisCluster rc = clusters.next();

							pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, password,
									super.serverSsl));
						}
					}
				}
			} else if (pools.isEmpty() && (currentPool == null))
			{
				for (int i = pools.size(); i < numPools; i++)
				{
					if (password == null)
					{
						synchronized (lockObj)
						{
							RedisCluster rc = clusters.next();

							pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, super.serverSsl));
						}

					} else
					{
						synchronized (lockObj)
						{
							RedisCluster rc = clusters.next();

							pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, password,
									super.serverSsl));
						}
					}
				}
			} else if ((pools.size() < (numPools - 1)) && (currentPool != null))
			{
				for (int i = pools.size(); i < (numPools - 1); i++)
				{
					if (password == null)
					{
						synchronized (lockObj)
						{
							RedisCluster rc = clusters.next();

							pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, super.serverSsl));
						}

					} else
					{
						synchronized (lockObj)
						{
							RedisCluster rc = clusters.next();

							pools.add(new JedisPool(config, rc.getHost(), rc.getPort(), timeout, password,
									super.serverSsl));
						}
					}
				}
			}

		} catch (Exception error)
		{
			CustomLogger.error("Checkpool failed: " + error.getMessage(), error);
		}
	}

	// Return a connection from the Pool
	// It blocks and waits when there are no more connections.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public Jedis getConnection()
	{
		CustomLogger.debug("In getConnection - getting a connection from pool");

		Jedis connection = null;
		try
		{
			if (currentPool != null)
			{
				connection = currentPool.getResource();
			}
		} catch (Exception ex)
		{
			CustomLogger.error("Error getting connection: " + ex.toString());
			throw ex;
		}

		return connection;
	}

	public JedisPool resetPool()
	{
		Jedis connection = null;
		try
		{
			connection = currentPool.getResource();
			connection.ping();

		} catch (Exception ex)
		{
			CustomLogger.error("Error pinging, resetting the connection: " + ex.toString());

			if (currentPool != null)
			{
				currentPool.destroy();
				currentPool = null;
			}

			while (pools.isEmpty())
			{
				CustomLogger.debug("No connection pools left... will go to sleep and wake up to check in a min");

				try
				{
					Thread.sleep(5L * timeout);
				} catch (InterruptedException e)
				{
					CustomLogger.error("Exception in sleeping for pool creation..." + e.getMessage(), e);

				}
			}

			if (!pools.isEmpty() && (currentPool == null))
			{
				currentPool = pools.remove();
			}
		} finally
		{
			if (connection != null)
			{
				connection.close();
			}
		}

		return currentPool;

	}

	/**
	 * private synchronized JedisPool getPool() { while (pools.isEmpty()) {
	 * CustomLogger.debug("No connection pools left... will go to sleep and wake up
	 * to check in a min");
	 *
	 * try { Thread.sleep(5L * timeout); } catch (InterruptedException e) {
	 * CustomLogger.error("Exception in sleeping for pool creation..." +
	 * e.getMessage(), e);
	 *
	 * } }
	 *
	 * if (!pools.isEmpty() && (currentPool == null)) { currentPool =
	 * pools.remove(); } semaphore.release();
	 *
	 * return currentPool;
	 *
	 * }
	 */
	@Override
	public void shutdown()
	{

		CustomLogger.debug("Shutting everything down");
		try
		{
			for (int i = 0; i < pools.size(); i++)
			{

				JedisPool jp = pools.remove();
				jp.destroy();

			}
			executor.shutdown();

		} catch (Exception error)
		{
			CustomLogger.error("Checkpool failed: " + error.getMessage(), error);
		}

	}
}
