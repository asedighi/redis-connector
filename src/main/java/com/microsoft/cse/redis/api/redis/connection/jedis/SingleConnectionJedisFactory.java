package com.microsoft.cse.redis.api.redis.connection.jedis;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.helper.CustomLogger;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * PoolableObjectFactory custom impl.
 */
class SingleConnectionJedisFactory implements PooledObjectFactory<Jedis>
{
	private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<>();
	private final int connectionTimeout;
	private final int soTimeout;
	private final String password;

	public SingleConnectionJedisFactory(final String host, final int port, final int connectionTimeout,
			final int soTimeout, final String password)
	{
		hostAndPort.set(new HostAndPort(host, port));
		this.connectionTimeout = connectionTimeout;
		this.soTimeout = soTimeout;
		this.password = password;
	}

	public void setHostAndPort(final HostAndPort hostAndPort)
	{
		this.hostAndPort.set(hostAndPort);
	}

	@Override
	public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception
	{
		// DO NOT REMOVE!
		// final BinaryJedis jedis = pooledJedis.getObject();
		// if (jedis.getDB() != database)
		// {
		// jedis.select(database);
		// }
	}

	@Override
	public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception
	{
		final BinaryJedis jedis = pooledJedis.getObject();
		if (jedis.isConnected())
		{
			try
			{
				jedis.quit();
			} catch (Exception e)
			{
				CustomLogger.error("Exception in destrying pool: " + e);

			}
			jedis.disconnect();
		}

	}

	@Override
	public PooledObject<Jedis> makeObject() throws Exception
	{
		final HostAndPort hp = hostAndPort.get();
		final Jedis jedis = new Jedis(hp.getHost(), hp.getPort(), connectionTimeout, soTimeout,
				ConfigurationManager.getServerSideSSLEnabled());
		jedis.auth(password);

		try
		{
			jedis.connect();

		} catch (JedisException je)
		{
			jedis.close();
			throw je;
		}

		return new DefaultPooledObject<>(jedis);
	}

	@Override
	public void passivateObject(PooledObject<Jedis> pooledJedis) throws Exception
	{
		// TODO maybe should select db 0? Not sure right now.
	}

	@Override
	public boolean validateObject(PooledObject<Jedis> pooledJedis)
	{
		final BinaryJedis jedis = pooledJedis.getObject();
		try
		{
			HostAndPort hostPort = hostAndPort.get();

			String connectionHost = jedis.getClient().getHost();
			int connectionPort = jedis.getClient().getPort();

			return hostPort.getHost().equals(connectionHost) && (hostPort.getPort() == connectionPort)
					&& jedis.isConnected() && jedis.ping().equals("PONG");
		} catch (final Exception e)
		{
			return false;
		}
	}
}