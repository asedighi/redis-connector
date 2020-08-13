package com.microsoft.cse.redis.api.redis.connection.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.microsoft.cse.redis.api.helper.ConfigurationManager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;

public class SingleConnectionJedisPool extends Pool<Jedis>
{

	public SingleConnectionJedisPool(String host, int port, String password)
	{

		this(new GenericObjectPoolConfig(), host, port, password);
	}

	public SingleConnectionJedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
			final String password)
	{

		super(poolConfig, new SingleConnectionJedisFactory(host, port, ConfigurationManager.getConnectionTimeoutMSec(),
				ConfigurationManager.getConnectionTimeoutMSec(), password));

	}

	@Override
	protected void returnBrokenResource(final Jedis resource)
	{
		if (resource != null)
		{
			returnBrokenResourceObject(resource);
		}
	}

	@Override
	protected void returnResource(final Jedis resource)
	{
		if (resource != null)
		{
			try
			{
				resource.resetState();
				returnResourceObject(resource);
			} catch (Exception e)
			{
				returnBrokenResource(resource);
				throw new JedisException("Resource is returned to the pool as broken", e);
			}
		}
	}
}
