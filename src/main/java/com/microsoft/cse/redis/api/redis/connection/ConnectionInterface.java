package com.microsoft.cse.redis.api.redis.connection;

import com.microsoft.cse.redis.api.exceptions.RedisApiException;
import com.microsoft.cse.redis.api.helper.ClusterEndpoints;
import com.microsoft.cse.redis.api.helper.ConfigurationManager;

import redis.clients.jedis.Jedis;

/*
Generic Connection class that has all of the properties needed for the 3 SDKs
*/
@SuppressWarnings("unused")
public abstract class ConnectionInterface
{

	protected final ClusterEndpoints clusters;
	protected final String password;
	protected final int timeout;
	protected boolean serverSsl;
	public volatile boolean resetFlag = true;

	public ConnectionInterface(ClusterEndpoints clusters, String pass, int timeoutmsec)
	{
		this.clusters = clusters;
		password = pass;
		timeout = timeoutmsec;
		serverSsl = ConfigurationManager.getServerSideSSLEnabled();
	}

	public abstract Jedis getConnection() throws RedisApiException;

	public abstract void shutdown();

}
