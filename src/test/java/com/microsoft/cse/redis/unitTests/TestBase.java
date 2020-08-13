package com.microsoft.cse.redis.unitTests;

import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;

/**
 * This class includes methods that set up / shut down the Connection Factory
 * used in the tests.
 */
public abstract class TestBase {
	private static ConnectionFactory connectionFactory;

	static ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	static void setup() {
		connectionFactory = ConnectionFactory.getPipelinedJedisPooledInstance(ConfigurationManager.getHostname(),
				ConfigurationManager.getPortnumber(), ConfigurationManager.getPassword(), 30, 1,
				ConfigurationManager.getConnectionTimeoutMSec());
	}

	static void teardown() {
		ConnectionFactory.getConnection().getJedisPooledConnection().shutdown();
	}
}