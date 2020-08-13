package com.microsoft.cse.redis.unitTests;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;

/**
 * This test class tests the ConnectionFactory.
 */
public class ConnectionFactoryTest
{

	/**
	 * Sets up the connection to Redis.
	 */
	@BeforeAll
	static void setup()
	{
		TestBase.setup();
	}

	/**
	 * Tests that the same Connection Factory is returned through the
	 * "getConnection" method.
	 */
	@Test
	public void shouldGetConnectionInstance()
	{
		assertSame(TestBase.getConnectionFactory(), ConnectionFactory.getConnection());
	}

	/**
	 * Shutsdown the connection to Redis.
	 */
	@AfterAll
	static void tearDown()
	{
		TestBase.teardown();
	}

}
