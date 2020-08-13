package com.microsoft.cse.redis.api.redis.data;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.helper.RetryValues;
import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;
import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory.conn_type;

public abstract class DataFactory
{

	protected final ConnectionFactory conn;

	private static volatile DataFactory INSTANCE;

	// This is a HORRIBLE HORRIBLE HORRIBLE idea. Did i Say horrible. But the darn
	// static checker likes it!
	protected static final String CAUGHTANEXCEPTION = "Caught an exception: ";

	// Singleton DataFactory instance bound to a specific Redis SDK
	public static DataFactory getDataFactory(ConnectionFactory connection)
	{
		if (INSTANCE == null)
		{
			synchronized (DataFactory.class)
			{
				if (INSTANCE == null)
				{
					if (connection.getSelectedType() == conn_type.SPRING)
					{
						INSTANCE = new SpringDataFactory(connection);
					} else if (connection.getSelectedType() == conn_type.JEDIS)
					{
						INSTANCE = new RedisDataFactory(connection);
					} else if (connection.getSelectedType() == conn_type.JEDISPOOL)
					{
						INSTANCE = new PooledRedisDataFactory(connection);
					} else if (connection.getSelectedType() == conn_type.JEDISPIPELINED)
					{
						INSTANCE = new PipelinedPooledRedisDataFactory(connection);
					} else
					{
						INSTANCE = new PooledRedisDataFactory(connection);
					}

				}
			}
		}
		return INSTANCE;
	}

	protected DataFactory(ConnectionFactory connection)
	{
		CustomLogger.debug("Creating a data factory");

		conn = connection;

	}

	// Simple Redis Key/Value operation. Based on the SDK type, different operations
	// are invoked to set the key to the specified value.
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract void set(String key, String value);

	// Simple Redis Set operation to add the value to the top of a Set
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract void setAdd(String key, String value);

	// Redis Set operation to add an entire array of strings to the top of a set.

	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract void setUnsortedSet(String key, List<String> values);

	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract Set<String> getUnsortedSet(String key);

	// Redis hashmap setthat overwrites an existing hash set with a new one with the
	// specified key.
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract void setMultimap(String key, Map<String, String> values);

	// Redis SortedSet operation that adds the value to the appropriate location in
	// the set based on score.
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract void setZSet(String key, String value, Double score);

	// Simplest Redis operation to get the value of a key in a typical name/value
	// pair.
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract String get(String key);

	// Get the HashSet associated with a key
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract Map<String, String> getMultimap(String key);

	// Get the HashSet associated with a set of keys
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract Map<String, Map<String, String>> getMultimap(Set<String> keys);

	// Gets the list of items that were previously added to an unordered Set
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract List<String> getMultivalue(String key);

	// Get's the list of items that were added to a Sorted Set for a specific key
	// and score value.
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract Set<String> getZSet(String key, Long minScore, Long maxScore);

	// Remove the specified key/value pair from Redis
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long remove(String key);

	// Remove the ordered set values for a specific key and score.
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long removeZSet(String key, double minScore, double maxScore);

	// Remove the spcified value from the order set (the key)
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long removeZSet(String key, String value);

	// Remove the specified value from the unordered set (the key)
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long removeSetValue(String key, String value);

	/**
	 * Sets a value if it doesn't exist.
	 *
	 * @param key
	 * @param value
	 * @return 0 if it was not set, meaning the item already exists. 1 when
	 *         sucessfully created the item. -1 when error.
	 */
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long setIfNotExists(String key, String value);

	/**
	 * Sets the TTL for a specific key.
	 *
	 * @param key
	 * @param timeout TTL value, in seconds
	 * @return
	 */
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long setExpiration(String key, Integer timeout);

	/**
	 * Deletes a key.
	 *
	 * @param key
	 * @return 1 if succeeds, 0 if fails.
	 */
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long delete(String key);

	/**
	 * Sets a hash set value if it does not exist
	 *
	 * @param hashSetName hash set name
	 * @param fieldName   field name
	 * @param value       value
	 * @return 0 if it was not set, meaning the item already exists. 1 when
	 *         sucessfully created the item. -1 when error.
	 */
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long setHashSetValueIfNotExists(String hashSetName, String fieldName, String value);

	/**
	 * Sets a hash set values if they don't exist. Do it using a pipeline.
	 *
	 * @param hashSetName hash set names
	 * @param fieldName   field name
	 * @param value       value
	 * @return 0 if it was not set, meaning the item already exists. 1 when
	 *         sucessfully created the item. -1 when error.
	 */
	public abstract Map<String, Long> setHashSetValueIfNotExists(Set<String> hashSetName, String fieldName,
			String value);

	/**
	 * Deletes a hash field value.
	 *
	 * @param hashSetName hash set name
	 * @param fieldName   field name
	 * @return 1 if succeeds, 0 if fails.
	 */
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long deleteHashSetValue(String hashSetName, String fieldName);

	/**
	 * Deletes hash field values.
	 *
	 * @param hashSetName hash set name
	 * @param fieldName   field name
	 * @return 1 if succeeds, 0 if fails.
	 */
	public abstract Map<String, Long> deleteHashSetValue(Set<String> hashSetNames, String fieldName);

	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public abstract long setIfNotExists(String key, String value, int ttl);

	public abstract long deleteFromMultivalue(String key, String keytoremove);

	public abstract long globalIncrement();

}