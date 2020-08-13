package com.microsoft.cse.redis.api.redis.data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.helper.RetryValues;
import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class PipelinedPooledRedisDataFactory extends PooledRedisDataFactory
{
	private static ThreadLocal<Pipeline> pipeline = ThreadLocal.withInitial(() -> null);
	private static ThreadLocal<Jedis> connection = ThreadLocal.withInitial(() -> null);

	public PipelinedPooledRedisDataFactory(ConnectionFactory conn)
	{
		super(conn);
	}

	public void startPipeline()
	{
		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		connection.set(pooledConnection.getConnection());
		Pipeline p = connection.get().pipelined();
		if (p != null)
		{
			pipeline.set(p);
		}

	}

	public void endPipeline()
	{

		Pipeline p = pipeline.get();
		if (p != null)
		{
			p.sync();
			p.close();
			p.clear();
			pipeline.remove();

			connection.get().close();
			connection.remove();
		}

	}

	private Pipeline getPipeline()
	{
		return pipeline.get();

	}

	private void resetPipeline()
	{
		pipeline.remove();
		connection.get().close();
		connection.remove();

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public void set(String key, String value)
	{
		if (getPipeline() == null)
		{
			super.set(key, value);
			return;
		}
		try
		{
			getPipeline().setex(key, ConfigurationManager.getRedisDataExpireSec(), value);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}
	}

	// Simple Redis Set operation to add the value to the top of a Set
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public void setAdd(String key, String value)
	{
		if (getPipeline() == null)
		{
			super.setAdd(key, value);
			return;
		}

		if (key == null)
		{
			CustomLogger.error("Received a null key in setArray");
			return;
		}

		CustomLogger.debug("Setting a value for keys setAdd: " + key);

		try
		{
			getPipeline().lpush(key, value);
			getPipeline().expire(key, ConfigurationManager.getRedisDataExpireSec());

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}
	}

	// Redis Set operation to add an entire array of strings to the top of a set.
	// TODO: the name setSet is somewhat misleading here as we are adding items to a
	// set.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public void setUnsortedSet(String key, List<String> values)
	{
		if (getPipeline() == null)
		{
			super.setUnsortedSet(key, values);
			return;
		}

		if (key == null)
		{
			CustomLogger.error("Received a null key in setUnsortedSet");
			return;
		}

		CustomLogger.debug("Setting a value for key: " + key);

		try
		{

			String[] array = values.toArray(new String[values.size()]);

			getPipeline().sadd(key, array);
			getPipeline().expire(key, ConfigurationManager.getRedisDataExpireSec());

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}
	}

	// Redis hashmap setthat overwrites an existing hash set with a new one with the
	// specified key.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public void setMultimap(String key, Map<String, String> values)
	{
		if (getPipeline() == null)
		{
			super.setMultimap(key, values);
			return;
		}
		if (key == null)
		{
			CustomLogger.error("Received a null key in setMultimap");
			return;
		}

		CustomLogger.debug("Setting a multimap value for key: " + key);

		try
		{
			getPipeline().hmset(key, values);
			getPipeline().expire(key, ConfigurationManager.getRedisDataExpireSec());

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}
	}

	// Redis SortedSet operation that adds the value to the appropriate location in
	// the set based on score.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public void setZSet(String key, String value, Double score)
	{
		if (getPipeline() == null)
		{
			super.setZSet(key, value, score);
			return;
		}

		if ((key == null) || (value == null))
		{
			CustomLogger.error("Received a null key in setZSet");
			return;
		}

		CustomLogger.debug("Setting a sorted set for key: " + key + " score: " + score);

		try
		{
			ConcurrentHashMap<String, Double> tempmap = new ConcurrentHashMap<>();
			tempmap.put(value, score);
			getPipeline().zadd(key, tempmap);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}

	}

	// Remove the specified key/value pair from Redis
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long remove(String key)
	{
		if (getPipeline() == null)
		{
			return super.remove(key);

		}
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing key: " + key);

		try
		{
			getPipeline().unlink(key);
			return 1;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}

	}

	// Remove the ordered set values for a specific key and score.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long removeZSet(String key, double minScore, double maxScore)
	{
		if (getPipeline() == null)
		{
			return super.removeZSet(key, minScore, maxScore);

		}
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing key: " + key + " based on scores: " + minScore + ":" + maxScore);

		try
		{
			getPipeline().zremrangeByScore(key, minScore, maxScore);
			return 1;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}

	}

	// Remove the spcified value from the order set (the key)
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long removeZSet(String key, String value)
	{
		if (getPipeline() == null)
		{
			return super.removeZSet(key, value);

		}
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing value " + value + " from set " + key);

		try
		{

			getPipeline().zrem(key, value);
			return 1;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}

	}

	// Remove the specified value from the unordered set (the key)
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long removeSetValue(String key, String value)
	{
		if (getPipeline() == null)
		{
			return super.removeSetValue(key, value);

		}
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing value " + value + " from set " + key);

		try
		{
			getPipeline().srem(key, value);
			return 1;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long setExpiration(String key, Integer timeout)
	{
		if (getPipeline() == null)
		{
			return super.setExpiration(key, timeout);

		}

		if (key == null)
		{
			CustomLogger.error("Received a null key in setExpiration");
			return -1;
		}

		CustomLogger.debug("Setting expiration for key: " + key);

		try
		{
			getPipeline().expire(key, timeout);
			return 1;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long delete(String key)
	{
		if (getPipeline() == null)
		{
			return super.delete(key);

		}

		if (key == null)
		{
			CustomLogger.error("Received a null key in delete");
			return -1;
		}

		try
		{
			getPipeline().del(key);
			return 1;
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long setHashSetValueIfNotExists(String hashSetName, String fieldName, String value)
	{
		if (getPipeline() == null)
		{
			return super.setHashSetValueIfNotExists(hashSetName, fieldName, value);

		}

		if (hashSetName == null)
		{
			CustomLogger.error("Received a null hashSetName in setHashSetValueIfNotExists");
			return -1;
		}

		CustomLogger.debug("Setting " + fieldName + " field for hash set " + hashSetName);

		try
		{
			getPipeline().hsetnx(hashSetName, fieldName, value);
			return 1;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long deleteHashSetValue(String hashSetName, String fieldName)
	{
		if (getPipeline() == null)
		{
			return super.deleteHashSetValue(hashSetName, fieldName);

		}

		if (hashSetName == null)
		{
			CustomLogger.error("Received a null hashSetName in deleteHashSetValue");
			return -1;
		}

		CustomLogger.debug("Setting " + fieldName + " field for hash set " + hashSetName);

		try
		{

			getPipeline().hdel(hashSetName, fieldName);
			return 1;
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long deleteFromMultivalue(String key, String keytoremove)
	{
		if (getPipeline() == null)
		{
			return super.deleteFromMultivalue(key, keytoremove);

		}
		if ((key == null) || (keytoremove == null))
		{
			CustomLogger.error("Received a null key in keytoremove");
			return -1;
		}

		CustomLogger.debug("Removing " + key + " and value " + keytoremove);

		try
		{

			getPipeline().lrem(key, 0, keytoremove);
			return 1;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			endPipeline();
			resetPipeline();
			conn.resetConnection();
			throw e;

		}
	}

}
