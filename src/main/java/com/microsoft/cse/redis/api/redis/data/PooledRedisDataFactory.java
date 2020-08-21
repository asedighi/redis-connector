package com.microsoft.cse.redis.api.redis.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import com.jcabi.log.Logger;
import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.helper.RetryValues;
import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;
import com.microsoft.cse.redis.api.redis.connection.jedis.PooledConnection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class PooledRedisDataFactory extends DataFactory
{

	protected PooledConnection pooledConnection;

	public PooledRedisDataFactory(ConnectionFactory conn)
	{
		super(conn);
	}

	// Simple Redis Key/Value operation. Based on the SDK type, different operations
	// are invoked to set the key to the specified value.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public void set(String key, String value)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in set");
			return;
		}

		CustomLogger.debug("Setting a value for key set: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			jd.setex(key, ConfigurationManager.getRedisDataExpireSec(), value);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public long globalIncrement()
	{

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			String h = jd.getClient().getHost();
			int hc = (h.hashCode() & 0x7fffffff) % 1000;

			/**
			 * This is the old way of doing increement. But it does not work and we need to
			 * use increment List<Long> responses = jd.bitfield("globalIncrement", "INCRBY",
			 * "u32", "0", "1", "OVERFLOW", "WRAP"); return responses.get(0);
			 */
			return jd.incr("global_id_number") + hc;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}
	}

	// Simple Redis Set operation to add the value to the top of a Set
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public void setAdd(String key, String value)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setArray");
			return;
		}

		CustomLogger.debug("Setting a value for keys setAdd: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			jd.lpush(key, value);
			jd.expire(key, ConfigurationManager.getRedisDataExpireSec());

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}
	}

	// Redis Set operation to add an entire array of strings to the top of a set.
	// TODO: the name setSet is somewhat misleading here as we are adding items to a
	// set.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public void setUnsortedSet(String key, List<String> values)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setUnsortedSet");
			return;
		}

		CustomLogger.debug("Setting a value for key: " + key);
		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();

			String[] array = values.toArray(new String[values.size()]);

			jd.sadd(key, array);
			jd.expire(key, ConfigurationManager.getRedisDataExpireSec());

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}
	}

	// Redis hashmap setthat overwrites an existing hash set with a new one with the
	// specified key.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public void setMultimap(String key, Map<String, String> values)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setMultimap");
			return;
		}

		CustomLogger.debug("Setting a multimap value for key: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			jd.hmset(key, values);
			jd.expire(key, ConfigurationManager.getRedisDataExpireSec());

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}
	}

	// Redis SortedSet operation that adds the value to the appropriate location in
	// the set based on score.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public void setZSet(String key, String value, Double score)
	{

		if ((key == null) || (value == null))
		{
			CustomLogger.error("Received a null key in setZSet");
			return;
		}

		CustomLogger.debug("Setting a sorted set for key: " + key + " score: " + score);
		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			ConcurrentHashMap<String, Double> tempmap = new ConcurrentHashMap<>();
			tempmap.put(value, score);
			jd.zadd(key, tempmap);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Simplest Redis operation to get the value of a key in a typical name/value
	// pair.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public String get(String key)
	{
		if (key == null)
		{
			return null;
		}

		CustomLogger.debug("Looking up key: " + key);
		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.get(key);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Get the HashSet associated with a key
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public Map<String, String> getMultimap(String key)
	{

		if (key == null)
		{
			return null;
		}

		CustomLogger.debug("looking up key to get a map: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.hgetAll(key);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Gets the list of items that were previously added to an unordered Set
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public List<String> getMultivalue(String key)
	{

		if (key == null)
		{
			return null;
		}

		CustomLogger.debug("looking up key to get a map: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.lrange(key, 0, -1);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Get's the list of items that were added to a Sorted Set for a specific key
	// and score value.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public Set<String> getZSet(String key, Long minScore, Long maxScore)
	{
		if ((key == null) || (maxScore < minScore) || (minScore < 0) || (maxScore < 0))
		{
			return null;
		}

		CustomLogger.debug("Getting based on scores: " + minScore + ":" + maxScore);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.zrangeByScore(key, minScore, maxScore);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Remove the specified key/value pair from Redis
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long remove(String key)
	{
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing key: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.unlink(key);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Remove the ordered set values for a specific key and score.
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long removeZSet(String key, double minScore, double maxScore)
	{
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing key: " + key + " based on scores: " + minScore + ":" + maxScore);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.zremrangeByScore(key, minScore, maxScore);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Remove the spcified value from the order set (the key)
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long removeZSet(String key, String value)
	{
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing value " + value + " from set " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.zrem(key, value);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	// Remove the specified value from the unordered set (the key)
	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long removeSetValue(String key, String value)
	{
		if (key == null)
		{
			return 0L;
		}

		CustomLogger.debug("Removing value " + value + " from set " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();

			return jd.srem(key, value);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long setIfNotExists(String key, String value)
	{
		return setIfNotExists(key, value, ConfigurationManager.getRedisDataExpireSec());
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long setIfNotExists(String key, String value, int ttl)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setIfNotExists");
			return -1;
		}

		CustomLogger.debug("Setting a value for key in setIfNotExists: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}

		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			Long l = jd.setnx(key, value);

			// don't set TTL if it wasn't set
			if (l > 0)
			{
				jd.expire(key, ttl);
			}

			return l;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long setExpiration(String key, Integer timeout)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setExpiration");
			return -1;
		}

		CustomLogger.debug("Setting expiration for key: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}

		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.expire(key, timeout);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long delete(String key)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in delete");
			return -1;
		}

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}

		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.del(key);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long setHashSetValueIfNotExists(String hashSetName, String fieldName, String value)
	{

		if (hashSetName == null)
		{
			CustomLogger.error("Received a null hashSetName in setHashSetValueIfNotExists");
			return -1;
		}

		CustomLogger.debug("Setting " + fieldName + " field for hash set " + hashSetName);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}

		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.hsetnx(hashSetName, fieldName, value);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long deleteHashSetValue(String hashSetName, String fieldName)
	{

		if (hashSetName == null)
		{
			CustomLogger.error("Received a null hashSetName in deleteHashSetValue");
			return -1;
		}

		CustomLogger.debug("Setting " + fieldName + " field for hash set " + hashSetName);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}

		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.hdel(hashSetName, fieldName);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long deleteFromMultivalue(String key, String keytoremove)
	{
		if ((key == null) || (keytoremove == null))
		{
			CustomLogger.error("Received a null key in keytoremove");
			return -1;
		}

		CustomLogger.debug("Removing " + key + " and value " + keytoremove);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}

		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();
			return jd.lrem(key, 0, keytoremove);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public Set<String> getUnsortedSet(String key)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setUnsortedSet");
			return null;
		}

		CustomLogger.debug("Setting a value for key getUnsortedSet: " + key);

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		try
		{
			jd = pooledConnection.getConnection();

			return jd.smembers(key);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
	public Map<String, Map<String, String>> getMultimap(Set<String> keys)
	{

		if (keys == null)
		{
			CustomLogger.error("Received a null key in getMultimapBatched");
			return null;
		}

		CustomLogger.debug("Get values for this many keys in getMultimapBatched: " + keys.size());

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		Map<String, Map<String, String>> ret = new HashMap<>();
		Map<String, Response<Map<String, String>>> responses = new HashMap<>();

		try
		{
			jd = pooledConnection.getConnection();
			Pipeline p = jd.pipelined();

			for (String key : keys)
			{
				responses.put(key, p.hgetAll(key));
			}
			if (p != null)
			{
				p.sync();
				p.close();
				p.clear();

			}

			for (String key : keys)
			{
				ret.put(key, responses.get(key).get());
			}

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

		return ret;
	}

	@Override
	public Map<String, Long> setHashSetValueIfNotExists(Set<String> hashSetNames, String fieldName, String value)
	{
		if (hashSetNames == null)
		{
			CustomLogger.error("Received a null hashSetNames in setHashSetValueIfNotExists");
			return null;
		}

		CustomLogger.debug("Get values for this many keys in setHashSetValueIfNotExists: " + hashSetNames.size());

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		Map<String, Long> ret = new HashMap<>();
		Map<String, Response<Long>> responses = new HashMap<>();

		try
		{
			jd = pooledConnection.getConnection();
			Pipeline p = jd.pipelined();

			for (String key : hashSetNames)
			{
				responses.put(key, p.hsetnx(key, fieldName, value));
			}
			if (p != null)
			{
				p.sync();
				p.close();
				p.clear();

			}

			for (String key : hashSetNames)
			{
				ret.put(key, responses.get(key).get());
			}

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

		return ret;
	}

	@Override
	public Map<String, Long> deleteHashSetValue(Set<String> hashSetNames, String fieldName)
	{
		if (hashSetNames == null)
		{
			CustomLogger.error("Received a null hashSetNames in deleteHashSetValue");
			return null;
		}

		CustomLogger.debug("Get values for this many keys in deleteHashSetValue: " + hashSetNames.size());

		if (pooledConnection == null)
		{
			pooledConnection = conn.getJedisPooledConnection();
		}
		Jedis jd = null;

		Map<String, Long> ret = new HashMap<>();
		Map<String, Response<Long>> responses = new HashMap<>();

		try
		{
			jd = pooledConnection.getConnection();
			Pipeline p = jd.pipelined();

			for (String key : hashSetNames)
			{
				responses.put(key, p.hdel(key, fieldName));
			}
			if (p != null)
			{
				p.sync();
				p.close();
				p.clear();

			}

			for (String key : hashSetNames)
			{
				ret.put(key, responses.get(key).get());
			}

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		} finally
		{
			if (jd != null)
			{
				jd.close();
			}
		}

		return ret;
	}
}
