package com.microsoft.cse.redis.api.redis.data;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import com.jcabi.aspects.RetryOnFailure;
import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.helper.RetryValues;
import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;
import com.microsoft.cse.redis.api.redis.connection.spring.SpringConnection;

public class SpringDataFactory extends DataFactory
{
	private SpringConnection springConnection;

	public SpringDataFactory(ConnectionFactory conn)

	{
		super(conn);
	}

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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			ValueOperations<String, String> values = st.opsForValue();
			values.set(key, value, ConfigurationManager.getRedisDataExpireSec());

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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

		CustomLogger.debug("Setting a value for keys setAdd:  " + key);
		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ListOperations<String, String> v = st.opsForList();

			v.leftPush(key, value);

			st.expire(key, Duration.ofSeconds(ConfigurationManager.getRedisDataExpireSec()));

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
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

		if (key == null)
		{
			CustomLogger.error("Received a null key in setMultimap");
			return;
		}

		CustomLogger.debug("Setting a multimap value for key: " + key);

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, Map<String, String>> st = springConnection.transactionRedisTemplate();
			HashOperations<String, Object, Object> v = st.opsForHash();
			v.putAll(key, values);
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
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

		if (key == null)
		{
			CustomLogger.error("Received a null key in setZSet");
			return;
		}

		CustomLogger.debug("Setting a sorted set for key: " + key + " score: " + score);
		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ZSetOperations<String, String> v = st.opsForZSet();
			v.add(key, value, score);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;
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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			ValueOperations<String, String> values = st.opsForValue();
			return values.get(key);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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
		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, Map<String, String>> st = springConnection.transactionRedisTemplate();
			// ValueOperations<String, Map<String, String>> values = st.opsForValue();
			// return values.get(key);

			HashOperations<String, Object, Object> v = st.opsForHash();
			// return (((Map<String, String>)

			Map<Object, Object> map = v.entries(key);//
			Map<String, String> newMap = new HashMap<>();
			for (Map.Entry<Object, Object> entry : map.entrySet())
			{
				if (entry.getValue() instanceof String)
				{
					newMap.put((String) entry.getKey(), (String) entry.getValue());
				}
			}
			return newMap;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ValueOperations<String, String> values = st.opsForValue();
			return values.getOperations().opsForList().range(key, 0, -1);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ZSetOperations<String, String> v = st.opsForZSet();

			return v.rangeByScore(key, minScore, maxScore);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;
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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ValueOperations<String, String> v = st.opsForValue();

			boolean r = v.getOperations().unlink(key);
			if (r == true)
			{
				return 1L;
			} else
			{
				return 0L;
			}

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;
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
		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{

			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ZSetOperations<String, String> v = st.opsForZSet();
			return v.removeRange(key, (long) minScore, (long) maxScore);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;
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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{

			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ZSetOperations<String, String> v = st.opsForZSet();
			return v.remove(key, value);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;
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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{

			RedisTemplate<String, String> st = springConnection.redisTemplate();
			SetOperations<String, String> v = st.opsForSet();
			return v.remove(key, value);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;
		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public void setUnsortedSet(String key, List<String> values)
	{
		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.strRedisTemplate();

			SetOperations<String, String> v = st.opsForSet();

			v.add(key, values.toArray(new String[0]));
			st.expire(key, Duration.ofSeconds(ConfigurationManager.getRedisDataExpireSec()));

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long globalIncrement()
	{

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}

		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			ValueOperations<String, String> values = st.opsForValue();

			return values.increment("global_id_number");

			/**
			 *
			 *
			 * BitFieldSubCommands b =
			 * BitFieldSubCommands.create().incr(BitFieldType.unsigned(32))
			 * .valueAt(BitFieldSubCommands.Offset.offset(0L))
			 * .overflow(BitFieldSubCommands.BitFieldIncrBy.Overflow.WRAP).by(1L);
			 *
			 * List<Long> responses = values.bitField("globalIncrement", b); return
			 * responses.get(0);
			 */

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long setIfNotExists(String key, String value)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setIfNotExists");
			return -1;
		}

		CustomLogger.debug("Setting a value for key setIfNotExists: " + key);

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			ValueOperations<String, String> values = st.opsForValue();
			st.expire(key, Duration.ofSeconds(ConfigurationManager.getRedisDataExpireSec()));

			return values.setIfAbsent(key, value) ? 1 : 0;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		}
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

		CustomLogger.debug("Setting a value for key setIfNotExists: " + key);

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			ValueOperations<String, String> values = st.opsForValue();
			st.expire(key, Duration.ofSeconds(ttl));
			return values.setIfAbsent(key, value) ? 1 : 0;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			return st.expire(key, timeout, TimeUnit.SECONDS) ? 1 : 0;
		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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

		CustomLogger.debug("Deleting key: " + key);

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			return st.delete(key) ? 1 : 0;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			HashOperations<String, String, String> values = st.opsForHash();
			return values.putIfAbsent(hashSetName, fieldName, value) ? 1 : 0;

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

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

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			StringRedisTemplate st = springConnection.strRedisTemplate();
			HashOperations<String, String, String> values = st.opsForHash();
			return values.delete(hashSetName, fieldName);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public long deleteFromMultivalue(String key, String keytoremove)
	{

		if (key == null)
		{
			CustomLogger.error("Received a null key in setArray");
			return -1;
		}

		CustomLogger.debug("Setting a value for keys deleteFromMultivalue: " + key);
		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.redisTemplate();
			ListOperations<String, String> v = st.opsForList();

			return v.remove(key, 0, keytoremove);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		}
	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public Set<String> getUnsortedSet(String key)
	{
		if (key == null)
		{
			CustomLogger.error("Received a null key in getUnsortedSet");
			return null;
		}

		if (springConnection == null)
		{
			springConnection = conn.getSpringConnection();
		}
		try
		{
			RedisTemplate<String, String> st = springConnection.strRedisTemplate();

			SetOperations<String, String> v = st.opsForSet();

			return v.members(key);

		} catch (Exception e)
		{
			CustomLogger.error(CAUGHTANEXCEPTION + e.getMessage(), e);
			conn.resetConnection();
			throw e;

		}

	}

	@Override
	@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)

	public Map<String, Map<String, String>> getMultimap(Set<String> key)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Long> setHashSetValueIfNotExists(Set<String> hashSetName, String fieldName, String value)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Long> deleteHashSetValue(Set<String> hashSetNames, String fieldName)
	{
		// TODO Auto-generated method stub
		return null;
	}
}
