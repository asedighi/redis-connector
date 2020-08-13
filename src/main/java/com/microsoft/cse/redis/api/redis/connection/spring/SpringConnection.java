package com.microsoft.cse.redis.api.redis.connection.spring;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.microsoft.cse.redis.api.exceptions.RedisApiException;
import com.microsoft.cse.redis.api.helper.ClusterEndpoints;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.helper.ClusterEndpoints.RedisCluster;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class SpringConnection extends com.microsoft.cse.redis.api.redis.connection.ConnectionInterface
{
	private JedisConnectionFactory connectionFactory;

	private StringRedisTemplate stringTemplate;
	private RedisTemplate<String, Map<String, String>> transactionTemplate;
	private RedisTemplate<String, String> setTemplate;
	private final JedisPoolConfig pool;
	private final RedisStandaloneConfiguration config;

	// Clustered version requires license. We need to revisit once we start working
	// in a real environment.
	// private final RedisClusterConfiguration config;

	public SpringConnection(ClusterEndpoints clusters, int numberOfConnection, String pass, int timeout)
	{
		super(clusters, pass, timeout);
		pool = new JedisPoolConfig();
		pool.setMaxTotal(numberOfConnection);

//      pool.setTestWhileIdle(false);
//      pool.setTestOnBorrow(false);
//      pool.setTestOnReturn(false);
//      pool.setMinEvictableIdleTimeMillis(60000);
//      pool.setTimeBetweenEvictionRunsMillis(30000);
//      pool.setNumTestsPerEvictionRun(-1);

		/**
		 * See above. Clustered version does not work as it requires license. config =
		 * new RedisClusterConfiguration(); config.clusterNode(host, port);
		 */

		/**
		 * this is the standalone configuration
		 */

		RedisCluster rc = clusters.next();

		config = new RedisStandaloneConfiguration(rc.getHost(), rc.getPort());

		if (pass != null)
		{
			RedisPassword p = RedisPassword.of(pass);

			config.setPassword(p);
		}

	}

	private JedisConnectionFactory connectionFactory()
	{

		/**
		 * this is the sentinel way of connecting to Redis.
		 *
		 * RedisSentinelConfiguration sentinelConfig = new
		 * RedisSentinelConfiguration().master("master1") .sentinel(hostname, portn);
		 *
		 * connectionFactory = new JedisConnectionFactory(sentinelConfig);
		 */

		/**
		 * this is the clustered way of connecting to redis connectionFactory = new
		 * JedisConnectionFactory(config, pool);
		 */

		/**
		 * this is the standalone way of connecting to redis
		 */
		connectionFactory = new JedisConnectionFactory(config);

		connectionFactory.setTimeout(timeout);
		// connectionFactory.setHostName(hostname);
		// connectionFactory.setPort(portn);
		// connectionFactory.setUsePool(true);
		connectionFactory.afterPropertiesSet();
		return connectionFactory;

	}

	@Override
	public Jedis getConnection() throws RedisApiException
	{
		throw new RedisApiException("Not implemented.  Please use the spring framework connection factory");
	}

	@Bean
	@Autowired
	public RedisTemplate<String, Map<String, String>> transactionRedisTemplate()
	{

		if (transactionTemplate == null)
		{
			transactionTemplate = new RedisTemplate<String, Map<String, String>>();
			transactionTemplate.setKeySerializer(new StringRedisSerializer());

			transactionTemplate.setConnectionFactory(connectionFactory());
			transactionTemplate.afterPropertiesSet();

		}

		return transactionTemplate;

	}

	@Bean
	@Autowired
	public StringRedisTemplate strRedisTemplate()
	{
		CustomLogger.debug("Creating a Spring String template");

		if (stringTemplate == null)
		{
			stringTemplate = new StringRedisTemplate();
			stringTemplate.setKeySerializer(new StringRedisSerializer());

			RedisSerializer<String> stringSerializer = new StringRedisSerializer();

			JdkSerializationRedisSerializer jdkSerializationRedisSerializer = new JdkSerializationRedisSerializer();

			stringTemplate.setConnectionFactory(connectionFactory());

			stringTemplate.setKeySerializer(stringSerializer);

			stringTemplate.setHashKeySerializer(stringSerializer);

			stringTemplate.setValueSerializer(jdkSerializationRedisSerializer);

			stringTemplate.setHashValueSerializer(jdkSerializationRedisSerializer);

			stringTemplate.setEnableTransactionSupport(true);

			stringTemplate.afterPropertiesSet();

		}

		return stringTemplate;
	}

	@Bean
	@Autowired
	public RedisTemplate<String, String> redisTemplate()
	{
		CustomLogger.debug("Spring creating a <string, string> template");

		if (setTemplate == null)
		{
			setTemplate = new RedisTemplate<String, String>();

			setTemplate.setConnectionFactory(connectionFactory());
			setTemplate.afterPropertiesSet();
		}

		return setTemplate;
	}

	@Override
	public void shutdown()
	{

		connectionFactory.destroy();
	}

}
