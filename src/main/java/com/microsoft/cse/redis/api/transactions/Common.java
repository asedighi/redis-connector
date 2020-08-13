package com.microsoft.cse.redis.api.transactions;

import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;
import com.microsoft.cse.redis.api.redis.data.DataFactory;
import com.microsoft.cse.redis.api.redis.data.PipelinedPooledRedisDataFactory;

/**
 * Common methods for data operations.
 */
public class Common
{

	private Common()
	{
	}

	/**
	 * Starts a pipeline for the current thread. Any subsequent call will be batched
	 * until endPipeline is called. Notice it only works for pipelined and pooled
	 * Jedis connection.
	 */
	public static void startPipeline()
	{
		ConnectionFactory f = ConnectionFactory.getConnection();
		DataFactory d = DataFactory.getDataFactory(f);
		if (PipelinedPooledRedisDataFactory.class.isInstance(d))
		{
			PipelinedPooledRedisDataFactory pipelinedFactory = (PipelinedPooledRedisDataFactory) d;
			pipelinedFactory.startPipeline();
		}
	}

	/**
	 * Commits a pipeline for the current thread. Notice it only works for pipelined
	 * and pooled Jedis connection.
	 */
	public static void endPipeline()
	{
		ConnectionFactory f = ConnectionFactory.getConnection();
		DataFactory d = DataFactory.getDataFactory(f);
		if (PipelinedPooledRedisDataFactory.class.isInstance(d))
		{
			PipelinedPooledRedisDataFactory pipelinedFactory = (PipelinedPooledRedisDataFactory) d;
			pipelinedFactory.endPipeline();
		}
	}
}