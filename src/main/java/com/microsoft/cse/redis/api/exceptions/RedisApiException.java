package com.microsoft.cse.redis.api.exceptions;

public final class RedisApiException extends Exception
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public RedisApiException(String message)
	{
		super(message);
	}
}