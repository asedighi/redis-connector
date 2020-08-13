package com.microsoft.cse.redis.api.exceptions;

public final class LockerException extends Exception
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public LockerException(String message)
	{
		super(message);
	}
}