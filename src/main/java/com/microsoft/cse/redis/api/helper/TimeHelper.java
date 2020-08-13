package com.microsoft.cse.redis.api.helper;

import java.time.Instant;

public class TimeHelper
{
	private TimeHelper()
	{
	}

	public static boolean expired(double timer)
	{
		long t = (long) timer;

		long now = Instant.now().toEpochMilli();
		if (now < t)
		{
			return false;
		}
		return true;

	}

}
