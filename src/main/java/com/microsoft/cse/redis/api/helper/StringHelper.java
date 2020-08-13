package com.microsoft.cse.redis.api.helper;

import java.security.SecureRandom;

public class StringHelper
{
	private static SecureRandom rnd = new SecureRandom();

	private StringHelper()
	{
	}

	public static String getSaltString()
	{
		String stringSalt = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		StringBuilder salt = new StringBuilder();
		while (salt.length() < 18)
		{ // length of the random string.
			int index = (int) (rnd.nextFloat() * stringSalt.length());
			salt.append(stringSalt.charAt(index));
		}
		return salt.toString();

	}
}
