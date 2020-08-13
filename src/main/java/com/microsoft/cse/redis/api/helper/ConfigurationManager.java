package com.microsoft.cse.redis.api.helper;

import java.io.InputStream;
import java.util.Properties;
import java.util.stream.Stream;

public class ConfigurationManager
{

	private static Properties prop = new Properties();

	private ConfigurationManager()
	{
	}

	static
	{
		InputStream inputStream = null;
		try
		{
			String propFileName = "config.properties";
			inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null)
			{
				prop.load(inputStream);
			} else
			{
				prop = null;
			}
			if (inputStream != null)
			{
				inputStream.close();
			}

		} catch (Exception e)
		{
			CustomLogger.error("Exception: " + e);
		}
	}

	public static String getProperty(String name)
	{
		String val = System.getenv(name);
		if ((val == null) && prop.containsKey(name))
		{
			val = prop.getProperty(name);
		}

		return val;

	}

	public static boolean getServerSideSSLEnabled()
	{

		String res = getProperty("SERVER_SIDE_SSL");
		if ((res != null) && res.equalsIgnoreCase("true"))
		{
			return true;
		}
		return false;

	}

	public static Integer getIntProperty(String name)
	{
		String val = getProperty(name);
		if ((val == null) || (val.equals("")))
		{

			CustomLogger.error("Property " + name + " does not have a value set in env or config file.  Exiting...");
			System.exit(-1);
		}
		return Integer.parseInt(val);

	}

	public static String[] getHostname()
	{
		String hosts = getProperty("HOST_NAME");
		if (hosts == null)
		{
			String[] h = { "localhost" };
			return h;
		}

		return hosts.split("\\s*,\\s*");
	}

	public static int[] getPortnumber()
	{
		String port = getProperty("HOST_PORT");
		if (port == null)
		{
			int[] p = { 6379 };
			return p;
		}

		String[] res = port.split("\\s*,\\s*");
		return Stream.of(res).mapToInt(Integer::parseInt).toArray();
	}

	public static String getPassword()
	{
		String password = getProperty("REDIS_PASSWORD");
		if ((password == null) || (password.isEmpty()))
		{
			return null;
		} else
		{
			return password;
		}
	}

	public static int getConnectionTimeoutMSec()
	{
		return getIntProperty("REDIS_CONNECTION_TIMEOUT");

	}

	public static int getConnectionCheckSec()
	{
		return getIntProperty("REDIS_CONNECTION_CHECK");

	}

	public static int getRedisDataExpireSec()
	{
		return getIntProperty("REDIS_DATA_EXPIRE");

	}

	public static int getLockTTL()
	{
		return getIntProperty("LOCK_TTL");
	}

	public static int getLockPullTime()
	{
		return getIntProperty("LOCK_PULL_TIME");
	}

	public static int getLockRenewTime()
	{
		return getIntProperty("LOCK_RENEW_TIME");
	}

	public static String getKeyStoreFileLocation()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public static String getSecurityProtocol()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public static String getSecurityProvider()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public static String getKeyStorePassword()
	{
		// TODO Auto-generated method stub
		return null;
	}

}