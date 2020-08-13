package com.microsoft.cse.redis.api.helper;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomLogger
{

	private static final HashMap<String, Logger> loggers = new HashMap<>();

	private CustomLogger()
	{
	}

	public static void debug(String message)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.debug(message);
	}

	public static void error(String message)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.error(message);
	}

	public static void error(String message, Exception e)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.error(message, e);
	}

	public static void error(String message, Throwable e)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.error(message, e);
	}

	public static void info(String message)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.info(message);
	}

	public static void info(String message, Exception e)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.info(message, e);
	}

	public static void trace(String message)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.trace(message);
	}

	public static void warn(String message)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.warn(message);
	}

	public static void warn(String message, Exception e)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.warn(message, e);
	}

	public static void warn(String message, Throwable e)
	{

		Logger log = CustomLogger.getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
		log.warn(message, e);
	}

	private static Logger getLogger(String clazz)
	{
		Logger log = CustomLogger.loggers.get(clazz);
		if (log == null)
		{

			log = LoggerFactory.getLogger(clazz);

			loggers.put(clazz, log);

		}
		return log;

	}

}