package com.microsoft.cse.redis.api.transactions;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.microsoft.cse.redis.api.exceptions.LockerException;
import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.helper.CustomLogger;
import com.microsoft.cse.redis.api.redis.connection.ConnectionFactory;
import com.microsoft.cse.redis.api.redis.data.DataFactory;

/**
 * Locker class represents a locker system in a memory cache
 */
public class DistributedLocker
{
	String lockName, ownerName;
	ConnectionFactory connectionFactory;
	DataFactory dataFactory;

	/**
	 * Initializes the locker.
	 *
	 * @param lockName  name of the lock, usually the name key of a key/value pair
	 * @param ownerName owner's name of the lock, usually the name value of a
	 *                  key/value pair
	 */
	public DistributedLocker(String lockName, String ownerName)
	{
		this.lockName = lockName;
		this.ownerName = ownerName;

		connectionFactory = ConnectionFactory.getConnection();
		dataFactory = DataFactory.getDataFactory(connectionFactory);
	}

	/**
	 * Gets the lock. While locked. The method tries to acquired the lock, if it is
	 * already locked, it will wait until it is realeased.
	 *
	 * @param fixedLock if true, lock will be renewed automatically
	 * @param timeout   time-out in milliseconds, if return false if the lock cannot
	 *                  be acquired after the timeout. If 0 is provided, it will
	 *                  wait indefinitely
	 * @return true if the lock was acquired, false if it timedout
	 */
	public boolean lock(boolean fixedLock, long timeout) throws LockerException
	{
		CustomLogger.trace("lock:start");
		long startTime = Instant.now().toEpochMilli();

		/**
		 * If I can't set the lock value (setnx == 0), then it means somebody ownes it,
		 * which means I will have to wait until it is released, except when I'm the
		 * lock owner.
		 */
		long lockResult;
		while ((lockResult = dataFactory.setIfNotExists(lockName, ownerName, ConfigurationManager.getLockTTL())) <= 0)
		{
			// If lockerResult is less than 0, then we had a problem
			if (lockResult < 0)
			{
				CustomLogger.trace("lock:error");
				throw new LockerException("Unable to acquire the lock: unkown error.");
			}

			CustomLogger.trace("lock:locked");
			// Gets the locker name. If it's me, just assume I can keep the lock
			String lockedPartition = dataFactory.get(lockName);
			if ((lockedPartition != null) && lockedPartition.equals(ownerName))
			{
				CustomLogger.trace("lock:owned_by_slf");
				return true;
			}

			/**
			 * If timeout is 0, don't watch for the timeout. Otherwise, give up trying if we
			 * reach the timeout
			 */
			if ((timeout > 0) && ((Instant.now().toEpochMilli() - startTime) >= timeout))
			{
				CustomLogger.debug("Timeout expired for lock " + lockName + ", owner " + ownerName + "...");
				return false;
			}

			// Wait for the next try
			CustomLogger.debug("Waiting lock " + lockName + " for " + ownerName + "...");
			try
			{
				CustomLogger.trace("lock:locked:wait");
				Thread.sleep(ConfigurationManager.getLockPullTime());
			} catch (InterruptedException e)
			{
				CustomLogger.warn("Lock wait was interrupted.", e);
			}
		}

		// If it is a fixed lock, then we try to renew it continuously
		if (fixedLock)
		{
			CustomLogger.trace("lock:fixedlock");
			CustomLogger.debug("Activating fixed lock " + lockName + " for " + ownerName + "...");

			// Fire and forget thread
			ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
			executor.scheduleAtFixedRate(() -> renewLock(), 0, ConfigurationManager.getLockRenewTime(),
					TimeUnit.MILLISECONDS);
		}

		CustomLogger.info(ownerName + " acquired lock on " + lockName);

		return true;
	}

	/**
	 * renewLock only sets the TTL of the lockm using a new connection from the
	 * connection pool and closing it afterwards.
	 */
	private void renewLock()
	{
		CustomLogger.trace("renewLock");
		try
		{
			dataFactory.setExpiration(lockName, ConfigurationManager.getLockTTL());
		} catch (Exception error)
		{
			CustomLogger.error("Error renewing lock: " + error.getMessage(), error);
		}
	}

	/**
	 * Releases the lock.
	 */
	public void unlock()
	{
		CustomLogger.trace("unlock:start");
		String currentOwner = dataFactory.get(lockName);

		/**
		 * If the owner name is empty or different from the current one, we simply
		 * ignore the unlock action.
		 */
		if (((currentOwner == null) || currentOwner.equals("")) || !currentOwner.equals(ownerName))
		{
			CustomLogger.trace("unlock:already_unlocked:locked_by_other");
			return;
		} else
		{
			CustomLogger.trace("unlock:locked_by_self:del");
			dataFactory.delete(lockName);
		}
	}
}