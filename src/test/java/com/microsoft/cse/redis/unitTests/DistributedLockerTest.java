package com.microsoft.cse.redis.unitTests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.cse.redis.api.exceptions.LockerException;
import com.microsoft.cse.redis.api.helper.ConfigurationManager;
import com.microsoft.cse.redis.api.helper.StringHelper;
import com.microsoft.cse.redis.api.transactions.DistributedLocker;

/**
 * Tests for distributed locker mechanism.
 */
public class DistributedLockerTest
{
	@BeforeAll
	static void setup()
	{
		TestBase.setup();
	}

	@AfterAll
	static void tearDown()
	{
		TestBase.teardown();
	}

	/**
	 * Tries to acquire the lock of a new, freed lock
	 */
	@Test
	public void getFreeLock()
	{
		String ownerName = StringHelper.getSaltString();
		String lockName = StringHelper.getSaltString();

		DistributedLocker locker = new DistributedLocker(lockName, ownerName);
		boolean acquired = false;
		try
		{
			acquired = locker.lock(false, 500);
		} catch (LockerException e)
		{
			fail("Got exception while trying to lock.", e);
		}

		assertEquals(true, acquired, "Could not acquired the lock.");
	}

	/**
	 * Tries to acquire a in-use lock.
	 */
	@Test
	public void getUsedLock()
	{
		String primaryOwnerName = StringHelper.getSaltString();
		String secondaryOwnerName = StringHelper.getSaltString();
		String lockName = "mylock";//StringHelper.getSaltString();

		// Primary owner acquires the lock
		DistributedLocker primaryOwnerLocker = new DistributedLocker(lockName, primaryOwnerName);
		boolean primaryAcquired = false;
		try
		{
			primaryAcquired = primaryOwnerLocker.lock(false, 500);
		} catch (LockerException e)
		{
			fail("Got exception while trying to lock.", e);
		}
		assertEquals(true, primaryAcquired, "Primary owner could not acquired the lock.");

		// Secondary owner acquires the lock, it must wait for the default TTL
		DistributedLocker secondaryOwnerLocker = new DistributedLocker(lockName, secondaryOwnerName);
		boolean secondaryAcquired = false;
		long secondaryLockStartTime = Instant.now().toEpochMilli();
		try
		{
			secondaryAcquired = secondaryOwnerLocker.lock(false, (ConfigurationManager.getLockTTL() + 1) * 1000);
		} catch (LockerException e)
		{
			fail("Got exception while trying to lock.", e);
		}

		/**
		 * If the total wait time for the locker is less than 50% of the TTL time, we
		 * can safely state the secondary owner didn't wait for the lock to be releases,
		 * meaning it didn't wait for the TTL. 50% is considered because some
		 * milliseconds might have passed since the primary lock and secondary lock.
		 */
		if (secondaryAcquired && ((Instant.now().toEpochMilli()
				- secondaryLockStartTime) < ((ConfigurationManager.getLockTTL() * 1000) * 0.5)))
		{
			fail("Secondary lock was acquired too early, meaning it didn't wait the primary owner to realese the locker.");
		}
		assertEquals(true, secondaryAcquired, "Secondary owner could not acquired the lock.");
	}

	/**
	 * Tries to acquire a in-use lock, with fixed lock enabled.
	 */
	@Test
	public void getUsedLockWithFixedLock()
	{
		String primaryOwnerName = StringHelper.getSaltString();
		String secondaryOwnerName = StringHelper.getSaltString();
		String lockName = StringHelper.getSaltString();

		/**
		 * Primary owner acquires a fixed lock, so it will never release it. Secondary
		 * owner should not be able to acquire it.
		 */
		DistributedLocker primaryOwnerLocker = new DistributedLocker(lockName, primaryOwnerName);
		boolean primaryAcquired = false;
		try
		{
			primaryAcquired = primaryOwnerLocker.lock(true, 500);
		} catch (LockerException e)
		{
			fail("Got exception while trying to lock.", e);
		}
		assertEquals(true, primaryAcquired, "Primary owner could not acquired the lock.");

		/**
		 * Secondary owner acquires the lock, it must wait for the default TTL, but it
		 * should net be able to get it, since the primary owner is using a fixed lock.
		 */
		DistributedLocker secondaryOwnerLocker = new DistributedLocker(lockName, secondaryOwnerName);
		boolean secondaryAcquired = false;
		try
		{
			secondaryAcquired = secondaryOwnerLocker.lock(false, ConfigurationManager.getLockTTL() * 1000);
		} catch (LockerException e)
		{
			fail("Got exception while trying to lock.", e);
		}

		assertEquals(false, secondaryAcquired,
				"Secondary owner acquired the lock. It should not be able to do that, since the primary owner requested a fixed locker.");
	}

	/**
	 * Tries to acquire a manually released lock.
	 */
	@Test
	public void getReleasedLock()
	{
		String primaryOwnerName = StringHelper.getSaltString();
		String secondaryOwnerName = StringHelper.getSaltString();
		String lockName = StringHelper.getSaltString();

		// Primary owner acquires a fixed lock.
		DistributedLocker primaryOwnerLocker = new DistributedLocker(lockName, primaryOwnerName);
		boolean primaryAcquired = false;
		try
		{
			primaryAcquired = primaryOwnerLocker.lock(true, 500);
		} catch (LockerException e)
		{
			fail("Got exception while trying to lock.", e);
		}
		assertEquals(true, primaryAcquired, "Primary owner could not acquired the lock.");

		// Primary owner releases the locker
		primaryOwnerLocker.unlock();

		// Secondary lockers should be able to acquire the lock, since it was released
		DistributedLocker secondaryOwnerLocker = new DistributedLocker(lockName, secondaryOwnerName);
		boolean secondaryAcquired = false;
		try
		{
			secondaryAcquired = secondaryOwnerLocker.lock(false, 500);
		} catch (LockerException e)
		{
			fail("Got exception while trying to lock.", e);
		}

		assertEquals(true, secondaryAcquired, "Secondary owner could not acquired the lock.");
	}

}