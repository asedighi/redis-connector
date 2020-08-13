package com.microsoft.cse.redis.unitTests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.cse.redis.api.helper.StringHelper;
import com.microsoft.cse.redis.api.redis.data.DataFactory;

/**
 * This test class tests the DataFactory for the API, and primarily focuses on
 * testing the setting, getting, and deleting of values from Redis through the
 * API.
 */
class DataFactoryTest
{

	public static DataFactory d;

	/**
	 * The setup method uses the test base to create/start the connection to Redis
	 * for these integrated tests.
	 */
	@BeforeAll
	static void setup()
	{
		TestBase.setup();

		d = DataFactory.getDataFactory(TestBase.getConnectionFactory());
	}

	/**
	 * Tests that an unset key/value pair returns null.
	 */
	@Test
	void shouldGetNullValue()
	{

		// get random string
		String k = StringHelper.getSaltString();

		// test retrieving an unset value with key, using DataFactory's "get"
		String v = d.get(k);

		// nothing was set in Redis, so it should return null as the value
		assertNull(v, "Retrieved value from unset key.");

	}

	/**
	 * Tests the basic set/get for Redis.
	 * 
	 * @throws Exception
	 */
	@Test
	void shouldSetAndGet() throws Exception
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// get random string (for value)
		String v = StringHelper.getSaltString();

		// test setting key to value in Redis database, using DataFactory's "set"
		d.set(k, v);

		// assert that the value is in the database mapped to the key
		assertEquals(v, d.get(k), "Single string set/get did not operate with expected values.");

	}

	/**
	 * Tests the setAdd/getMultivalue methods (list datatype).
	 */
	@Test
	void shouldAddAndGetSet()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// get random strings (for values)
		String v = StringHelper.getSaltString();
		String v2 = StringHelper.getSaltString();

		// test the simple Redis set operation to add the value to the top of a set
		d.setAdd(k, v);
		d.setAdd(k, v2);

		// assert that the values were added to the set by getting the list of values
		// stored at key
		// v2 should be at the top/head because it was added most recently
		assertEquals(v, d.getMultivalue(k).get(1), "List set/get did not operate correctly.");
		assertEquals(v2, d.getMultivalue(k).get(0), "List set/get did not operate correctly.");

	}

	/**
	 * Tests the setUnsortedSet/getUnsortedSet methods (unordered set datatype).
	 */
	@Test
	void shouldSetUnsortedSet()
	{

		// create lists of keys/values to test with
		String key = "hello";

		String[] arr2 = { "1", "2", "3" };
		List<String> values = Arrays.asList(arr2);

		// testing setting an unsorted set
		d.setUnsortedSet(key, values);

		// testing getting an unsorted set
		Set<String> res = d.getUnsortedSet(key);

		// assert the keys/values were set properly
		assertEquals(new HashSet<>(values), res, "Unsorted set was not set or retrieved correctly.");

	}

	/**
	 * Tests the setMultimap/getMultimap methods (hashmap datatype).
	 */
	@Test
	void shouldSetAndGetMultimap()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// create map to set as value
		Map<String, String> v = new HashMap<String, String>();

		// input random values into map
		for (int i = 0; i < 5; i++)
		{
			String key = StringHelper.getSaltString();
			String val = StringHelper.getSaltString();

			v.put(key, val);
		}

		// test setting key to value map in Redis database, using DataFactory's
		// "setMultimap"
		d.setMultimap(k, v);

		// assert that the the multimap was set and gotten properly
		assertEquals(v, d.getMultimap(k), "Hashmap set/get did not operate correctly.");

	}

	/**
	 * Tests the setZSet/getZSet methods (ordered set datatype).
	 */
	@Test
	void shouldSetAndGetZSet()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		Set<String> v = new HashSet<>();

		// test Redis SortedSet operation that adds the value to the appropriate
		// location in the set based on score
		for (int i = 0; i < 3; i++)
		{

			// setZSet(String key, String value, Double score)
			d.setZSet(k, Integer.toString(i), (double) i);
			v.add(Integer.toString(i));
		}

		// use DataFactory's "getZSet" to retrieve the set of items in the sorted set at
		// key with a score between 0 and MAX_VALUE
		Set<String> res = d.getZSet(k, Integer.toUnsignedLong(0), Integer.toUnsignedLong(Integer.MAX_VALUE));

		// assert that the sets are the same size and contain the same things
		assertEquals(v, res, "Ordered set set/get did not operate correctly.");

	}

	/**
	 * Tests the removal of keys from a sorted set based on a range of scores.
	 */
	@Test
	void shouldRemoveZSetByScore()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// add values to sorted set
		for (int i = 0; i < 5; i++)
		{

			// setZSet(String key, String value, Double score)
			d.setZSet(k, Integer.toString(i), (double) i);

		}

		// create the expected set to test against
		String[] arr = { "3", "4" };
		Set<String> v = new HashSet<String>(Arrays.asList(arr));

		// use DataFactory's "getZSet" to retrieve the all the items in the sorted set
		Set<String> res = d.getZSet(k, Integer.toUnsignedLong(0), Integer.toUnsignedLong(Integer.MAX_VALUE));

		// test DataFactory's "removeZSet" to remove by score the items with scores [0,
		// 2]
		d.removeZSet(k, 0, 2);

		// the only remaining values should be 3 and 4, so assert that
		res = d.getZSet(k, Integer.toUnsignedLong(0), Integer.toUnsignedLong(Integer.MAX_VALUE));
		assertEquals(v, res, "Ordered set values were not removed correctly.");

	}

	/**
	 * Tests the removal from a sorted/ordered set.
	 */
	@Test
	void shouldRemoveZSetByKey()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// add values to sorted set [0-4]
		for (int i = 0; i < 5; i++)
		{

			// setZSet(String key, String value, Double score)
			d.setZSet(k, Integer.toString(i), (double) i);

		}

		// test DataFactory's "removeZSet" to remove the member "2" at key k
		d.removeZSet(k, "2");

		// create the set to test against
		String[] arr = { "0", "1", "3", "4" };
		Set<String> v = new HashSet<String>(Arrays.asList(arr));

		// use DataFactory's "getZSet" to retrieve the all the items in the sorted set
		Set<String> res = d.getZSet(k, Integer.toUnsignedLong(0), Integer.toUnsignedLong(Integer.MAX_VALUE));

		// assert that the key/value pair was removed
		assertEquals(v, res, "Ordered set values were not removed correctly.");

	}

	/**
	 * Tests the removal of a specified key/value pair from Redis.
	 */
	@Test
	void shouldRemove()
	{

		// get random strings
		String k = StringHelper.getSaltString();

		String v = StringHelper.getSaltString();

		// set
		d.set(k, v);

		// assert that it is in Redis
		assertEquals(v, d.get(k), "Key/value pair was not set correctly.");

		// then remove it
		d.remove(k);

		// assert that the mapping is removed
		assertNull(d.get(k), "Regular key/value pair not removed correctly.");

	}

	/**
	 * Tests the removal of the specified value from the unordered set.
	 */
	@Test
	void shouldRemoveSetValue()
	{

		// create lists of keys/values to test with
		String key = "hello";

		String[] arr = { "1", "2", "3" };
		List<String> values = Arrays.asList(arr);

		// set
		d.setUnsortedSet(key, values);

		// assert the keys/values were set properly
		assertEquals(new HashSet<>(values), d.getUnsortedSet(key), "Unsorted set was not set or retrieved correctly.");

		// test removing a value from the unsorted set
		d.removeSetValue(key, "b");

		// retrieve the set
		Set<String> res = d.getUnsortedSet(key);

		// make the original set match to test against
		values.remove("b");

		// assert the keys/values were set properly
		assertEquals(new HashSet<>(values), res, "Unsorted set value was not removed correctly.");
	}

	/**
	 * Tests that a value should be set/created through "shouldSetIfNotExists"
	 * because it did not exist previously.
	 */
	@Test
	void shouldSetIfNotExists()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// get random string (for value)
		String v = StringHelper.getSaltString();

		// test setting the value if it didn't exist, using DataFactory's
		// "setIfNotExists"
		long result = d.setIfNotExists(k, v);

		// assert that it should have returned 1, because it did not exist previously
		assertEquals(1, result, "Key/value pair was not created.");

		// assert that the values were set properly
		assertEquals(v, d.get(k), "Value was not set correctly despite not existing previously.");

	}

	/**
	 * Tests that a value will not be overwritten/created through
	 * "shouldSetIfNotExists" because it already existed.
	 */
	@Test
	void shouldNotSetAlreadyExisting()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// get random string (for value)
		String v = StringHelper.getSaltString();

		// set the key/value pair
		d.set(k, v);

		String v2 = StringHelper.getSaltString();

		// test setting the value if it didn't exist, using DataFactory's
		// "setIfNotExists"
		long result = d.setIfNotExists(k, v2);

		// assert that it should have returned 0, it was not set, meaning the item
		// already exists
		assertEquals(0, result, "Key/value pair created even though it already existed.");

		// assert that the value is still set to the original value
		assertEquals(v, d.get(k), "Value was set despite already existing.");

	}

	/**
	 * Tests that a value should be set/created through "shouldSetIfNotExists"
	 * because it did not exist previously (but with TTL). Also tests that it
	 * expires.
	 */
	@Test
	void shouldSetIfNotExistsWithTTL()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// get random string (for value)
		String v = StringHelper.getSaltString();

		// test setting the value if it didn't exist (with TTL), using DataFactory's
		// "setIfNotExists"
		long result = d.setIfNotExists(k, v, 1);

		// assert that it should have returned 1, because it did not exist previously
		assertEquals(1, result);

		// assert that the values were set properly
		assertEquals(v, d.get(k), "Key/value pair not set correctly.");

		// wait for the key/value to expire
		try
		{
			Thread.sleep(1500);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
			System.out.println("Caught exception: " + e.getMessage());
		}

		// assert that it has been expired from Redis
		assertNull(d.get(k), "Key did not expire after having expiration set.");
	}

	/**
	 * Tests that setExpiration sets a timeout on key. After the timeout has
	 * expired, the key will automatically be deleted.
	 */
	@Test
	void shouldSetExpiration()
	{

		// get random string (for key)
		String k = StringHelper.getSaltString();

		// get random string (for value)
		String v = StringHelper.getSaltString();

		// set key/value pair
		d.set(k, v);

		// test setting the expiration/TTL for a specific key
		d.setExpiration(k, 1);

		try
		{
			Thread.sleep(1500);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
			System.out.println("Caught exception: " + e.getMessage());
		}

		// assert that the key/value pair has expired
		assertNull(d.get(k), "Key/value pair did not expire properly.");
	}

	/**
	 * Tests that a key/value pair is able to be deleted via key.
	 */
	@Test
	void shouldDelete()
	{

		// get random strings
		String k = StringHelper.getSaltString();

		String v = StringHelper.getSaltString();

		// set key/value pair
		d.set(k, v);

		// assert that it is in Redis
		assertEquals(v, d.get(k), "Regular key/value pair not set/get correctly.");

		// then remove it
		d.delete(k);

		// assert that the key/value pair is gone/null
		assertNull(d.get(k), "Regular key/value pair not deleted correctly.");

	}

	/**
	 * Tests that with this method it creates a new field in the hash stored at key
	 * to hashvalue, because the field does not yet exist.
	 */
	@Test
	void shouldSetHashValueIfNotExists()
	{

		// get random string (for key of hashmap)
		String k = StringHelper.getSaltString();

		// get random string (for value to set)
		String hashvalue = StringHelper.getSaltString();

		// create map
		Map<String, String> v = new HashMap<String, String>();

		// input random values into map
		for (int i = 0; i < 5; i++)
		{
			String field = StringHelper.getSaltString();
			String val = StringHelper.getSaltString();
			v.put(field, val);
		}

		// set the hashmap in advance
		d.setMultimap(k, v);

		// get random string (for field)
		String fieldName = StringHelper.getSaltString();

		// testing setting the hash value of a nonexistent field
		long result = d.setHashSetValueIfNotExists(k, fieldName, hashvalue);

		// assert that the value was set/created successfully
		assertEquals(1, result, "Value was not set/created in field that did not previously exist.");

		// assert that it has created a field for the new value
		assertEquals(hashvalue, d.getMultimap(k).get(fieldName), "Value was not created with the correct value.");

	}

	/**
	 * Tests that with this method it does not set a hash set value if it already
	 * exists.
	 */
	@Test
	void shouldNotSetHashValueIfNotExists()
	{

		// get random string (for key of hashmap)
		String k = StringHelper.getSaltString();

		// get random string (for value to set)
		String hashvalue = StringHelper.getSaltString();

		// create map
		Map<String, String> v = new HashMap<String, String>();

		// create list to keep track of keys
		List<String> fieldList = new ArrayList<>();

		// input random values into map
		for (int i = 0; i < 5; i++)
		{
			String field = StringHelper.getSaltString();
			String val = StringHelper.getSaltString();
			// keep track of the keys/field names
			fieldList.add(field);

			v.put(field, val);
		}

		// set the hash in advance
		d.setMultimap(k, v);

		// test setting the hash value with already existing field
		long result = d.setHashSetValueIfNotExists(k, fieldList.get(0), hashvalue);

		// assert that the value was not set
		assertEquals(0, result, "Value was set in field that already existed.");

		// assert that it remains true to the original set value
		assertEquals(v.get(fieldList.get(0)), d.getMultimap(k).get(fieldList.get(0)),
				"Value was overwritten although its field already existed.");

	}

	/**
	 * Tests that this method removes the specified fields from the hash stored at
	 * key. Specified fields that do not exist within this hash are ignored. If key
	 * does not exist, it is treated as an empty hash and this command returns 0.
	 */
	@Test
	void shouldDeleteHashValue()
	{

		// get random string (for key of hashmap)
		String k = StringHelper.getSaltString();

		// create map to set as value for setMultimap
		Map<String, String> v = new HashMap<String, String>();

		// create list to keep track of keys
		List<String> fieldList = new ArrayList<>();

		// input random values into map
		for (int i = 0; i < 5; i++)
		{
			String field = StringHelper.getSaltString();
			String val = StringHelper.getSaltString();
			// keep track of the keys/field names
			fieldList.add(field);

			v.put(field, val);
		}

		// set hashmap
		d.setMultimap(k, v);

		// assert that the the multimap was set and retrieved properly
		assertEquals(v, d.getMultimap(k), "Hashmap set/get did not operate correctly.");

		// test deleting the first field from the hash map
		long result = d.deleteHashSetValue(k, fieldList.get(0));

		// assert the success of the operation
		assertEquals(1, result, "Delete hash value operation not successful.");

		// assert that the value is gone
		assertNull(d.getMultimap(k).get(fieldList.get(0)), "Delete hash value operation not successful.");

	}

	/**
	 * Tests the removal of elements with deleteFromMultivalue (list datatype).
	 * "lrem" Redis operation removes the first count occurrences of elements equal
	 * to element from the list stored at key.
	 */
	@Test
	void shouldDeleteFromMultivalue()
	{

		// make array to keep track of values added to list in Redis
		String[] arr = new String[3];

		// get random string (for key)
		String k = StringHelper.getSaltString();

		for (int i = 0; i < 3; i++)
		{

			// get random strings (for values)
			String v = StringHelper.getSaltString();
			arr[i] = v;
			// add value to list
			d.setAdd(k, v);
		}

		// test deleting from multivalue (list)
		d.deleteFromMultivalue(k, arr[0]);

		// retrieve the list
		List<String> res = d.getMultivalue(k);

		// assert that the first value inserted was deleted from the list in Redis
		assertTrue(!res.contains(arr[0]));
	}

	/*** Exception/Edge-case Testing ***/

	/**
	 * Tests the regular "get" function when passing in a null key.
	 */
	@Test
	void shouldReturnNullWithNullKeyUsingGet()
	{

		// create a null key
		String k = null;

		// assert that it returns null
		assertNull(d.get(k), "Null key returned something other than null.");
	}

	/**
	 * Tests getting a multimap with a null key.
	 */
	@Test
	void shouldReturnNullWithNullKeyUsingGetMultimap()
	{

		// create a null key
		String k = null;

		// assert that it returns null
		assertNull(d.getMultimap(k), "Null key returned something other than null.");
	}

	/**
	 * Tests getting a multivalue with a null key.
	 */
	@Test
	void shouldReturnNullWithNullKeyUsingGetMultivalue()
	{

		// create a null key
		String k = null;

		// assert that it returns null
		assertNull(d.getMultivalue(k), "Null key returned something other than null.");
	}

	/**
	 * Tests getting a sorted set (ZSet) with a null key.
	 */
	@Test
	void shouldReturnNullWithNullKeyUsingGetZSet()
	{

		// create a null key and long values for scores
		String k = null;
		Long x = Integer.toUnsignedLong(0);
		Long y = Integer.toUnsignedLong(100);

		// assert that it returns null
		assertNull(d.getZSet(k, x, y), "Null key returned something other than null.");
	}

	/**
	 * Shuts down the connection to Redis.
	 */
	@AfterAll
	static void tearDown()
	{
		TestBase.teardown();
	}
}
