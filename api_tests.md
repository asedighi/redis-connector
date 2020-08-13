# API Integration Tests

These tests use JUnit 5 and can be run in your IDE or with Maven.
They require a running instance of Redis to complete.

```sh
# To run all the tests:

mvn clean test

# To run one test method:

mvn -Dtest=insertTestClassNameHere#insertTestMethodNameHere test
```

## ConnectionFactoryTest.java

setup
@BeforeAll static void setup()
Sets up the connection to Redis.

shouldGetConnectionInstance
@Test public void shouldGetConnectionInstance()
Tests that the same Connection Factory is returned through the "getConnection" method.

tearDown
@AfterAll static void tearDown()
Shutsdown the connection to Redis.

## DataFactoryTest.java

setup
@BeforeAll static void setup()
The setup method uses the test base to create/start the connection to Redis for these integrated tests.

shouldGetNullValue
@Test void shouldGetNullValue()
Tests that an unset key/value pair returns null.

shouldSetAndGet
@Test void shouldSetAndGet() throws java.lang.Exception
Tests the basic set/get for Redis.
Throws:
java.lang.Exception

shouldAddAndGetSet
@Test void shouldAddAndGetSet()
Tests the setAdd/getMultivalue methods (list datatype).

shouldSetUnsortedSet
@Test void shouldSetUnsortedSet()
Tests the setUnsortedSet/getUnsortedSet methods (unordered set datatype).

shouldSetAndGetMultimap
@Test void shouldSetAndGetMultimap()
Tests the setMultimap/getMultimap methods (hashmap datatype).

shouldSetAndGetZSet
@Test void shouldSetAndGetZSet()
Tests the setZSet/getZSet methods (ordered set datatype).

shouldRemoveZSetByScore
@Test void shouldRemoveZSetByScore()
Tests the removal of keys from a sorted set based on a range of scores.

shouldRemoveZSetByKey
@Test void shouldRemoveZSetByKey()
Tests the removal from a sorted/ordered set.

shouldRemove
@Test void shouldRemove()
Tests the removal of a specified key/value pair from Redis.

shouldRemoveSetValue
@Test void shouldRemoveSetValue()
Tests the removal of the specified value from the unordered set.

shouldSetIfNotExists
@Test void shouldSetIfNotExists()
Tests that a value should be set/created through "shouldSetIfNotExists" because it did not exist previously.

shouldNotSetAlreadyExisting
@Test void shouldNotSetAlreadyExisting()
Tests that a value will not be overwritten/created through "shouldSetIfNotExists" because it already existed.

shouldSetIfNotExistsWithTTL
@Test void shouldSetIfNotExistsWithTTL()
Tests that a value should be set/created through "shouldSetIfNotExists" because it did not exist previously (but with TTL). Also tests that it expires.

shouldSetExpiration
@Test void shouldSetExpiration()
Tests that setExpiration sets a timeout on key. After the timeout has expired, the key will automatically be deleted.

shouldDelete
@Test void shouldDelete()
Tests that a key/value pair is able to be deleted via key.

shouldSetHashValueIfNotExists
@Test void shouldSetHashValueIfNotExists()
Tests that with this method it creates a new field in the hash stored at key to hashvalue, because the field does not yet exist.

shouldNotSetHashValueIfNotExists
@Test void shouldNotSetHashValueIfNotExists()
Tests that with this method it does not set a hash set value if it already exists.

shouldDeleteHashValue
@Test void shouldDeleteHashValue()
Tests that this method removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored. If key does not exist, it is treated as an empty hash and this command returns 0.

shouldDeleteFromMultivalue
@Test void shouldDeleteFromMultivalue()
Tests the removal of elements with deleteFromMultivalue (list datatype). "lrem" Redis operation removes the first count occurrences of elements equal to element from the list stored at key.

shouldReturnNullWithNullKeyUsingGet
@Test void shouldReturnNullWithNullKeyUsingGet()
Tests the regular "get" function when passing in a null key.

shouldReturnNullWithNullKeyUsingGetMultimap
@Test void shouldReturnNullWithNullKeyUsingGetMultimap()
Tests getting a multimap with a null key.

shouldReturnNullWithNullKeyUsingGetMultivalue
@Test void shouldReturnNullWithNullKeyUsingGetMultivalue()
Tests getting a multivalue with a null key.

shouldReturnNullWithNullKeyUsingGetZSet
@Test void shouldReturnNullWithNullKeyUsingGetZSet()
Tests getting a sorted set (ZSet) with a null key.

tearDown
@AfterAll static void tearDown()
Shuts down the connection to Redis.

## DistributedLockerTest.java

setup
@BeforeAll static void setup()

tearDown
@AfterAll static void tearDown()

getFreeLock
@Test public void getFreeLock()
Tries to acquire the lock of a new, freed lock.

getUsedLock
@Test public void getUsedLock()
Tries to acquire a in-use lock.

getUsedLockWithFixedLock
@Test public void getUsedLockWithFixedLock()
Tries to acquire a in-use lock, with fixed lock enabled.

getReleasedLock
@Test public void getReleasedLock()
Tries to acquire a manually released lock.

## PipelineTest.java

setup
@BeforeAll static void setup()
The setup method uses the test base to create/start the connection to Redis for these integrated tests.

tearDown
@AfterAll static void tearDown()
Shuts down the connection to Redis.

testPipelinedOperation
@Test public void testPipelinedOperation()
Tests deletion using a pipelined operation (and commits).

testPipelinedUncommittedOperation
@Test public void testPipelinedUncommittedOperation()
Tests deletion using a pipelined operation (and does not commit).

testPipelinedTransactionData
@Test public void testPipelinedTransactionData()
Tests multiple pipelined transactions being removed.

testRegularTransactionData
@Test public void testRegularTransactionData()
Tests inserting and deleting transactions with pipelined removal.

## TransactionManagerTest.java

createTransactionValues
public void createTransactionValues()
Helper method that creates the transaction stock/generic values.

enqueueGenericTransaction
public TransactionData enqueueGenericTransaction()
Helper method used for testing the getters/setters of transactions.

setup
@BeforeAll static void setup()
Sets up the connection to Redis.

tearDown
@AfterAll static void tearDown()
Shutsdown the connection to Redis.

shouldEnqueue
@Test void shouldEnqueue()
Tests the Enqueue and Unique TID Lookup use-case of the API.

shouldLookup
@Test void shouldLookup()
Tests the Lookup use-case of the API.

shouldRead
@Test void shouldRead()
Tests the Read use-case of the API.

shouldDequeue
@Test void shouldDequeue()
Tests the Dequeue use-case of the API.

transactionsShouldExpire
@Test void transactionsShouldExpire()
Tests that transactions expire and can be flagged as such.

shouldRetrieveZSetOfTransactions
@Test void shouldRetrieveZSetOfTransactions()
Tests that a sorted set of transactions can be retrieved based on expiry time.

concurrency
@Test public void concurrency() throws java.lang.InterruptedException
Test concurrency with 200 parallel insert operations, at the end we check if they were created in Redis. Using fixed expiration time for all of them, so it is easier to look for it in the cache.
Throws:
java.lang.InterruptedException

getOwnershipNotOwned
@Test void getOwnershipNotOwned()
Tests updating the ownership of a transaction in-memory.

getOwnershipOwned
@Test void getOwnershipOwned()
Tests updating the ownership of a transaction that is already owned.

shouldSetAndGetPartition
@Test void shouldSetAndGetPartition()
Tests setting/getting a transaction's partition name.

shouldSetAndGetExpTime
@Test void shouldSetAndGetExpTime()
Tests setting/getting a transaction's expiration time.

shouldSetAndGetRoutingCode
@Test void shouldSetAndGetRoutingCode()
Tests setting/getting a transaction's routing code.

shouldSetAndGetTransactionID
@Test void shouldSetAndGetTransactionID()
Tests setting/getting a transaction's TID.

shouldSetAndGetTransactionKey
@Test void shouldSetAndGetTransactionKey()
Tests setting/getting a transaction's key.

shouldSetAndGetLargeMsg
@Test void shouldSetAndGetLargeMsg()
Tests setting/getting a transaction's large message.

shouldUpdateIDLookup
@Test void shouldUpdateIDLookup()
Tests the "andUpdate" method which updates the id:lookup HashSet with the values from the current TransactionData object.
