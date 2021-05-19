![Java CI with Maven](https://github.com/CSE-FIS/api/workflows/Java%20CI%20with%20Maven/badge.svg)
# Redis API Abstraction Layer
Interfacing with Redis using Spring, Jedis and Lettuce 

## Developer Settings
The POM is fully configured to create a jar file.  You must do a complete build as the aspect insertions require a full rebuild after every change.  

```
mvn clean package install
```
The package command will also run the unit tests.  


## Creating a runnable Docker Container Image from this repository

* Start Redis in a standalone Docker Container

```
$ docker run --name some-redis -d redis
```

* Use the [provided Multi-Stage Build Dockerfile](Dockerfile) to build a Docker Image for the api

* You can then run this command from the root of your project:

```
/api$ docker build -t "api:latest" .
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NOTES: 

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- The "-t" option allows you to provide the container name and/or tag name of your choice for the container.

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- For more on Docker Multi-Stage Builds, please see <a href="https://docs.docker.com/develop/develop-images/multistage-build/" > here. </a>

* You can then run the api via 

```
$ docker run api
(Or however you named your container)
```


## How the api works
The config.properties has all the required parameters needed to configure the API.  You need to explicitly call the configuration parameter class to read the configs:

```
				connectionFactory = ConnectionFactory.getPipelinedJedisPooledInstance(ConfigurationManager.getHostname(),
				ConfigurationManager.getPortnumber(), ConfigurationManager.getPassword(), 30, 1,
				ConfigurationManager.getConnectionTimeoutMSec());
```

This line creates a Jedis pool.  The number of connection in the pool is 30, and the number of pools is 1.  The rest of the values are taken from config.properties file.  


## API Structure
Figure below shows the various layers of the API.  

![API](https://github.com/asedighi/redis-connector/blob/master/api.jpg)

The API follows a layered approach.  The "lowest" layer is the connection layer which manages connections via Jedis/Spring/JedisPool to the backend Redis endpoints.  JedisPool connection type also supports pipelining as a subtype. Pipelining performs betters in batch-type requests, but it is can default back to the Jedispool connection type if no pipelining is used.  The Jedis version the code is similar to JedisPool, but the pool is implemented by the API. It should perform better, but it is less resilient.  In the Jedis version, you are essentially creating a many pools of single connections to Redis.  These custom pools have less overhead, but a failure will cause a new pool to be created.

The Spring version of the connection type uses the Spring framework.  This method of communication if 75% code complete, and requires further testing.   


The next layer is the data layer that abstracts data communication (set/get/delete) via the chosen connection type to Redis.  Each communication type uses a specific version of the data layer, but the type is selected at runtime.  

The resiliency and recovery is covered in the next section.  

As the API moves to the meta-data layer, it ties business data types to the lower Redis types.  Each business object, such as Transactions, is facade around a number of connection-specific data types.  The business meta-data layer abstracts communication and management of data as it flows through the system.  

## Recovery and Resiliency

The API uses an Aspect-based API called jcabi to provide recovery and resiliency.  The following annotation covers how recovery is accomplished in the aPI:

```
@RetryOnFailure(attempts = RetryValues.retrycount, delay = RetryValues.delay, unit = TimeUnit.MILLISECONDS, verbose = RetryValues.verbose)
```

The above line can be added to any method call, and if the method throws and excetion, the method is retried.  The configuration for the Aspects are in a class, and can be chnaged as needed:

```
package com.fisglobal.api.helper;

public interface RetryValues
{
	public final static int delay = 100;
	public final static int retrycount = 3;
	public final static boolean verbose = true;
}

```
  
The important thing to note is that any change to the code *requires* full rebuild of the API.  As aspects are used thruout the API, and they are injected in to the code at compile time, a complete re-packaging of the API is needed when a change is made.  

## Configuration 

Runtime configuration of the API is accomplished by making changes to the config.properties file:

```
#this is a line of comment

HOST_NAME=localhost, 127.0.0.1
HOST_PORT=6379, 6379

REDIS_PASSWORD=

#Connection timeout is in mseconds
REDIS_CONNECTION_TIMEOUT=5000

#how often to check for redis connection in mseconds
REDIS_CONNECTION_CHECK=100

#Data expire value is in seconds
REDIS_DATA_EXPIRE=999999

# seconds: time too live for an expiry lock
LOCK_TTL=1

# milliseconds: delay between pullings for checking the lock state, only affects clients that are waiting for a lock release
LOCK_PULL_TIME=30

# milliseconds: delay between lock renewals, only used for fixed locks, usually because the primary worker of a partition wants to monopolize that partition
LOCK_RENEW_TIME=800

# gRPC Port for Event Service
port=8080

#Server-side SSL 
SERVER_SIDE_SSL=false

```
There are comments in the config file.  The connection timeout and check values are all in milliseconds.  Hostname[s] and port[s] are circular in that if we are building 4 pools but only two host names are given, the four pools will connect to host_1, host_2, host_1, host_2.  


