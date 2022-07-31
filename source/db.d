module mastoduck.db;

import ddb.postgres;
import vibe.vibe;

@safe:

class NotInitializedError : Exception
{
	this(string msg)
	{
		super(msg);
	}
}

class SingletonConnector(T)
{
public:
	static void initialize(Args...)(Args arguments)
	{
		_instance = new T(arguments);
	}

	static T getInstance()
	{
		if (_instance is null)
		{
			throw new NotInitializedError(T.classinfo.name ~ " is not initialized");
		}

		return _instance;
	}

	static void cleanup()
	{
		if (_instance !is null)
		{
			_instance.doCleanup();
		}
	}

protected:
	abstract void doCleanup();

private:
	static T _instance;
}

class PostgresConnector : SingletonConnector!(PostgresConnector)
{
public:
	auto lockConnection()
	{
		return connectionPool.lockConnection();
	}

protected:
	this(immutable(string[string]) settings)
	{
		dbSettings = settings;
		connectionPool = new ConnectionPool!PGConnection(&createConnection, 16);
	}

	override void doCleanup()
	{
		connectionPool.removeUnused((conn) @trusted {
			try
			{
				conn.close();
			}
			catch (Exception) {}
		});
	}

private:
	PGConnection createConnection() @trusted
	{
		return new PGConnection(dbSettings);
	}

	immutable(string[string]) dbSettings;
	ConnectionPool!PGConnection connectionPool;
}

class RedisConnector : SingletonConnector!(RedisConnector)
{
public:
	void subscribe(string channelName)
	{
		redisSubscriber.subscribe(channelName);
	}

	void unsubscribe(string channelName)
	{
		redisSubscriber.unsubscribe(channelName);
	}

	void listen(F)(F callback)
	{
		redisSubscriber.listen(callback);
	}

	void setEX(string key, string value, long seconds)
	{
		redisDatabase.setEX(key, seconds, value);
	}

protected:
	this(string host, ushort port, string password, long dbIndex)
	{
		redisClient = connectRedis(host, port);

		if (password)
		{
			redisClient.auth(password);
		}

		redisSubscriber = redisClient.createSubscriber();
		redisDatabase = redisClient.getDatabase(dbIndex);
	}

	override void doCleanup()
	{
		redisSubscriber.bstop();
		redisClient.quit();
		redisClient.releaseUnusedConnections();
	}

private:
	RedisClient redisClient;
	RedisSubscriber redisSubscriber;
	RedisDatabase redisDatabase;
}
