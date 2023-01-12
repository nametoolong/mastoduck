module mastoduck.main;

import mastoduck.db;
import mastoduck.env;
import mastoduck.stream;
import mastoduck.web;

import std.conv : to;
import std.typecons : Tuple;

import vibe.vibe;

alias PostgresSettings = Tuple!(
	immutable(string[string]), "params",
	uint, "concurrency"
);

immutable(PostgresSettings) getDbSettings()
{
	string hostVar = Env["DB_HOST"];
	string portVar = Env["DB_PORT"];
	string userVar = Env["DB_USER"];
	string passVar = Env["DB_PASS"];
	string dbName = Env["DB_NAME"];
	string poolSize = Env["DB_POOL"];

	if (!dbName) {
		final switch (Env.envType)
		{
		case EnvType.development:
			dbName = "mastodon_development";
			break;
		case EnvType.production:
			dbName = "mastodon_production";
			break;
		case EnvType.test:
			dbName = "mastodon_test";
			break;
		case EnvType.unspecified:
			dbName = "mastodon";
		}
	}

	return PostgresSettings(
		[
			"host": hostVar.length != 0 ? hostVar : "localhost",
			"port": portVar.length != 0 ? portVar : "5432" ,
			"user": userVar.length != 0 ? userVar : "mastodon",
			"password": passVar,
			"database": dbName
		],
		poolSize.length != 0 ? to!uint(poolSize) : 16
	);
}

alias RedisSettings = Tuple!(
	string, "host",
	ushort, "port",
	string, "password",
	long, "dbIndex"
);

immutable(RedisSettings) getRedisSettings()
{
	string hostVar = Env["REDIS_HOST"];
	string portVar = Env["REDIS_PORT"];
	string passwordVar = Env["REDIS_PASSWORD"];
	string redisDbIndex = Env["REDIS_DB"];

	return RedisSettings(
		hostVar.length != 0 ? hostVar : "localhost",
		portVar.length != 0 ? to!ushort(portVar) : 6379,
		passwordVar,
		redisDbIndex ? to!long(redisDbIndex) : 0
	);
}

HTTPServerSettings getServerSettings()
{
	string bindVar = Env["BIND"];
	string portVar = Env["PORT"];

	auto settings = new HTTPServerSettings;
	settings.bindAddresses = [bindVar ? bindVar : "127.0.0.1"];
	settings.port = portVar ? to!ushort(portVar) : 4000;
	settings.errorPageHandler = toDelegate(&writeJsonErrorInfo);
	settings.webSocketPingInterval = 30.seconds;

	return settings;
}

void main()
{
	Env.loadEnv();

	debug
	{
		setLogLevel(LogLevel.verbose4);
	}

	PostgresConnector.initialize(getDbSettings().expand);
	scope (exit)
	{
		PostgresConnector.cleanup();
	}

	RedisConnector.initialize(getRedisSettings().expand);
	scope (exit)
	{
		RedisConnector.cleanup();
	}
	
	auto settings = getServerSettings();

	auto listener = listenHTTP(settings, buildRequestHandler());
	scope (exit)
	{
		listener.stopListening();
	}

	logInfo("Listening on %(%s, %); port %d.", settings.bindAddresses, settings.port);

	SubscriptionManager.listenRedis();

	runApplication();
}
