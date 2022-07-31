module mastoduck.stream;

import mastoduck.auth;
import mastoduck.channel;
import mastoduck.db;

import std.algorithm.mutation : remove, SwapStrategy;
import std.algorithm.searching : canFind;
import std.array : array;
import std.container.rbtree : RedBlackTree;
import std.datetime : dur;
import std.exception : enforce;
import std.format : format;
import std.typecons;
import std.random;

import ddb.postgres : PGCommand, PGType;
import vibe.core.channel;
import vibe.vibe;

enum MessageType
{
	event,
	heartbeat,
	error
}

struct PushMessage
{
	MessageType type;
	Nullable!StreamName streamName;
	string event;
	string payload;
	long queuedAt;
}

@safe:

struct SubscriptionManager
{
	static Nullable!ulong nextConnId()
	{
		foreach (_; 0..3)
		{
			ulong rand = rng.front;
			rng.popFront();

			if (!(rand in stateMap))
			{
				return Nullable!ulong(rand);
			}
		}

		return Nullable!ulong.init;
	}

	static void addState(ConnectionState state)
	{
		enforce(!(state.connId in stateMap),
			format("SubscriptionManager: adding a duplicate connId %d", state.connId));
		stateMap[state.connId] = state;
	}

	static void removeState(ConnectionState state)
	{
		enforce(state.connId in stateMap, 
			format("SubscriptionManager: removing an invalid connId %d", state.connId));
		stateMap.remove(state.connId);
	}

	static void subscribe(ConnectionState state, string channelName)
	{
		enforce(state.connId in stateMap, "Can't find connId in stateMap");

		if (!(channelName in connIdByChannel))
		{
			connIdByChannel[channelName] = new ConnectionIdSet();
		}

		connIdByChannel[channelName].insert(state.connId);
		RedisConnector.getInstance().subscribe(channelName);
	}

	static void unsubscribe(ConnectionState state, string channelName)
	{
		enforce(state.connId in stateMap, "Can't find connId in stateMap");
		enforce(channelName in connIdByChannel, "Invalid channel name to unsubscribe");

		connIdByChannel[channelName].removeKey(state.connId);

		if (connIdByChannel[channelName].empty)
		{
			connIdByChannel.remove(channelName);
			RedisConnector.getInstance().unsubscribe(channelName);
		}
	}

	static void listenRedis() @trusted
	{
		RedisConnector.getInstance().listen(toDelegate(&redisCallback));
		setTimer(dur!"seconds"(channelHeartbeatInterval), toDelegate(&tellSubscribed), true);
	}

private:
	enum long channelHeartbeatInterval = 6 * 60;

	alias ConnectionIdSet = RedBlackTree!ulong;

	static Mt19937_64 rng;
	static ConnectionState[ulong] stateMap;
	static ConnectionIdSet[string] connIdByChannel;

	static this()
	{
		rng.seed(unpredictableSeed!ulong);
	}

	static void redisCallback(string channelName, string message) nothrow
	{
		if (channelName == "Error")
		{
			logError("Redis pubsub error: %s", message);
			return;
		}

		if (!(channelName in connIdByChannel))
		{
			logError("Received a Redis message from an unknown channel %s", channelName);
			return;
		}

		runTask((string name, string msg) {
			auto connIds = array(connIdByChannel[name][]);

			foreach (ulong id; connIds)
			{
				logDebug("Pushing message to connection %d", id);
				if (auto state = id in stateMap)
				{
					state.onMessage(name, msg);
				}
			}
		}, channelName, message);
	}

	static void tellSubscribed() nothrow
	{
		try
		{
			auto channelNames = array(connIdByChannel.byKey());

			foreach (string name; channelNames)
			{
				RedisConnector.getInstance().setEX(
					format("subscribed:%r", name),
					"1", 3 * channelHeartbeatInterval);
			}
		}
		catch (Exception e)
		{
			logFatal("Redis connection error: %s", e.msg);
		}
	}
}

class ConnectionState
{
	static ConnectionState create(scope const(HTTPServerRequest) req) @trusted
	{
		auto maybeConnId = SubscriptionManager.nextConnId();

		if (maybeConnId.isNull)
		{
			return null;
		}

		logDebug("Creating connection state with id %d", maybeConnId.get());

		AuthenticationInfo authInfo = req.context.get!(AuthenticationInfo)
			("authenticationInfo", cast(AuthenticationInfo)null);
		ConnectionState instance = new ConnectionState(authInfo, maybeConnId.get());
		SubscriptionManager.addState(instance);
		instance.subscribeToSystemChannel();
		return instance;
	}

	AuthenticationInfo getAuthInfo()
	{
		return authContext.isNull ? null : authContext.get().authInfo;
	}

	void subscribeToChannels(SubscriptionInfo info)
	{
		foreach (channelId; info.channelIds)
		{
			StreamName[] names = streamNamesById.require(channelId, []);

			if (canFind(names, info.streamName))
			{
				logDebug("Already listening on %s->%s, doing nothing",
					channelId, info.streamName);
				continue;
			}

			logDebug("Appending %s to channel names under id %s, original %s",
				info.streamName, channelId, names);

			streamNamesById[channelId] ~= info.streamName;
			subscribedChannels[channelId] = info.flags;
			SubscriptionManager.subscribe(this, channelId);
		}
	}

	void unsubscribeFromChannels(SubscriptionInfo info)
	{
		foreach (channelId; info.channelIds)
		{
			auto names = channelId in streamNamesById;

			if (names is null || !canFind(*names, info.streamName))
			{
				continue;
			}

			streamNamesById[channelId] = remove!(
				name => name == info.streamName,
				SwapStrategy.unstable
			)(*names);

			if (streamNamesById[channelId].length == 0)
			{
				subscribedChannels.remove(channelId);
				streamNamesById.remove(channelId);
				SubscriptionManager.unsubscribe(this, channelId);
			}
		}
	}

	void onMessage(string channelName, string message) nothrow
	{
		Json json;

		try
		{
			json = parseJsonString(message);
		}
		catch (Exception e)
		{
			logDebug("Error parsing message from Redis: %s", e.message);
			return;
		}

		try
		{
			processMessage(channelName, json);
		}
		catch (Exception e)
		{
			debug
			{
				logException(e, "Error dispatching message");
			}
			else
			{
				logDebug("Error dispatching message: %s", e.msg);
			}

			return;
		}
	}

	PushMessage getMessage()
	{
		return messagePipe.consumeOne();
	}

	void sendHeartbeat()
	{
		PushMessage msg = {type: MessageType.heartbeat};
		tryPutMessage(msg);
	}

	void sendError(string error)
	{
		PushMessage msg = {
			type: MessageType.error,
			payload: error
		};
		tryPutMessage(msg);
	}

	void close() nothrow
	{
		if (closed)
		{
			return;
		}

		closed = true;

		try
		{
			messagePipe.close();
	
			foreach (string name; subscribedChannels.byKey())
			{
				SubscriptionManager.unsubscribe(this, name);
			}
	
			if (!authContext.isNull)
			{
				SubscriptionManager.unsubscribe(this, authContext.get().accessTokenChannelId);
				SubscriptionManager.unsubscribe(this, authContext.get().systemChannelId);
			}
	
			SubscriptionManager.removeState(this);
		}
		catch (Exception e)
		{
			logError("Error cleaning up connection state: %s", e.msg);
		}
	}

	immutable(ulong) connId;

private:
	this(AuthenticationInfo authInfo, ulong id)
	{
		connId = id;

		if (authInfo)
		{
			string accessTokenChannelId =
				format("timeline:access_token:%d",
					authInfo.accessTokenId);
			string systemChannelId =
				format("timeline:system:%d",
					authInfo.accountId);
			authContext = AuthenticationContext(
				authInfo,
				accessTokenChannelId,
				systemChannelId
			);
		}

		messagePipe = createChannel!(PushMessage, messagePipeCapacity);
	}

	void subscribeToSystemChannel()
	{
		if (authContext.isNull)
		{
			return;
		}

		SubscriptionManager.subscribe(this, authContext.get().accessTokenChannelId);
		SubscriptionManager.subscribe(this, authContext.get().systemChannelId);
	}

	void tryPutMessage(const(PushMessage) msg)
	{
		// Dangerous hack
		// Only works in a single-threaded environment
		if (messagePipe.bufferFill == messagePipeCapacity)
		{
			logWarn("Dropping a message because queue is full");
			return;
		}

		logDebug("Pushing message with type %d", msg.type);
		messagePipe.put(msg);
	}

	void processSystemMessage(string event)
	{
		if (event == "kill")
		{
			logInfo("Closing connection for %d due to expired access token",
				authContext.get().authInfo.accountId);
			close();
		}
	}

	void processMessage(string channel, Json json)
	{
		string event = json["event"].get!string();

		if (!authContext.isNull &&
			(channel == authContext.get().accessTokenChannelId ||
				channel == authContext.get().systemChannelId))
		{
			processSystemMessage(event);
			return;
		}

		Json payload = json["payload"];
		long queuedAt = 0;

		if (json["queued_at"].type() == Json.Type.int_)
		{
			queuedAt = json["queued_at"].get!long();
		}

		if (event == "update")
		{
			enforce(payload.type() == Json.Type.object,
				"Payload must be an object");

			bool localOnly =
				payload["local_only"].type() == Json.Type.bool_ ?
					payload["local_only"].get!bool() : false;
	
			if (localOnly &&
				(authContext.isNull ||
					!subscribedChannels[channel].allowLocalOnly))
			{
				logDebug("Message %s filtered because it was local-only",
					payload["id"].get!string());
				return;
			}

			if (subscribedChannels[channel].needsFiltering)
			{
				bool shouldSend = filterMessage(payload);
	
				if (!shouldSend)
				{
					return;
				}
			}
		}

		foreach (ref name; streamNamesById[channel])
		{
			logDebug("Pushing %s message to channel %s", event, name);

			string encodedPayload =
				payload.type() != Json.Type.string ?
					serializeToJsonString(payload) :
					payload.get!string();

			PushMessage msg = {
				type: MessageType.event,
				streamName: name,
				event: event,
				payload: encodedPayload,
				queuedAt: queuedAt
			};
	
			tryPutMessage(msg);
		}
	}

	bool filterMessage(Json payload)
	{
		if (authContext.isNull)
		{
			return true;
		}

		if (payload["language"].type() == Json.Type.string)
		{
			string language = payload["language"].get!string();
			auto chosenLanguages = authContext.get().authInfo.chosenLanguages;

			if (chosenLanguages.length != 0 &&
				!chosenLanguages.canFind(language))
			{
				logDebug("Message %s filtered by language %s",
					payload["id"].get!string(), language);
				return false;
			}
		}

		long acctId = authContext.get().authInfo.accountId;
		long sourceId = payload["account"]["id"].to!long();
		string[] accountName = payload["account"]["acct"].get!string().split("@");
		Json mentions = payload["mentions"];
		long[] mentionedIds;

		if (mentions.type() == Json.Type.array)
		{
			mentionedIds.length = mentions.length;

			for (size_t i = 0; i < mentionedIds.length; i++)
			{
				mentionedIds[i] = mentions[i]["id"].to!long();
			}
		}

		bool result = checkAccountBlock(acctId, sourceId, mentionedIds);

		if (accountName.length >= 2)
		{
			string accountDomain = accountName[1];
			result = result && checkDomainBlock(acctId, accountDomain);
		}

		if (!result)
		{
			logDebug("Message %s filtered by blocks", payload["id"].get!string());
		}

		return result;
	}

	bool checkAccountBlock(long acctId, long sourceId, long[] mentionedIds) @trusted
	{
		enum statementTemplate =
			`SELECT 1
			FROM blocks
			WHERE (account_id = $1 AND target_account_id IN (%r))
				OR (account_id = $2 AND target_account_id = $1)
			UNION
			SELECT 1
			FROM mutes
			WHERE account_id = $1 AND target_account_id IN (%r)`;

		enforce(mentionedIds.length < 126, "Too many mentioned accounts");

		string[] placeholders = new string[mentionedIds.length + 1];

		for (size_t i = 0; i < placeholders.length; i++)
		{
			placeholders[i] = format("$%d", i + 2);
		}

		string placeholderList = placeholders.join(",");
		string statement = format(statementTemplate, placeholderList, placeholderList);

		auto conn = PostgresConnector.getInstance().lockConnection();
		auto cmd = new PGCommand(conn, statement);
		cmd.parameters.add(1, PGType.INT8).value = acctId;
		cmd.parameters.add(2, PGType.INT8).value = sourceId;

		for (size_t i = 0; i < mentionedIds.length; i++)
		{
			cmd.parameters.add(cast(short)(i + 3), PGType.INT8).value = mentionedIds[i];
		}

		auto result = cmd.executeQuery();
		scope (exit) {
			result.close();
		}

		if (result.empty())
		{
			return true;
		}

		return false;
	}

	bool checkDomainBlock(long acctId, string accountDomain) @trusted
	{
		enum statement =
			`SELECT 1
			FROM account_domain_blocks
			WHERE account_id = $1 AND domain = $2`;

		auto conn = PostgresConnector.getInstance().lockConnection();
		auto cmd = new PGCommand(conn, statement);
		cmd.parameters.add(1, PGType.INT8).value = acctId;
		cmd.parameters.add(2, PGType.TEXT).value = accountDomain;

		auto result = cmd.executeQuery();
		scope (exit) {
			result.close();
		}

		if (result.empty())
		{
			return true;
		}

		return false;
	}

	const struct AuthenticationContext
	{
		AuthenticationInfo authInfo;
		string accessTokenChannelId;
		string systemChannelId;
	}

	Nullable!AuthenticationContext authContext;
	SubscriptionFlags[string] subscribedChannels;
	StreamName[][string] streamNamesById;

	enum messagePipeCapacity = 8;
	Channel!(PushMessage, messagePipeCapacity) messagePipe;

	bool closed = false;
}
