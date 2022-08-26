module mastoduck.stream;

import mastoduck.auth;
import mastoduck.channel;
import mastoduck.db;
import mastoduck.filter;
import mastoduck.util;

import std.algorithm.iteration : map;
import std.algorithm.mutation : remove, SwapStrategy;
import std.algorithm.searching : any, canFind;
import std.array : array;
import std.conv : to;
import std.datetime : dur;
import std.exception : enforce;
import std.format : format;
import std.typecons;

import ddb.postgres : PGCommand, PGType;
import vibe.core.channel;
import vibe.vibe;

@safe:

immutable struct AccountInfo
{
	long id;
	string acct;

	this() @disable;

	static AccountInfo fromBSON(Bson bson)
	{
		Nullable!long id;
		string acct;

		foreach (key, value; bson.byKeyValue())
		{
			switch (key)
			{
			case "id":
				id = to!long(value.get!string());
				break;
			case "acct":
				acct = value.get!string();
				break;
			default:
			}
		}

		enforce(!id.isNull, "Account.id must exist");
		enforce(acct.length != 0, "Account name must not be empty");

		AccountInfo obj = {
			id: id.get(),
			acct: acct
		};

		return obj;
	}
}

struct StreamData
{
	string channelName;
	string event;
	Bson payload;
	long queuedAt;

	static StreamData fromBSON(string channelName, Bson bson)
	{
		string event;
		Bson payload;
		long queuedAt;

		foreach (key, value; bson.byKeyValue())
		{
			switch (key)
			{
			case "event":
				event = value.get!string();
				break;
			case "payload":
				payload = value;
				break;
			case "queued_at":
				queuedAt = value.tryReadInteger(0);
				break;
			default:
			}
		}

		enforce(channelName.length != 0, "Channel name must not be empty");
		enforce(event.length != 0, "Event must not be empty");
		enforce(payload.type != Bson.Type.undefined, "Payload must be an object");

		StreamData obj = {
			channelName: channelName,
			event: event,
			payload: payload,
			queuedAt: queuedAt
		};

		return obj;
	}
}

enum MessageType
{
	event,
	heartbeat,
	error
}

struct StreamMessage
{
	MessageType type;
	string errorMessage;
	StreamData data;
}

const struct PushMessage
{
	MessageType type;
	StreamName[] streamNames;
	string event;
	string payload;
	long queuedAt;
}

struct SubscriptionManager
{
	static void subscribe(ConnectionState state, string channelName)
	in (state !is null && channelName.length != 0)
	{
		ConnectionState[] ids = connStateByChannel.require(channelName, []);

		if (ids.any!(o => o is state))
		{
			return;
		}

		connStateByChannel[channelName] ~= state;
		RedisConnector.getInstance().subscribe(channelName);
		setSubscribedFlag(channelName);
	}

	static void unsubscribe(ConnectionState state, string channelName)
	in (state !is null && channelName.length != 0)
	{
		enforce(channelName in connStateByChannel, "Invalid channel name to unsubscribe");

		connStateByChannel[channelName] = remove!(
			o => o is state,
			SwapStrategy.unstable
		)(connStateByChannel[channelName].dup);

		if (connStateByChannel[channelName].length == 0)
		{
			connStateByChannel.remove(channelName);
			RedisConnector.getInstance().unsubscribe(channelName);
			// Note: a race condition can happen with another task
			// in setSubscribedFlag. At worst this causes a user to
			// receive no streaming events, but fixing this
			// requires either coarse-grained locking or less precise
			// detection of online status from Ruby side.
			// Leave this as is for this moment.
			delSubscribedFlag(channelName);
		}
	}

	static void listenRedis() @trusted
	{
		RedisConnector.getInstance().listen(toDelegate(&redisCallback));
		setTimer(dur!"seconds"(channelHeartbeatInterval), toDelegate(&tellSubscribed), true);
	}

private:
	enum long channelHeartbeatInterval = 6 * 60;

	static ConnectionState[][string] connStateByChannel;

	static void redisCallback(string channelName, string content) @trusted nothrow
	{
		if (channelName == "Error")
		{
			logError("Redis pubsub error: %s", content);
			return;
		}

		auto ptr = channelName in connStateByChannel;

		if (ptr is null)
		{
			logError("Received a Redis message from an unknown channel %s", channelName);
			return;
		}

		ConnectionState[] states = *ptr;

		try
		{
			Bson bson = Bson(Bson.Type.object, cast(bdata_t)content);
			StreamData data = StreamData.fromBSON(channelName, bson);

			foreach (state; states)
			{
				state.processMessage(data);
			}
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
		}
	}

	static void tellSubscribed() nothrow
	{
		try
		{
			auto keys = array(connStateByChannel.byKey());

			foreach (string name; keys)
			{
				setSubscribedFlag(name);
			}
		}
		catch (Exception e)
		{
			logFatal("Redis connection error: %s", e.msg);
		}
	}

	static void setSubscribedFlag(string channelName)
	{
		RedisConnector.getInstance().setEX(
			format("subscribed:%r", channelName),
			"1", 3 * channelHeartbeatInterval);
	}

	static void delSubscribedFlag(string channelName)
	{
		RedisConnector.getInstance().del(
			format("subscribed:%r", channelName));
	}
}

class ConnectionState
{
	debug
	{
		~this() @system
		{
			logDebug("Destroying a connection state");
		}
	}

	static ConnectionState create(scope const(HTTPServerRequest) req) @trusted
	{
		AuthenticationInfo authInfo = req.context.get!(AuthenticationInfo)
			("authenticationInfo", cast(AuthenticationInfo)null);
		ConnectionState instance = new ConnectionState(authInfo);
		logDebug("Created a connection state");
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
			if (closed)
			{
				return;
			}

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
			if (closed)
			{
				return;
			}

			if (!(channelId in streamNamesById))
			{
				continue;
			}

			streamNamesById[channelId] = remove!(
				name => name == info.streamName,
				SwapStrategy.unstable
			)(streamNamesById[channelId].dup);

			if (streamNamesById[channelId].length == 0)
			{
				subscribedChannels.remove(channelId);
				streamNamesById.remove(channelId);
				SubscriptionManager.unsubscribe(this, channelId);
			}
		}
	}

	void sendHeartbeat()
	{
		if (closed)
		{
			return;
		}

		StreamMessage message = {
			type: MessageType.heartbeat
		};
		tryPutMessage(message);
	}

	void sendError(string error)
	{
		if (closed)
		{
			return;
		}

		StreamMessage message = {
			type: MessageType.error,
			errorMessage: error
		};
		tryPutMessage(message);
	}

	PushMessage getMessage()
	{
		do
		{
			StreamMessage message = messagePipe.consumeOne();

			if (message.type == MessageType.event)
			{
				StreamData data = message.data;

				try
				{
					bool blocked = data.event == "update" &&
						filterMessage(data.channelName, data.payload);

					if (blocked)
					{
						continue;
					}
				}
				catch (Exception e)
				{
					logError("Error filtering message: %s", e.msg);
				}

				if (auto streamNames = data.channelName in streamNamesById)
				{
					string payload;

					if (data.payload.type == Bson.Type.string)
					{
						payload = data.payload.get!string;
					}
					else
					{
						payload = data.payload.toJsonString();
					}

					PushMessage pushMessage = {
						type: MessageType.event,
						streamNames: *streamNames,
						event: data.event,
						payload: payload,
						queuedAt: data.queuedAt
					};

					return pushMessage;
				}
			}
			else if (message.type == MessageType.heartbeat)
			{
				PushMessage pushMessage = {
					type: MessageType.heartbeat
				};

				return pushMessage;
			}
			else if (message.type == MessageType.error)
			{
				PushMessage pushMessage = {
					type: MessageType.error,
					event: message.errorMessage
				};

				return pushMessage;
			}
			else
			{
				assert(false, "Unexpected message type in getMessage()");
			}
		} while (true);
	}

	/* A little document on message formats:
	 *
	 * {"event": "announcement.delete", "payload": "... (announcement.id as a string)"}
	 *
	 * {"event": "delete", "payload": "... (status.id as a string)"}
	 *
	 * {"event": "announcement", "payload": {... (Announcement object)}}
	 *
	 * {"event": "announcement.reaction", "payload": {... (AnnouncementReactions object)}}
	 *
	 * {"event": "update", "payload": {... (Status object)}}
	 *
	 * {"event": "status.update", "payload": {... (Status object)}}
	 *
	 * {"event": "notification", "payload": {... (Notification object)}}
	 *
	 * {"event": "update", "payload": {... (Status object)},
	 *  "queued_at": ... (milliseconds since Unix epoch)}
	 *
	 * {"event": "status.update", "payload": {... (Status object)}}
	 *  "queued_at": ... (milliseconds since Unix epoch)}
	 *
	 * {"event": "conversation", "payload": {... (Conversation object)},
	 *  "queued_at": ... (milliseconds since Unix epoch)}
	 *
	 * {"event": "encrypted_message", "payload": {... (Conversation object)},
	 *  "queued_at": ... (milliseconds since Unix epoch)}
	 *
	 * {"event": "filters_changed"}
	 *
	 * {"event": "kill"}
	 *
	 */
	void processMessage(StreamData data)
	{
		if (closed)
		{
			return;
		}

		if (!authContext.isNull &&
			(data.channelName == authContext.get().accessTokenChannelId ||
				data.channelName == authContext.get().systemChannelId))
		{
			processSystemMessage(data.event);
			return;
		}

		StreamMessage message = {
			type: MessageType.event,
			data: data
		};
		tryPutMessage(message);
	}

	void close() nothrow
	{
		if (closed)
		{
			return;
		}

		closed = true;

		auto unsubscribeFrom = (string name) nothrow {
			try
			{
				SubscriptionManager.unsubscribe(this, name);
			}
			catch (Exception e)
			{
				logDebug("Error unsubscribing from channel %s: %s",
					name, e.msg);
			}
		};

		messagePipe.close();

		foreach (string name; subscribedChannels.byKey())
		{
			unsubscribeFrom(name);
		}

		if (!authContext.isNull)
		{
			unsubscribeFrom(authContext.get().accessTokenChannelId);
			unsubscribeFrom(authContext.get().systemChannelId);
		}
	}

private:
	this(AuthenticationInfo authInfo)
	{
		if (authInfo)
		{
			string accessTokenChannelId = format(
				"timeline:access_token:%d",
				authInfo.accessTokenId
			);
			string systemChannelId = format(
				"timeline:system:%d",
				authInfo.accountId
			);
			authContext = AuthenticationContext(
				authInfo,
				accessTokenChannelId,
				systemChannelId
			);
		}

		messagePipe = createChannel!(StreamMessage, messagePipeCapacity)();
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

	void processSystemMessage(string event)
	{
		if (event == "kill")
		{
			logInfo("Closing connection for %d due to expired access token",
				authContext.get().authInfo.accountId);
			runTask(&close);
		}
	}

	void tryPutMessage(const(StreamMessage) message)
	{
		// Dangerous hack
		// Only works in a single-threaded environment
		if (messagePipe.bufferFill == messagePipeCapacity)
		{
			logWarn("Dropped a message because queue is full");
			return;
		}

		logDebug("Pushing message with type %d", message.type);
		messagePipe.put(message);
	}

	bool filterMessage(string channel, Bson payload)
	{
		enforce(payload.type() == Bson.Type.object, "Payload must be an object");

		bool localOnly = false;
		string messageId = "unknown";
		Bson account, language, mentions;

		foreach (key, value; payload.byKeyValue())
		{
			switch (key)
			{
			case "local_only":
				localOnly = value.tryReadBool(false);
				break;
			case "id":
				messageId = value.get!string();
				break;
			case "account":
				account = value;
				break;
			case "language":
				language = value;
				break;
			case "mentions":
				mentions = value;
				break;
			default:
			}
		}

		if ((authContext.isNull || !subscribedChannels[channel].allowLocalOnly) &&
			localOnly)
		{
			logDebug("Message %s filtered because it was local-only", messageId);
			return true;
		}

		if (authContext.isNull || !subscribedChannels[channel].needsFiltering)
		{
			return false;
		}

		enforce(account.type() == Bson.Type.object, "Account must be an object");

		AccountInfo acctInfo = AccountInfo.fromBSON(account);

		bool result =
			filterByLanguage(messageId, language) ||
			filterByAccount(messageId, acctInfo, mentions) ||
			filterByDomain(messageId, acctInfo);

		return result;
	}

	bool filterByAccount(string messageId, AccountInfo statusOwner, Bson mentions)
	in (!authContext.isNull)
	{
		long ourId = authContext.get().authInfo.accountId;
		auto mentionedIds = mentions.byValue().map!(o => AccountInfo.fromBSON(o).id);
		bool blocked = isBlockedByAccount(ourId, statusOwner.id, mentionedIds, mentions.length);

		if (blocked)
		{
			logDebug("Filtered message %s because it comes from a blocked or muted account",
				messageId);
		}

		return blocked;
	}

	bool filterByDomain(string messageId, AccountInfo statusOwner)
	in (!authContext.isNull)
	{
		long ourId = authContext.get().authInfo.accountId;
		string[] accountName = statusOwner.acct.split("@");

		if (accountName.length < 2)
		{
			return false;
		}

		string accountDomain = accountName[1];
		bool blocked = isBlockedByDomain(ourId, accountDomain);

		if (blocked)
		{
			logDebug("Filtered message %s because it is from domain %s",
				messageId, accountDomain);
		}

		return blocked;
	}

	bool filterByLanguage(string messageId, Bson language)
	in (!authContext.isNull)
	{
		if (language.type() != Bson.Type.string)
		{
			return false;
		}

		auto chosenLanguages = authContext.get().authInfo.chosenLanguages;
		string langName = language.get!string();
		bool blocked = chosenLanguages.length != 0 && !chosenLanguages.canFind(langName);

		if (blocked)
		{
			logDebug("Filtered message %s because it is in language %s",
				messageId, langName);
		}

		return blocked;
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
	Channel!(StreamMessage, messagePipeCapacity) messagePipe;

	bool closed = false;
}
