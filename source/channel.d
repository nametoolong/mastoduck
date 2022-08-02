module mastoduck.channel;

import mastoduck.auth;

import std.algorithm.comparison : among;
import std.conv : to, toChars;
import std.format : format;
import std.meta : allSatisfy;
import std.traits : isSomeString;
import std.typecons;
import std.sumtype;

import vibe.core.log;

@safe:

public:

alias ChannelDefaults = Tuple!(string, "channelId", Ternary, "localOnly");

immutable(ChannelDefaults[string]) publicChannels;
immutable(ChannelDefaults[string]) hashTagChannels;

shared static this()
{
	publicChannels["public"] = ChannelDefaults("timeline:public", Ternary.unknown);
	publicChannels["public:media"] = ChannelDefaults("timeline:public:media", Ternary.unknown);
	publicChannels["public:local"] = ChannelDefaults("timeline:public:local", Ternary.yes);
	publicChannels["public:local:media"] = ChannelDefaults("timeline:public:local:media", Ternary.yes);
	publicChannels["public:remote"] = ChannelDefaults("timeline:public:remote", Ternary.no);
	publicChannels["public:remote:media"] = ChannelDefaults("timeline:public:remote:media", Ternary.no);
	hashTagChannels["hashtag"] = ChannelDefaults("timeline:hashtag:%r", Ternary.yes);
	hashTagChannels["hashtag:local"] = ChannelDefaults("timeline:hashtag:%r:local", Ternary.yes);
}

enum SubscriptionOption
{
	needsFiltering = 1 << 0,
	allowLocalOnly = 1 << 1,
}

alias SubscriptionFlags = BitFlags!SubscriptionOption;

const struct SubscriptionRequest
{
	AuthenticationInfo authInfo;
	string channelName;
	string listId;
	string tagName;
	bool allowLocalOnly;
}

struct StreamName
{
	string channelName;
	Nullable!(string, null) extra;

	this() @disable;

	this(string channelName, string extra = null)
	{
		this.channelName = channelName;
		this.extra = extra;
	}

	bool opEquals(const(StreamName) s) const
	{
		return s.channelName == channelName && s.extra == extra;
	}

	string toString() const
	{
		return extra.isNull ? channelName : format("%s/%s", channelName, extra.get());
	}
}

immutable struct SubscriptionInfo
{
	StreamName streamName;
	string[] channelIds;
	SubscriptionFlags flags;
}

immutable struct SubscriptionError
{
	string msg;
}

alias SubscriptionResult = SumType!(SubscriptionInfo, SubscriptionError);

SubscriptionResult requestToSubscriptionInfo(scope SubscriptionRequest request)
{
	AuthenticationInfo authInfo = request.authInfo;
	string channelName = request.channelName;

	if (auto defaults = channelName in publicChannels)
	{
		return publicChannelInfo(channelName, *defaults, request.allowLocalOnly);
	}
	else if (auto defaults = channelName in hashTagChannels)
	{
		if (request.tagName.length == 0)
		{
			return errorResult("No tag for stream provided");
		}

		return publicChannelInfo(channelName, *defaults, true, request.tagName);
	}
	else if (channelName == "list")
	{
		long listId;

		try
		{
			listId = to!long(request.listId);
		}
		catch (Exception e)
		{
			return errorResult("Invalid list id");
		}

		if (!authorizeListAccess(authInfo, listId))
		{
			return errorResult("Not authorized to stream this list");
		}

		return successResult(
			StreamName(channelName, request.listId),
			[format("timeline:list:%d", listId)],
			SubscriptionFlags.init | SubscriptionOption.allowLocalOnly
		);
	}
	else
	{
		logDebug("Checking OAuth scopes for %s", channelName);

		bool isNotificationChannel = channelName == "user:notification";
		bool scopesMatch = isNotificationChannel ?
			authInfo.isInScope("read", "read:notifications") :
			authInfo.isInScope("read", "read:statuses");

		if (!scopesMatch)
		{
			return errorResult("Access token does not cover required scopes");
		}

		return userChannelInfo(authInfo, channelName);
	}
}

private:

auto errorResult(immutable(string) msg)
{
	return SubscriptionResult(SubscriptionError(msg));
}

auto successResult(
	StreamName name,
	immutable(string[]) ids,
	immutable(SubscriptionFlags) flags)
{
	return SubscriptionResult(SubscriptionInfo(name, ids, flags));
}

SubscriptionResult publicChannelInfo(
	string channelName,
	immutable(ChannelDefaults) defaults,
	bool allowLocalOnly,
	string tag = null)
{
	import std.string : strip;
	import std.uni : toLower;

	SubscriptionFlags flags;
	flags |= SubscriptionOption.needsFiltering;

	if (defaults.localOnly == Ternary.yes ||
		(defaults.localOnly == Ternary.unknown && allowLocalOnly))
	{
		flags |= SubscriptionOption.allowLocalOnly;
	}

	if (tag)
	{
		string tagChannelId = tag.strip().toLower();

		return successResult(
			StreamName(channelName, tag),
			[format(defaults.channelId, tagChannelId)],
			flags
		);
	}
	else
	{
		return successResult(
			StreamName(channelName),
			[defaults.channelId],
			flags
		);
	}
}

SubscriptionResult userChannelInfo(AuthenticationInfo authInfo, string name)
{
	if (!authInfo)
	{
		return errorResult("Missing access token");
	}

	auto accountId = toChars(authInfo.accountId);

	StreamName streamName = StreamName(name);
	SubscriptionFlags flags = SubscriptionFlags.init | SubscriptionOption.allowLocalOnly;

	switch (name)
	{
	case "user":
		return successResult(
			streamName,
			channelsForUserStream(authInfo, accountId),
			flags
		);
	case "user:notification":
		return successResult(
			streamName,
			[format("timeline:%r:notifications", accountId)],
			flags
		);
	case "direct":
		return successResult(
			streamName,
			[format("timeline:direct:%r", accountId)],
			flags
		);
	default:
		return errorResult("Invalid channel name");
	}
}

bool isInScope(Types...)(AuthenticationInfo authInfo, Types requiredScopes)
	if (Types.length != 0 && allSatisfy!(isSomeString, Types))
{
	if (!authInfo)
	{
		return false;
	}

	foreach (scopeName; authInfo.scopes)
	{
		if (scopeName.among(requiredScopes))
		{
			return true;
		}
	}

	return false;
}

immutable(string[])
channelsForUserStream(Range)
	(AuthenticationInfo authInfo, Range accountId)
in (authInfo !is null)
{
	string[] channelIds;
	channelIds.reserve(3);

	channelIds ~= format("timeline:%r", accountId);

	if (authInfo.isInScope("crypto") && !authInfo.deviceId.isNull)
	{
		channelIds ~= format("timeline:%r:%r", accountId, toChars(authInfo.deviceId.get()));
	}

	if (authInfo.isInScope("read", "read:notifications"))
	{
		channelIds ~= format("timeline:%r:notifications", accountId);
	}

	return channelIds.idup;
}
