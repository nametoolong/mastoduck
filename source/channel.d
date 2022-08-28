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

immutable dstring nonAsciiChars = `ÀÁÂÃÄÅàáâãäåĀāĂăĄąÇçĆćĈĉĊċČčÐðĎďĐđÈÉÊËèéêëĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħÌÍÎÏìíîïĨĩĪīĬĭĮįİıĴĵĶķĸĹĺĻļĽľĿŀŁłÑñŃńŅņŇňŉŊŋÒÓÔÕÖØòóôõöøŌōŎŏŐőŔŕŖŗŘřŚśŜŝŞşŠšſŢţŤťŦŧÙÚÛÜùúûüŨũŪūŬŭŮůŰűŲųŴŵÝýÿŶŷŸŹźŻżŽž`;
immutable dstring equivalentAsciiChars = `AAAAAAaaaaaaAaAaAaCcCcCcCcCcDdDdDdEEEEeeeeEeEeEeEeEeGgGgGgGgHhHhIIIIiiiiIiIiIiIiIiJjKkkLlLlLlLlLlNnNnNnNnnNnOOOOOOooooooOoOoOoRrRrRrSsSsSsSssTtTtTtUUUUuuuuUuUuUuUuUuUuWwYyyYyYZzZzZz`;
immutable(dchar[dchar]) asciiConversionTable;

static assert(nonAsciiChars.length == equivalentAsciiChars.length);

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

	foreach (size_t i, dchar ch; nonAsciiChars)
	{
		asciiConversionTable[ch] = equivalentAsciiChars[i];
	}
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

const struct StreamName
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
		return publicChannelInfo(channelName, *defaults, true, request.tagName);
	}
	else if (channelName == "list")
	{
		return listChannelInfo(authInfo, request.listId);
	}
	else
	{
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

	SubscriptionFlags flags;
	flags |= SubscriptionOption.needsFiltering;

	if (defaults.localOnly == Ternary.yes ||
		(defaults.localOnly == Ternary.unknown && allowLocalOnly))
	{
		flags |= SubscriptionOption.allowLocalOnly;
	}

	if (tag)
	{
		string normalizedTag = normalizeTag(tag);

		if (normalizedTag.length == 0)
		{
			return errorResult("Invalid tag name");
		}

		return successResult(
			StreamName(channelName, tag),
			[format(defaults.channelId, normalizedTag)],
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

SubscriptionResult listChannelInfo(AuthenticationInfo authInfo, string listIdStr)
{
	long listId;

	try
	{
		listId = to!long(listIdStr);
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
		StreamName("list", listIdStr),
		[format("timeline:list:%d", listId)],
		SubscriptionFlags.init | SubscriptionOption.allowLocalOnly
	);
}

SubscriptionResult userChannelInfo(AuthenticationInfo authInfo, string channelName)
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

	auto accountId = toChars(authInfo.accountId);
	StreamName streamName = StreamName(channelName);
	SubscriptionFlags flags = SubscriptionFlags.init | SubscriptionOption.allowLocalOnly;

	switch (channelName)
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

string normalizeTag(string tag) @trusted
{
	import std.encoding;
	import std.string;
	import std.regex;
	import std.uni;

	if (!tag.isValid())
	{
		return "";
	}

	dchar[] tagNameUtf32;
	tag.transcode(tagNameUtf32);
	tagNameUtf32 = tagNameUtf32.strip().normalize!NFKC();
	tagNameUtf32.toLowerInPlace();

	foreach (ref dchar ch; tagNameUtf32)
	{
		if (auto substitute = ch in asciiConversionTable)
		{
			ch = *substitute;
		}
	}

	enum regex = regex("[^\\p{L}\\p{N}_\\u00b7\\u200c]"d);
	tagNameUtf32 = tagNameUtf32.replaceAll(regex, ""d);

	string normalizedTag;
	tagNameUtf32.transcode(normalizedTag);
	return normalizedTag;
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
channelsForUserStream(Range)(AuthenticationInfo authInfo, Range accountId)
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

unittest
{
	// The tests are taken from Mastodon's hashtag_normalizer_spec.rb
	assert(normalizeTag("Ｓｙｎｔｈｗａｖｅ") == "synthwave");
	assert(normalizeTag("ｼｰｻｲﾄﾞﾗｲﾅｰ") == "シーサイドライナー");
	assert(normalizeTag("BLÅHAJ") == "blahaj");
	assert(normalizeTag("#foo") == "foo");
	assert(normalizeTag("a·b") == "a·b");

	// Invalid tags
	assert(normalizeTag(" ") == "");
	assert(normalizeTag("\x80\x99") == "");
}

@system:
unittest
{
	bool conditionsHold(bool[] cond...)
	{
		import std.algorithm.iteration : fold;
		return cond.fold!((a, b) => a && b)(true);
	}

	assert(publicChannelInfo("public", publicChannels["public"], false).match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "public",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:public",
			(cast(int) info.flags) ==
				SubscriptionOption.needsFiltering
		),
		(SubscriptionError err) => false
	));

	assert(publicChannelInfo("public", publicChannels["public"], true).match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "public",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:public",
			(cast(int) info.flags) == (
				SubscriptionOption.needsFiltering |
				SubscriptionOption.allowLocalOnly
			)
		),
		(SubscriptionError err) => false
	));

	assert(publicChannelInfo("public:local", publicChannels["public:local"], false).match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "public:local",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:public:local",
			(cast(int) info.flags) == (
				SubscriptionOption.needsFiltering |
				SubscriptionOption.allowLocalOnly
			)
		),
		(SubscriptionError err) => false
	));

	assert(publicChannelInfo("public:remote", publicChannels["public:remote"], true).match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "public:remote",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:public:remote",
			(cast(int) info.flags) ==
				SubscriptionOption.needsFiltering
		),
		(SubscriptionError err) => false
	));

	assert(publicChannelInfo("hashtag", hashTagChannels["hashtag"], false, "").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Invalid tag name"
	));

	assert(publicChannelInfo("hashtag", hashTagChannels["hashtag"], false, " \x80\x99 ").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Invalid tag name"
	));

	assert(publicChannelInfo("hashtag", hashTagChannels["hashtag"], true, "BLÅHAJ").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "hashtag",
			info.streamName.extra.get() == "BLÅHAJ",
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:hashtag:blahaj",
			(cast(int) info.flags) == (
				SubscriptionOption.needsFiltering |
				SubscriptionOption.allowLocalOnly
			)
		),
		(SubscriptionError err) => false
	));

	assert(listChannelInfo(null, "").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Invalid list id"
	));

	assert(listChannelInfo(null, "a").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Invalid list id"
	));

	assert(listChannelInfo(null, "-1").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Not authorized to stream this list"
	));

	assert(userChannelInfo(null, "user").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Access token does not cover required scopes"
	));

	assert(userChannelInfo(null, "user:notification").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Access token does not cover required scopes"
	));

	AuthenticationInfo authInfo = new AuthenticationInfo(
		AuthenticationInfo.Row(1234, 5678, [], "whatever read crypto", Nullable!long(8765))
	);

	assert(userChannelInfo(authInfo, "user").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "user",
			info.streamName.extra.isNull,
			info.channelIds.length == 3,
			info.channelIds[0] == "timeline:5678",
			info.channelIds[1] == "timeline:5678:8765",
			info.channelIds[2] == "timeline:5678:notifications",
			(cast(int) info.flags) ==
				SubscriptionOption.allowLocalOnly
		),
		(SubscriptionError err) => false
	));

	assert(userChannelInfo(authInfo, "user:notification").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "user:notification",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:5678:notifications",
			(cast(int) info.flags) ==
				SubscriptionOption.allowLocalOnly
		),
		(SubscriptionError err) => false
	));

	assert(userChannelInfo(authInfo, "direct").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "direct",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:direct:5678",
			(cast(int) info.flags) ==
				SubscriptionOption.allowLocalOnly
		),
		(SubscriptionError err) => false
	));

	assert(userChannelInfo(authInfo, "unknown").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Invalid channel name"
	));

	AuthenticationInfo authInfoScopeNotifications = new AuthenticationInfo(
		AuthenticationInfo.Row(1234, 5678, [], "read:notifications", Nullable!long.init)
	);

	assert(userChannelInfo(authInfoScopeNotifications, "user").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Access token does not cover required scopes"
	));

	assert(userChannelInfo(authInfoScopeNotifications, "user:notification").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "user:notification",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:5678:notifications",
			(cast(int) info.flags) ==
				SubscriptionOption.allowLocalOnly
		),
		(SubscriptionError err) => false
	));

	AuthenticationInfo authInfoScopeStatuses = new AuthenticationInfo(
		AuthenticationInfo.Row(1234, 5678, [], "read:statuses", Nullable!long(8765))
	);

	assert(userChannelInfo(authInfoScopeStatuses, "user").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "user",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:5678",
			(cast(int) info.flags) ==
				SubscriptionOption.allowLocalOnly
		),
		(SubscriptionError err) => false
	));

	assert(userChannelInfo(authInfoScopeStatuses, "user:notification").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Access token does not cover required scopes"
	));

	AuthenticationInfo authInfoNoDeviceId = new AuthenticationInfo(
		AuthenticationInfo.Row(1234, 5678, [], "whatever read crypto", Nullable!long.init)
	);

	assert(userChannelInfo(authInfoNoDeviceId, "user").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "user",
			info.streamName.extra.isNull,
			info.channelIds.length == 2,
			info.channelIds[0] == "timeline:5678",
			info.channelIds[1] == "timeline:5678:notifications",
			(cast(int) info.flags) ==
				SubscriptionOption.allowLocalOnly
		),
		(SubscriptionError err) => false
	));

	AuthenticationInfo authInfoNoCrypto = new AuthenticationInfo(
		AuthenticationInfo.Row(1234, 5678, [], "whatever read", Nullable!long(8765))
	);

	assert(userChannelInfo(authInfoNoCrypto, "user").match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "user",
			info.streamName.extra.isNull,
			info.channelIds.length == 2,
			info.channelIds[0] == "timeline:5678",
			info.channelIds[1] == "timeline:5678:notifications",
			(cast(int) info.flags) ==
				SubscriptionOption.allowLocalOnly
		),
		(SubscriptionError err) => false
	));

	AuthenticationInfo authInfoNoScope = new AuthenticationInfo(
		AuthenticationInfo.Row(0, 0, [], "whatever", Nullable!long.init)
	);

	assert(userChannelInfo(authInfoNoScope, "user").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Access token does not cover required scopes"
	));

	assert(userChannelInfo(authInfoNoScope, "user:notification").match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Access token does not cover required scopes"
	));

	SubscriptionRequest requestPublic = {
		authInfo: null,
		channelName: "public",
		listId: "",
		tagName: "",
		allowLocalOnly: true
	};

	assert(requestToSubscriptionInfo(requestPublic).match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "public",
			info.streamName.extra.isNull,
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:public",
			(cast(int) info.flags) == (
				SubscriptionOption.needsFiltering |
				SubscriptionOption.allowLocalOnly
			)
		),
		(SubscriptionError err) => false
	));

	SubscriptionRequest requestHashtag = {
		authInfo: null,
		channelName: "hashtag",
		listId: "",
		tagName: "Meow!",
		allowLocalOnly: true
	};

	assert(requestToSubscriptionInfo(requestHashtag).match!(
		(SubscriptionInfo info) => conditionsHold(
			info.streamName.channelName == "hashtag",
			info.streamName.extra.get() == "Meow!",
			info.channelIds.length == 1,
			info.channelIds[0] == "timeline:hashtag:meow",
			(cast(int) info.flags) == (
				SubscriptionOption.needsFiltering |
				SubscriptionOption.allowLocalOnly
			)
		),
		(SubscriptionError err) => false
	));

	SubscriptionRequest requestListInvalid = {
		authInfo: null,
		channelName: "list",
		listId: "a",
		tagName: "",
		allowLocalOnly: false
	};

	assert(requestToSubscriptionInfo(requestListInvalid).match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Invalid list id"
	));

	SubscriptionRequest requestListNonexistent = {
		authInfo: null,
		channelName: "list",
		listId: "-1",
		tagName: "",
		allowLocalOnly: false
	};

	assert(requestToSubscriptionInfo(requestListNonexistent).match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Not authorized to stream this list"
	));

	SubscriptionRequest requestUser = {
		authInfo: null,
		channelName: "user",
		listId: "",
		tagName: "",
		allowLocalOnly: false
	};

	assert(requestToSubscriptionInfo(requestUser).match!(
		(SubscriptionInfo info) => false,
		(SubscriptionError err) => err.msg == "Access token does not cover required scopes"
	));
}
