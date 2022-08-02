module mastoduck.util;

import mastoduck.channel : StreamName;

import std.exception : enforce;

import vibe.data.bson;
import vibe.data.json;

@safe:

T getValue(T)(Json json, string key, lazy T defaultValue)
{
	enforce(json.type() == Json.Type.object, "Attempting to getValue on non-object");

	Json val = json[key];

	switch (val.type())
	{
	case Json.typeId!(T):
		return val.get!T();
	default:
		return defaultValue();
	}
}

Json toJsonArray(StreamName stream)
{
	Json obj = Json.emptyArray;

	obj.appendArrayElement(Json(stream.channelName));

	if (!stream.extra.isNull)
	{
		obj.appendArrayElement(Json(stream.extra.get()));
	}

	return obj;
}

bool tryReadBool(Bson bson, lazy bool defaultValue)
{
	switch (bson.type())
	{
	case Bson.Type.bool_:
		return bson.get!bool();
	default:
		return defaultValue();
	}
}

long tryReadInteger(Bson bson, lazy long defaultValue)
{
	switch (bson.type())
	{
	case Bson.Type.int_:
		return bson.get!int();
	case Bson.Type.long_:
		return bson.get!long();
	default:
		return defaultValue();
	}
}
