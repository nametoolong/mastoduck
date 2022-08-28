module mastoduck.util;

import mastoduck.channel : StreamName;

import std.exception : enforce;
import std.format : format, formattedWrite;
import std.range.primitives : isOutputRange;

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

string toJsonString(Bson bson)
{
	import std.array : appender;

	auto ret = appender!string();
	writeBsonToJsonString(ret, bson);
	return ret.data;
}

private:

// The following functions are taken from vibe.data.json and modified
// Copyright: © 2012-2015 Sönke Ludwig

void writeBsonToJsonString(R)(ref R dst, in Bson bson) if (isOutputRange!(R, char))
{
	switch (bson.type)
	{
	case Bson.Type.null_:
		dst.put("null");
		break;
	case Bson.Type.bool_:
		dst.put(bson.get!bool ? "true" : "false");
		break;
	case Bson.Type.int_:
	case Bson.Type.long_:
		auto i = bson.tryReadInteger(throw new Exception("Impossible integer type in writeBsonToJsonString"));
		formattedWrite(dst, "%d", i);
		break;
	case Bson.Type.double_:
		auto d = bson.get!double;
		if (d != d)
		{
			dst.put("null"); // JSON has no NaN value so set null
		}
		else
		{
			formattedWrite(dst, "%.16g", d);
		}
		break;
	case Bson.Type.string:
		dst.put('\"');
		jsonEscape(dst, bson.get!string);
		dst.put('\"');
		break;
	case Bson.Type.date:
		dst.put('\"');
		jsonEscape(dst, bson.get!BsonDate.toString());
		dst.put('\"');
		break;
	case Bson.Type.array:
		dst.put('[');
		bool first = true;
		foreach (ref const Bson e; bson.byValue) {
			if (!first)
			{
				dst.put(",");
			}
			first = false;
			writeBsonToJsonString!(R)(dst, e);
		}
		dst.put(']');
		break;
	case Bson.Type.object:
		dst.put('{');
		bool first = true;
		foreach (string k, ref const Bson e; bson.byKeyValue) {
			if (!first)
			{
				dst.put(',');
			}
			first = false;
			dst.put('\"');
			jsonEscape(dst, k);
			dst.put(`":`);
			writeBsonToJsonString!(R)(dst, e);
		}
		dst.put('}');
		break;
	default:
		throw new Exception(format("BSON type %d is not supported", bson.type));
	}
}

void jsonEscape(R)(ref R dst, const(char)[] s)
{
	size_t startPos = 0;

	void putInterval(size_t curPos)
	{
		if (curPos > startPos)
			dst.put(s[startPos..curPos]);
		startPos = curPos + 1;
	}

	for (size_t pos = 0; pos < s.length; pos++) {
		immutable(char) ch = s[pos];

		switch (ch) {
		default:
			if (ch < 0x20)
			{
				putInterval(pos);
				dst.formattedWrite("\\u%04X", ch);
			}
			break;
		case '\\': putInterval(pos); dst.put("\\\\"); break;
		case '\r': putInterval(pos); dst.put("\\r"); break;
		case '\n': putInterval(pos); dst.put("\\n"); break;
		case '\t': putInterval(pos); dst.put("\\t"); break;
		case '\"': putInterval(pos); dst.put("\\\""); break;
		}
	}
	// last interval
	putInterval(s.length);
}

unittest
{
	import std.algorithm.searching : startsWith;
	import std.exception;

	assertThrown(Json().getValue!bool("key", false));
	assertThrown(Json([0, 1, 2]).getValue!bool("key", false));

	assert(Json(["key": Json("value")]).getValue!int("", 345) == 345);
	assert(Json(["key": Json("value")]).getValue!int("key", 345) == 345);
	assert(Json(["key": Json("value")]).getValue!string("key", "345") == "value");

	assert(StreamName("abc").toJsonArray().toString() == `["abc"]`);
	assert(StreamName("abc", "def").toJsonArray().toString() == `["abc","def"]`);

	assert(Bson("").tryReadBool(true) == true);
	assert(Bson("").tryReadBool(false) == false);
	assert(Bson(false).tryReadBool(true) == false);
	assert(Bson("").tryReadInteger(8765L) == 8765L);
	assert(Bson(false).tryReadInteger(80L) == 80L);
	assert(Bson(12345).tryReadInteger(0L) == 12345L);
	assert(Bson(12345678912L).tryReadInteger(0L) == 12345678912L);

	assert(Bson(null).toJsonString() == `null`);
	assert(Bson(true).toJsonString() == `true`);
	assert(Bson(false).toJsonString() == `false`);
	assert(Bson(0).toJsonString() == `0`);
	assert(Bson(-50).toJsonString() == `-50`);
	assert(Bson(10000).toJsonString() == `10000`);
	assert(Bson(12345678912L).toJsonString() == `12345678912`);
	assert(Bson(4.0).toJsonString() == `4`);
	assert(Bson(3.14159265358979323846).toJsonString().length >= 17 &&
		Bson(3.14159265358979323846).toJsonString().length <= 19);
	assert(Bson(3.14159265358979323846).toJsonString().startsWith("3.141592653589793"));
	assert(Bson("").toJsonString() == `""`);
	assert(Bson("测试").toJsonString() == `"测试"`);
	assert(Bson("😋test").toJsonString() == `"😋test"`);
	assert(Bson(cast(Bson[])[]).toJsonString() == `[]`);
	assert(Bson([Bson(cast(Bson[])[])]).toJsonString() == `[[]]`);
	assert(Bson([Bson([Bson(null)])]).toJsonString() == `[[null]]`);
	assert(Bson([Bson(1), Bson("2"), Bson([Bson(3), Bson("4")])]).toJsonString() == `[1,"2",[3,"4"]]`);

	Bson[string] emptyObj;
	assert(Bson(emptyObj).toJsonString() == `{}`);
	assert(Bson(["key": Bson(emptyObj)]).toJsonString() == `{"key":{}}`);
	assert(Bson(["key": Bson([Bson(1), Bson(["1": Bson("2"), "3": Bson(4)]), Bson(null)])]).toJsonString() == `{"key":[1,{"1":"2","3":4},null]}`);
}