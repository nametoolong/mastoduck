module mastoduck.web;

import mastoduck.auth;
import mastoduck.channel;
import mastoduck.env;
import mastoduck.stream;
import mastoduck.util;

import std.algorithm.comparison : max, min;
import std.format : format;
import std.string;
import std.sumtype : match;

import vibe.vibe;

private:

immutable(bool[string]) falseValues;
immutable(string[string]) endpoints;
immutable(string[string]) onlyMediaChannels;

shared static this()
{
	falseValues = [
		"0": false,
		"f": false,
		"F": false,
		"false": false,
		"FALSE": false,
		"off": false,
		"OFF": false
	];

	endpoints = [
		"/api/v1/streaming/user": "user",
		"/api/v1/streaming/user/notification": "user:notification",
		"/api/v1/streaming/public": "public",
		"/api/v1/streaming/public/local": "public:local",
		"/api/v1/streaming/public/remote": "public:remote",
		"/api/v1/streaming/hashtag": "hashtag",
		"/api/v1/streaming/hashtag/local": "hashtag:local",
		"/api/v1/streaming/direct": "direct",
		"/api/v1/streaming/list": "list"
	];

	onlyMediaChannels = [
		"public": "public:media",
		"public:local": "public:local:media",
		"public:remote": "public:remote:media"
	];
}

void setCrossOriginHeaders(HTTPServerRequest req, HTTPServerResponse res)
{
	res.headers["Access-Control-Allow-Origin"] = "*";
	res.headers["Access-Control-Allow-Headers"] = "Authorization, Accept, Cache-Control";
	res.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS";
}

void authenticateClient(HTTPServerRequest req, HTTPServerResponse res)
{
	if (req.method == HTTPMethod.OPTIONS)
	{
		return;
	}

	string authorization = req.headers.get("Authorization", "");
	string wsAccessToken = req.headers.get("Sec-Websocket-Protocol", "");
	string queryAccessToken = req.query.get("access_token", "");
	string token =
		authorization.length != 0 ?
			authorization.chompPrefix("Bearer ") :
			(wsAccessToken.length != 0 ?
				wsAccessToken :
				queryAccessToken);

	if (token.length != 0)
	{
		AuthenticationInfo authenticationInfo = accountFromToken(token);

		if (authenticationInfo)
		{
			req.context["authenticationInfo"] = authenticationInfo;
		}
		else
		{
			res.writeJsonBody(["error": "Invalid access token"], 401);
		}
	}
	else if (Env.alwaysRequireAuth)
	{
		res.writeJsonBody(["error": "Missing access token"], 401);
	}
}

bool isParamTruthy(HTTPServerRequest req, string name)
{
	string param = req.query.get(name, "");
	return param.length != 0 && falseValues.get(param, true);
}

string channelNameFromPath(HTTPServerRequest req)
{
	string path = req.requestPath.toString();
	string channelName = endpoints.get(path, "");
	bool onlyMedia = req.isParamTruthy("only_media");

	return onlyMedia ? onlyMediaChannels.get(channelName, channelName) : channelName;
}

bool subscribeByURL(alias onSuccess, alias onError)
	(ConnectionState connState, HTTPServerRequest req, string channelName)
{
	if (channelName.length == 0)
	{
		return false;
	}

	bool result;

	SubscriptionRequest request = {
		authInfo: connState.getAuthInfo(),
		channelName: channelName,
		listId: req.query.get("list", ""),
		tagName: req.query.get("tag", ""),
		allowLocalOnly: req.isParamTruthy("allow_local_only")
	};

	requestToSubscriptionInfo(request).match!(
		(SubscriptionInfo info) {
			onSuccess(info);
			result = true;
		},
		(SubscriptionError err) {
			onError(err);
			result = false;
		}
	);

	return result;
}

bool subscribeByJSON(alias onSuccess, alias onError)
	(ConnectionState connState, Json json)
{
	bool result;

	SubscriptionRequest request = {
		authInfo: connState.getAuthInfo(),
		channelName: json.getValue!string("stream", ""),
		listId: json.getValue!string("list", ""),
		tagName: json.getValue!string("tag", ""),
		allowLocalOnly: json.getValue!bool("allow_local_only", false)
	};

	requestToSubscriptionInfo(request).match!(
		(SubscriptionInfo info) {
			onSuccess(info);
			result = true;
		},
		(SubscriptionError err) {
			onError(err);
			result = false;
		}
	);

	return result;
}

void checkDeliveryTime(PushMessage message)
{
	if (message.queuedAt == 0)
	{
		return;
	}

	long now = Clock.currTime.toUnixTime!long;
	long queuedAt = message.queuedAt / 1000;

	if (now - queuedAt > 5)
	{
		debug
		{
			logWarn("A message (event %s) took more than 5 seconds to send! payload: %s",
				message.event, message.payload);
		}
		else
		{
			logWarn("A message (event %s) took more than 5 seconds to send!", message.event);
		}
	}
}

void handleHTTPConnection(HTTPServerRequest req, HTTPServerResponse res)
{
	string channelName = channelNameFromPath(req);

	if (channelName.length == 0)
	{
		res.writeJsonBody(["error": "Not found"], 404);
		return;
	}

	ConnectionState connState = ConnectionState.create(req);

	if (!connState)
	{
		logError("Unable to create a ConnectionState");
		res.writeJsonBody(["error": "Internal error"], 500);
		return;
	}

	scope (exit)
	{
		connState.close();
	}

	auto subscribeToChannels =
		(SubscriptionInfo info) => connState.subscribeToChannels(info);
	auto sendErrorMessage =
		(SubscriptionError err) => res.writeJsonBody(["error": err.msg], 403);
	alias subscribe = subscribeByURL!(subscribeToChannels, sendErrorMessage);

	bool success = subscribe(connState, req, channelName);

	if (!success)
	{
		return;
	}

	res.headers["Content-Type"] = "text/event-stream";
	res.headers["Cache-Control"] = "no-store";
	res.headers["Transfer-Encoding"] = "chunked";

	res.bodyWriter.write(":)\n");
	res.bodyWriter.flush();

	enum httpHeartbeatInterval = 15.seconds;
	Timer heartbeatTimer = setTimer(
		httpHeartbeatInterval, &connState.sendHeartbeat, true);

	scope (exit)
	{
		logDebug("Ending stream for %s", req.peer);
		heartbeatTimer.stop();
	}

	while (res.connected)
	{
		try
		{
			PushMessage message = connState.getMessage();

			if (message.type == MessageType.event)
			{
				string payload = message.payload.to!string();

				logDebug("Pushing %s message to streams %(%s, %)",
					message.event, message.streamNames);

				foreach (name; message.streamNames)
				{
					res.bodyWriter.write(
						format("event: %r\ndata: %r\n\n",
							message.event, payload));
				}

				res.bodyWriter.flush();
				checkDeliveryTime(message);
			}
			else if (message.type == MessageType.heartbeat)
			{
				res.bodyWriter.write(":thump\n");
				res.bodyWriter.flush();
			}
			else if (message.type == MessageType.error)
			{
				res.bodyWriter.write(
					format("error: %r\n", message.event));
				res.bodyWriter.flush();
				return;
			}
		}
		catch (Exception e)
		{
			debug
			{
				logException(e, "HTTP streaming handler returning");
			}

			return;
		}
	}
}

void handleWSConnection(scope WebSocket sock)
{
	// Bravely cast away const
	// because query.get is non-const
	HTTPServerRequest req = cast(HTTPServerRequest)(sock.request);

	ConnectionState connState = ConnectionState.create(req);

	if (!connState)
	{
		logError("Unable to create a ConnectionState");
		sock.send(serializeToJsonString(["error": "Internal error"]));
		return;
	}

	scope (exit)
	{
		connState.close();
	}

	auto subscribeToChannels =
		(SubscriptionInfo info) => connState.subscribeToChannels(info);
	auto unsubscribeFromChannels =
		(SubscriptionInfo info) => connState.unsubscribeFromChannels(info);
	auto sendErrorMessage =
		(SubscriptionError err) => connState.sendError(err.msg);
	alias subscribeQuery = subscribeByURL!(subscribeToChannels, sendErrorMessage);
	alias subscribeWS = subscribeByJSON!(subscribeToChannels, sendErrorMessage);
	alias unsubscribeWS = subscribeByJSON!(unsubscribeFromChannels, sendErrorMessage);

	subscribeQuery(connState, req, req.query.get("stream", ""));

	auto reader = runTask(() nothrow {
		import core.time;

		scope (exit)
		{
			connState.close();
		}

		int recentRequests = 0;
		MonoTime lastSuccess = MonoTime.currTime;

		while (true)
		{
			try
			{
				bool readable = sock.waitForData();

				if (!readable)
				{
					return;
				}

				string msg = sock.receiveText();

				long msecsSinceLastSuccess = 
					(MonoTime.currTime - lastSuccess).total!"msecs";
				bool shouldUpdateThrottle = msecsSinceLastSuccess > 1000;

				if (shouldUpdateThrottle)
				{
					long count = min(
						msecsSinceLastSuccess / 500,
						recentRequests
					);
					recentRequests -= max(count, 0);
				}

				if (recentRequests >= 3)
				{
					logInfo("Throttling JSON request from %s", req.peer);
					connState.sendError("Throttled");
					continue;
				}

				Json json = parseJsonString(msg);
				string type = json["type"].get!string();

				recentRequests++;

				if (shouldUpdateThrottle)
				{
					lastSuccess = MonoTime.currTime;
				}

				if (type == "subscribe")
				{
					subscribeWS(connState, json);
				}
				else if (type == "unsubscribe")
				{
					unsubscribeWS(connState, json);
				}
			}
			catch (JSONException e)
			{
				logDebug("Error parsing JSON message from %s: %s", req.peer, e.msg);
				continue;
			}
			catch (InterruptException e)
			{
				return;
			}
			catch (Exception e)
			{
				logDebug("Error handling request from %s: %s", req.peer, e.msg);
				return;
			}
		}
	});

	scope (exit)
	{
		logDebug("Stopping streaming for %s", req.peer);
		connState.close();
		reader.interrupt();
		reader.join();
		logDebug("Ended stream for %s", req.peer);
	}

	while (sock.connected)
	{
		try
		{
			PushMessage message = connState.getMessage();
	
			if (message.type == MessageType.event)
			{
				Json event = Json(message.event);
				Json payload = message.payload;

				if (payload.type() == Json.Type.object)
				{
					payload = Json(serializeToJsonString(payload));
				}

				logDebug("Pushing %s message to streams %(%s, %)",
					message.event, message.streamNames);

				foreach (name; message.streamNames)
				{
					string data = serializeToJsonString([
						"stream": name.toJsonArray(),
						"event": event,
						"payload": payload
					]);

					sock.send(data);
				}

				checkDeliveryTime(message);
			}
			else if (message.type == MessageType.error)
			{
				string data = serializeToJsonString([
					"error": message.event
				]);

				sock.send(data);
			}
		}
		catch (Exception e)
		{
			debug
			{
				logException(e, "WebSockets handler returning");
			}

			return;
		}
	}
}

public:

void writeJsonErrorInfo(HTTPServerRequest req, HTTPServerResponse res, HTTPServerErrorInfo err)
{
	if (err.exception)
	{
		debug
		{
			import std.format : format;
			logException(err.exception, format("%s: %s", req.peer, err.message));
		}
		else
		{
			logError("%s: %s (%s, at %s:%d)",
				req.peer, err.message, err.exception.msg,
				err.exception.file, err.exception.line);
		}
	}
	else
	{
		logError("%s: %s", req.peer, err.message);
	}

	res.writeJsonBody(["error": err.message], err.code);
}

auto buildRequestHandler()
{
	auto router = new URLRouter;

	router.any("*", &setCrossOriginHeaders);

	router.get("/api/v1/streaming/health", (req, res) {
		res.writeBody("OK", "text/plain");
	});

	debug
	{
		router.get("/debug/force_gc", (req, res) {
			import core.memory;
			GC.collect();
			res.writeBody("Done", "text/plain");
		});
	}

	router.any("*", &authenticateClient);

	router.get("/api/v1/streaming", handleWebSockets(&handleWSConnection));
	router.get("/api/v1/streaming/", handleWebSockets(&handleWSConnection));
	router.get("/api/v1/streaming/*", &handleHTTPConnection);

	return router;
}