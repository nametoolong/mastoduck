module mastoduck.auth;
import mastoduck.db;

import std.string : split;
import std.typecons : Nullable;

import ddb.postgres : PGCommand, PGType;

const class AuthenticationInfo
{
	struct Row
	{
		long accessTokenId;
		long accountId;
		string[] chosenLanguages;
		string _scopes;
		Nullable!long deviceId;
	}

	Row data;
	string[] scopes;

	alias data this;

	this(Row row)
	{
		data = row;
		scopes = data._scopes.split(" ");
	}
}

AuthenticationInfo accountFromToken(string token) @trusted
{
	enum statement =
		`SELECT oauth_access_tokens.id, users.account_id, users.chosen_languages, oauth_access_tokens.scopes, devices.device_id
		FROM oauth_access_tokens
		INNER JOIN users ON oauth_access_tokens.resource_owner_id = users.id
		LEFT OUTER JOIN devices ON oauth_access_tokens.id = devices.access_token_id
		WHERE oauth_access_tokens.token = $1 AND oauth_access_tokens.revoked_at IS NULL
		LIMIT 1`;

	auto conn = PostgresConnector.getInstance().lockConnection();
	auto cmd = new PGCommand(conn, statement);
	cmd.parameters.add(1, PGType.TEXT).value = token;

	auto result = cmd.executeQuery!(AuthenticationInfo.Row);
	scope (exit) {
		result.close();
	}

	if (result.empty())
	{
		return null;
	}

	debug
	{
		import vibe.core.log;
		logDebugV("Token %s is linked to account id %d with scopes %s", token, result.front.accountId, result.front._scopes);
	}

	return new AuthenticationInfo(result.front.base);
}

bool authorizeListAccess(AuthenticationInfo authInfo, long listId) @trusted
{
	enum statement = `SELECT id, account_id FROM lists WHERE id = $1 LIMIT 1`;

	if (!authInfo)
	{
		return false;
	}

	auto conn = PostgresConnector.getInstance().lockConnection();
	auto cmd = new PGCommand(conn, statement);
	cmd.parameters.add(1, PGType.INT8).value = listId;

	auto result = cmd.executeQuery();
	scope (exit) {
		result.close();
	}

	if (result.empty() || result.front["account_id"] != authInfo.accountId)
	{
		return false;
	}

	return true;
}
