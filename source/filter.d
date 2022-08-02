module mastoduck.filter;
import mastoduck.db; 

import std.algorithm.searching : boyerMooreFinder, find;
import std.conv : to, toChars;
import std.format : format;
import std.range.primitives : isInputRange;

import ddb.postgres : PGCommand, PGType;

enum maxMentions = 125;
enum placeholdersTemplate = generatePlaceholders!(2, maxMentions + 1);

string generatePlaceholders(int start, int n)()
{
	import std.array : appender;

	static assert(n > 0);

	auto placeholders = appender!string;

	for (size_t i = 0; i < n - 1; i++)
	{
		placeholders.put("$");
		placeholders.put(toChars(i + start));
		placeholders.put(",");
	}

	placeholders.put("$");
	placeholders.put(toChars(n + 1));

	return placeholders[];
}

string getPlaceholders(size_t n)
in (n <= maxMentions + 2)
{
	auto needle = to!string(n);
	auto remaining = placeholdersTemplate.find(boyerMooreFinder(needle));
	size_t end = placeholdersTemplate.length - remaining.length + needle.length;

	return placeholdersTemplate[0 .. end];
}

unittest
{
	static foreach(int i; 1 .. maxMentions + 1)
	{
		assert(getPlaceholders(i + 2) == generatePlaceholders!(2, i + 1)());
	}
}

bool isBlockedByAccount(Range)(long ourId, long ownerId, Range mentionedIds, size_t mentionLength) @trusted
if (isInputRange!Range)
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

	if (mentionLength > maxMentions)
	{
		// Prevents hellthreads from using a lot of memory.
		// Is there a way to handle mentions without being so strict?
		return true;
	}

	string placeholders = getPlaceholders(mentionLength + 2);
	string statement = format(statementTemplate, placeholders, placeholders);

	auto conn = PostgresConnector.getInstance().lockConnection();
	auto cmd = new PGCommand(conn, statement);

	short index = 1;
	cmd.parameters.add(index++, PGType.INT8).value = ourId;
	cmd.parameters.add(index++, PGType.INT8).value = ownerId;

	foreach (long id; mentionedIds)
	{
		if (index > mentionLength + 2)
		{
			break;
		}

		cmd.parameters.add(index++, PGType.INT8).value = id;
	}

	auto result = cmd.executeQuery();
	scope (exit) {
		result.close();
	}

	if (!result.empty())
	{
		return true;
	}

	return false;
}

bool isBlockedByDomain(long ourId, string targetDomain) @trusted
{
	enum statement =
		`SELECT 1
		FROM account_domain_blocks
		WHERE account_id = $1 AND domain = $2`;

	auto conn = PostgresConnector.getInstance().lockConnection();
	auto cmd = new PGCommand(conn, statement);
	cmd.parameters.add(1, PGType.INT8).value = ourId;
	cmd.parameters.add(2, PGType.TEXT).value = targetDomain;

	auto result = cmd.executeQuery();
	scope (exit) {
		result.close();
	}

	if (!result.empty())
	{
		return true;
	}

	return false;
}