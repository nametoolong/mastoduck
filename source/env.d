module mastoduck.env; 

enum EnvType
{
	unspecified,
	development,
	production,
	test
}

class Env
{
	static import dotenv;

	static void loadEnv()
	{
		import std.process : environment;
		envType = environment.get("RAILS_ENV");
		dotenv.Env.load(envType == EnvType.production ? ".env.production" : ".env");
		alwaysRequireAuth =
			dotenv.Env["LIMITED_FEDERATION_MODE"] == "true" ||
			dotenv.Env["WHITELIST_MODE"] == "true" ||
			dotenv.Env["AUTHORIZED_FETCH"] == "true";
	}

	@property
	static bool alwaysRequireAuth()
	{
		return _alwaysRequireAuth;
	}

	@property
	static EnvType envType()
	{
		return _envType;
	}

	static string opIndex(string name)
	{
		return dotenv.Env[name];
	}

protected:
	@property
	static bool alwaysRequireAuth(bool val)
	{
		return _alwaysRequireAuth = val;
	}

	@property
	static EnvType envType(EnvType envEnum)
	{
		return _envType = envEnum;
	}

	@property
	static string envType(string envStr)
	{
		switch (envStr)
		{
		case "development":
			envType = EnvType.development;
			break;
		case "production":
			envType = EnvType.production;
			break;
		case "test":
			envType = EnvType.test;
			break;
		default:
			envType = EnvType.unspecified;
		}
		return envStr;
	}

private:
	static bool _alwaysRequireAuth;
	static EnvType _envType;
}
