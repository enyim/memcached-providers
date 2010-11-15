using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Specialized;
using Enyim.Reflection;

namespace Enyim.Caching.Web
{
	internal static class ProviderHelper
	{
		public static string GetAndRemove(NameValueCollection nvc, string name, bool required)
		{
			var tmp = nvc[name];
			if (tmp == null)
			{
				if (required) throw new System.Configuration.ConfigurationErrorsException("Missing parameter: " + name);
			}
			else
				nvc.Remove(name);

			return tmp;
		}

		public static void CheckForUnknownAttributes(NameValueCollection nvc)
		{
			if (nvc.Count > 0)
				throw new System.Configuration.ConfigurationErrorsException("Unknown parameter: " + nvc.Keys[0]);
		}

		public static IMemcachedClient GetClient(string name, NameValueCollection config, Func<IMemcachedClientFactory> createDefault)
		{
			var factory = GetFactoryInstance(ProviderHelper.GetAndRemove(config, "factory", false), createDefault);
			System.Diagnostics.Debug.Assert(factory != null, "factory == null");

			return factory.Create(name, config);
		}

		private static IMemcachedClientFactory GetFactoryInstance(string typeName, Func<IMemcachedClientFactory> createDefault)
		{
			if (String.IsNullOrEmpty(typeName))
				return createDefault();

			var type = Type.GetType(typeName, false);
			if (type == null)
				throw new System.Configuration.ConfigurationErrorsException("Could not load type: " + typeName);

			if (!typeof(IMemcachedClientFactory).IsAssignableFrom(type))
				throw new System.Configuration.ConfigurationErrorsException("Type '" + typeName + "' must implement IMemcachedClientFactory");

			return FastActivator.Create(type) as IMemcachedClientFactory;
		}

	}
}
