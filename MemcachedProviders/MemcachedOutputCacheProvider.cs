using System;
using System.Collections.Specialized;
using System.Web.Caching;
using Enyim.Caching.Memcached;
using Enyim.Reflection;

namespace Enyim.Caching.Web
{
	public class MemcachedOutputCacheProvider : OutputCacheProvider
	{
		private IMemcachedClient client;

		public override void Initialize(string name, NameValueCollection config)
		{
			base.Initialize(name, config);

			var factory = this.GetFactoryInstance(GetAndRemove(config, "factory", false));
			System.Diagnostics.Debug.Assert(factory != null, "factory == null");

			this.client = factory.Create(name, config);

			CheckForUnknownAttributes(config);
		}

		private IMemcachedClientFactory GetFactoryInstance(string typeName)
		{
			if (String.IsNullOrEmpty(typeName))
				return new DefaultClientFactory();

			var type = Type.GetType(typeName, false);
			if (type == null)
				throw new System.Configuration.ConfigurationErrorsException("Could not load type: " + typeName);

			if (!typeof(IMemcachedClientFactory).IsAssignableFrom(type))
				throw new System.Configuration.ConfigurationErrorsException("Type '" + typeName + "' must implement IMemcachedClientFactory");

			return FastActivator.Create(type) as IMemcachedClientFactory;
		}

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

		#region [ OutputCacheProvider          ]
		public override object Add(string key, object entry, DateTime utcExpiry)
		{
			// make sure that the expiration date is flagges as utc.
			// the client converts the expiration to utc to calculate the unix time
			// and this way we can skip the utc -> ToLocal -> ToUtc chain
			utcExpiry = DateTime.SpecifyKind(utcExpiry, DateTimeKind.Utc);

			// we should only store the item if it's not in the cache
			if (this.client.Store(StoreMode.Add, key, entry, utcExpiry))
				return null;

			// if it's in the cache we should return it
			var retval = client.Get(key);

			// the item got evicted between the Add and the Get (very rare)
			// so we store it anyway, but this time with Set to make sure it gets into the cache
			if (retval == null)
				this.client.Store(StoreMode.Set, key, entry, utcExpiry);

			return retval;
		}

		public override object Get(string key)
		{
			return this.client.Get(key);
		}

		public override void Remove(string key)
		{
			this.client.Remove(key);
		}

		public override void Set(string key, object entry, DateTime utcExpiry)
		{
			utcExpiry = DateTime.SpecifyKind(utcExpiry, DateTimeKind.Utc);

			this.client.Store(StoreMode.Set, key, entry, utcExpiry);
		}
		#endregion
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
