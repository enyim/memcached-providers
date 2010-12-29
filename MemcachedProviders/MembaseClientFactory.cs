using System;
using System.Collections.Specialized;
using System.Configuration;
using Membase.Configuration;

namespace Enyim.Caching.Web
{
	public sealed class MembaseClientFactory : IMemcachedClientFactory
	{
		IMemcachedClient IMemcachedClientFactory.Create(string name, NameValueCollection config)
		{
			var sectionName = ProviderHelper.GetAndRemove(config, "section", false);
			if (String.IsNullOrEmpty(sectionName))
				return new Membase.MembaseClient();

			var section = ConfigurationManager.GetSection(sectionName) as IMembaseClientConfiguration;
			if (section == null) throw new InvalidOperationException("Invalid config section: " + section);

			return new Membase.MembaseClient(section);
		}
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
