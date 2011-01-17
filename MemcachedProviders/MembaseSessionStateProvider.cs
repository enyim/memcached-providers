using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.SessionState;
using System.Web;
using System.IO;
using System.Web.UI;
using Membase.Configuration;
using System.Net;
using Membase;

namespace Enyim.Caching.Web
{
	public class MembaseSessionStateProvider : SessionStateStoreProviderBase
	{
		private IMemcachedClient client;

		public override void Initialize(string name, System.Collections.Specialized.NameValueCollection config)
		{
			base.Initialize(name, config);
			this.client = ProviderHelper.GetClient(name, config, () => (IMemcachedClientFactory)new MembaseClientFactory());

			ProviderHelper.CheckForUnknownAttributes(config);
		}

		public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
		{
			return new SessionStateStoreData(new SessionStateItemCollection(), SessionStateUtility.GetSessionStaticObjects(context), timeout);
		}

		public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
		{
			var e = new SessionStateItem
			{
				Data = new SessionStateItemCollection(),
				Flag = SessionStateActions.InitializeItem,
				LockId = 0,
				Timeout = timeout
			};

			e.Save(this.client, id, false, false);
		}

		public override void Dispose()
		{
			client.Dispose();
		}

		public override void EndRequest(HttpContext context) { }

		public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			var e = Get(context, false, id, out locked, out lockAge, out lockId, out actions);

			return (e == null)
					? null
					: e.ToStoreData(context);
		}

		public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			var e = Get(context, true, id, out locked, out lockAge, out lockId, out actions);

			return (e == null)
					? null
					: e.ToStoreData(context);
		}

		private SessionStateItem Get(HttpContext context, bool acquireLock, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			locked = false;
			lockId = null;
			lockAge = TimeSpan.Zero;
			actions = SessionStateActions.None;

			var e = SessionStateItem.Load(this.client, id, false);
			if (e == null) return null;

			if (acquireLock)
			{
				// repeat until we can update the retrieved 
				// item (i.e. nobody changes it between the 
				// time we get it from the store and updates its attributes)
				// Save() will return false if Cas() fails
				while (true)
				{
					if (e.LockId > 0) break;

					actions = e.Flag;

					e.LockId = e.HeadCas;
					e.LockTime = DateTime.UtcNow;
					e.Flag = SessionStateActions.None;

					// try to update the item in the store
					if (e.Save(this.client, id, true, true))
					{
						locked = true;
						lockId = e.LockId;

						return e;
					}

					// it has been modifed between we loaded and tried to save it
					e = SessionStateItem.Load(this.client, id, false);
					if (e == null) return null;
				}
			}

			locked = true;
			lockAge = DateTime.UtcNow - e.LockTime;
			lockId = e.LockId;
			actions = SessionStateActions.None;

			return acquireLock ? null : e;
		}

		public override void InitializeRequest(HttpContext context)
		{
		}

		public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
		{
			if (!(lockId is ulong))
				return;

			var tmp = (ulong)lockId;
			var e = SessionStateItem.Load(this.client, id, true);

			if (e != null && e.LockId == tmp)
			{
				e.LockId = 0;
				e.LockTime = DateTime.MinValue;

				e.Save(this.client, id, true, true);
			}
		}

		public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
		{
			if (!(lockId is ulong)) return;

			var tmp = (ulong)lockId;
			var e = SessionStateItem.Load(this.client, id, true);

			if (e != null && e.LockId == tmp)
			{
				SessionStateItem.Remove(this.client, id);
			}
		}

		public override void ResetItemTimeout(HttpContext context, string id)
		{
			var e = SessionStateItem.Load(this.client, id, false);
			if (e != null)
				e.Save(this.client, id, false, true);
		}

		public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
		{
			SessionStateItem e = null;
			bool existing = false;

			if (!newItem)
			{
				if (!(lockId is ulong))
					return;

				var tmp = (ulong)lockId;
				e = SessionStateItem.Load(this.client, id, true);
				existing = e != null;

				// if we're expecting an existing item, but
				// it's not in the cache
				// or it's not locked
				// or it's locked by someone else, then quit
				if (!newItem
					&& (!existing
						|| e.LockId == 0
						|| e.LockId != tmp))
					return;
			}

			if (!existing) e = new SessionStateItem();

			// set the new data and reset the locks
			e.Timeout = item.Timeout;
			e.Data = (SessionStateItemCollection)item.Items;
			e.Flag = SessionStateActions.None;
			e.LockId = 0;
			e.LockTime = DateTime.MinValue;

			e.Save(this.client, id, false, existing && !newItem);
		}

		public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
		{
			return false;
		}

		#region [ SessionStateItem             ]

		class SessionStateItem
		{
			private static readonly string HeaderPrefix = (System.Web.Hosting.HostingEnvironment.SiteName ?? String.Empty).Replace(" ", "-") + "+" + System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath + "info-";
			private static readonly string DataPrefix = (System.Web.Hosting.HostingEnvironment.SiteName ?? String.Empty).Replace(" ", "-") + "+" + System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath + "data-";

			public SessionStateItemCollection Data;
			public SessionStateActions Flag;
			public ulong LockId;
			public DateTime LockTime;

			// this is in minutes
			public int Timeout;

			public ulong HeadCas;
			public ulong DataCas;

			private void SaveHeader(MemoryStream ms)
			{
				var p = new Pair(
									(byte)1,
									new Triplet(
													(byte)this.Flag,
													this.Timeout,
													new Pair(
																this.LockId,
																this.LockTime.ToBinary()
															)
												)
								);

				new ObjectStateFormatter().Serialize(ms, p);
			}

			public bool Save(IMemcachedClient client, string id, bool metaOnly, bool useCas)
			{
				using (var ms = new MemoryStream())
				{
					this.SaveHeader(ms);
					bool retval;
					var ts = TimeSpan.FromMinutes(this.Timeout);

					retval = useCas
								? client.Cas(Memcached.StoreMode.Set, HeaderPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int)ms.Length), ts, this.HeadCas).Result
								: client.Store(Memcached.StoreMode.Set, HeaderPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int)ms.Length), ts);

					if (!metaOnly)
					{
						ms.Position = 0;

						using (var bw = new BinaryWriter(ms))
						{
							this.Data.Serialize(bw);
							retval = useCas
										? client.Cas(Memcached.StoreMode.Set, DataPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int)ms.Length), ts, this.DataCas).Result
										: client.Store(Memcached.StoreMode.Set, DataPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int)ms.Length), ts);
						}
					}

					return retval;
				}
			}

			private static SessionStateItem LoadItem(MemoryStream ms)
			{
				var graph = new ObjectStateFormatter().Deserialize(ms) as Pair;
				if (graph == null) return null;

				if (((byte)graph.First) != 1) return null;

				var t = (Triplet)graph.Second;
				var retval = new SessionStateItem();

				retval.Flag = (SessionStateActions)((byte)t.First);
				retval.Timeout = (int)t.Second;

				var lockInfo = (Pair)t.Third;

				retval.LockId = (ulong)lockInfo.First;
				retval.LockTime = DateTime.FromBinary((long)lockInfo.Second);

				return retval;
			}

			public static SessionStateItem Load(IMemcachedClient client, string id, bool metaOnly)
			{
				var header = client.GetWithCas<byte[]>(HeaderPrefix + id);
				if (header.Result == null) return null;

				SessionStateItem entry;

				using (var ms = new MemoryStream(header.Result))
					entry = SessionStateItem.LoadItem(ms);

				if (entry != null) entry.HeadCas = header.Cas;
				if (metaOnly) return entry;

				var data = client.GetWithCas<byte[]>(DataPrefix + id);
				if (data.Result == null) return null;

				using (var ms = new MemoryStream(data.Result))
				using (var br = new BinaryReader(ms))
					entry.Data = SessionStateItemCollection.Deserialize(br);

				entry.DataCas = data.Cas;

				return entry;
			}

			public SessionStateStoreData ToStoreData(HttpContext context)
			{
				return new SessionStateStoreData(this.Data, SessionStateUtility.GetSessionStaticObjects(context), this.Timeout);
			}

			public static void Remove(IMemcachedClient client, string id)
			{
				client.Remove(DataPrefix + id);
				client.Remove(HeaderPrefix + id);
			}
		}

		#endregion
	}
}
