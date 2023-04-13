using System;
using System.Linq;
using System.Collections.Concurrent;
using ClearScada.Client;
using ClearScada.Client.Advanced;

/// <summary>
/// Create one of these to feed data out. 
/// The class uses a variable NextKey to hold next point change id, used to identify incoming data
/// It uses a concurrent dictionary of PointInfo objects to retain information about monitored points
/// There are concurrent queues of point data changes and configuration changes.
/// Concurrent queues are used in order to avoid any async ClearSCADA API calls, as the API is not
/// thread-safe.
/// Four function/action callbacks need to be passed in to handle data change, shutdown and new point
/// filtering. 
/// </summary>
namespace FeederEngine
{
	// Replaces TagUpdate in ClearScada.Client, which is read-only
	public class CustomTagUpdate
	{
		public int Id;
		public string Status;
		public object Value;
		public long Quality;
		public DateTimeOffset Timestamp;
	}

	public static class Feeder
	{
		static IServer AdvConnection;

		// Dict of PointInfo
		private static ConcurrentDictionary<int, PointInfo> PointDictionary = new ConcurrentDictionary<int, PointInfo>();
		private static int NextKey = 1000;
		private static ServerState CurrentServerState = ServerState.None;
		// Dict of Updates
		private static ConcurrentQueue<CustomTagUpdate> UpdateQueue = new ConcurrentQueue<CustomTagUpdate>();
		// Dict of Config changes
		private static ConcurrentQueue<ObjectUpdateEventArgs> ConfigQueue = new ConcurrentQueue<ObjectUpdateEventArgs>();
		// Only one of these is used for any point
		public static int UpdateIntervalHisSec; // Used for all historic points added to the engine
		public static int UpdateIntervalCurSec; // Used for all non-Historic points added to the engine
		public static int MaxDataAgeDays; // Limits historic retrieval on start-up to protect the server

		private static bool UpdateConfigurationOnStart;
		private static bool UpdateDataOnStart;

		private static Action<string, int, string, double, DateTimeOffset, long> ProcessNewData;
		private static Action<string, int, string> ProcessNewConfig;
		private static Action Shutdown;
		private static Func<ObjectDetails, bool> FilterNewPoint;

		/// <summary>
		/// Acts as the initialiser/constructor
		/// </summary>
		/// <param name="_AdvConnection">Database connection</param>
		/// <param name="_UpdateConfigurationOnStart">If true, send a config message for each point added.</param>
		/// <param name="_UpdateDataOnStart">If true, send a data message for each point added.</param>
		/// <param name="_UpdateIntervalHisSec">See strong warnings in sample Program.cs about setting this too low.</param>
		/// <param name="_UpdateIntervalCurSec">See strong warnings in sample Program.cs about setting this too low.</param>
		/// <param name="_ProcessNewData">Callback</param>
		/// <param name="_ProcessNewConfig">Callback</param>
		/// <param name="_Shutdown">Callback</param>
		/// <param name="_FilterNewPoint">Callback</param>
		/// <returns></returns>
		public static bool Connect(IServer _AdvConnection,
									bool _UpdateConfigurationOnStart,
									bool _UpdateDataOnStart,
									int _UpdateIntervalHisSec,
									int _UpdateIntervalCurSec,
									int _MaxDataAgeDays,
									Action<string, int, string, double, DateTimeOffset, long> _ProcessNewData,
									Action<string, int, string> _ProcessNewConfig,
									Action _Shutdown,
									Func<ObjectDetails, bool> _FilterNewPoint)
		{
			AdvConnection = _AdvConnection;
			UpdateIntervalHisSec = _UpdateIntervalHisSec;
			UpdateIntervalCurSec = _UpdateIntervalCurSec;
			UpdateConfigurationOnStart = _UpdateConfigurationOnStart;
			UpdateDataOnStart = _UpdateDataOnStart;
			MaxDataAgeDays = _MaxDataAgeDays;

			// Check server is valid
			CurrentServerState = AdvConnection.GetServerState().State;
			if (!(CurrentServerState == ServerState.Main) && !(CurrentServerState == ServerState.Standby))
			{
				return false;
			}
			// Set action callbacks in application code
			ProcessNewData = _ProcessNewData;
			ProcessNewConfig = _ProcessNewConfig;
			Shutdown = _Shutdown;
			FilterNewPoint = _FilterNewPoint;

			// Set event callback
			AdvConnection.TagsUpdated += TagUpdateEvent;

			// Set config change callback
			var WaitFor = new string[] { "CDBPoint", "CAccumulatorBase" };
			AdvConnection.ObjectUpdated += ObjUpdateEvent;
			AdvConnection.AdviseObjectUpdates(false, WaitFor);

			// Disconnect Callbacks
			AdvConnection.StateChanged += DBStateChangeEvent;
			AdvConnection.AdviseStateChange();

			return true;
		}

		/// <summary>
		/// Called when server state changes, and the creator of this object must drop it and create a new one
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="stateChange"></param>
		static void DBStateChangeEvent( object sender, StateChangeEventArgs stateChange)
		{
			if (stateChange.StateDetails.State != CurrentServerState)
			{
				EngineShutdown();
			}
		}
		/// <summary>
		/// Called if there is a comms error, deletes dict and calls back
		/// Consider here recording the last retrieve dates of each point
		/// </summary>
		static void EngineShutdown()
		{
			// Remove all items and advise caller that database has shut down
			foreach (var Key in PointDictionary.Keys)
			{
				PointDictionary.TryRemove(Key, out PointInfo point);
			}
			Shutdown();
		}

		/// <summary>
		/// Useful info
		/// </summary>
		/// <returns></returns>
		public static int SubscriptionCount()
		{
			return PointDictionary.Count();
		}
		public static int ProcessQueueCount()
		{
			return UpdateQueue.Count();
		}
		public static int ConfigQueueCount()
		{
			return ConfigQueue.Count();
		}

		/// <summary>
		/// Add a new point/accumulator to be monitored
		/// </summary>
		/// <param name="FullName">Object name string - FullName</param>
		/// <param name="LastChange">If historic object, then data retrieval starts at this time</param>
		/// <returns>Success or Failure</returns>
		public static bool AddSubscription( string FullName, DateTimeOffset LastChange)
		{
			// Protect against long historic queries
			DateTimeOffset MaxAge = DateTimeOffset.UtcNow.AddDays(-MaxDataAgeDays);
			DateTimeOffset LastChangeAdjusted = LastChange > MaxAge ? LastChange : MaxAge;
			// Create the PointInfo and add to our dictionary
			bool s = PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, FullName, UpdateIntervalHisSec, UpdateIntervalCurSec, LastChangeAdjusted, AdvConnection, ProcessNewData));
			if (s)
			{
				if (UpdateConfigurationOnStart)
				{
					ProcessNewConfig("Added", PointDictionary[NextKey].PointId, FullName);
				}
				if (UpdateDataOnStart)
				{
					// Queue a data update request
					var Update = new CustomTagUpdate
					{
						Id = NextKey,
						Status = "Init" // Special status confers this is an initialisation/catch-up update
					};
					UpdateQueue.Enqueue(Update);
					//Console.WriteLine("Tag Update: " + Update.Id.ToString() + ", " + Update.Value.ToString());
				}
				NextKey++;
			}
			return s;
		}

		/// <summary>
		/// Caller should make regular calls to this to process items off the queues
		/// </summary>
		/// <returns></returns>
		public static long ProcessUpdates()
		{
			long UpdateCount = 0;

			// If queued updates
			//if (UpdateQueue.Count > 0)
				//Console.WriteLine("Queued Updates: " + UpdateQueue.Count.ToString());
			DateTimeOffset ProcessStartTime = DateTimeOffset.UtcNow;
			while (UpdateQueue.Count > 0)
			{
				if (UpdateQueue.TryDequeue(out CustomTagUpdate update))
				{
					// Find point
					if (PointDictionary.TryGetValue(update.Id, out PointInfo info))
					{
						UpdateCount += info.ReadHistoric(AdvConnection, update);
					}
					//Console.WriteLine("Queue now " + UpdateQueue.Count.ToString()); 
				}
				if ((DateTimeOffset.UtcNow - ProcessStartTime).TotalSeconds > 1)
				{
					//Console.WriteLine("End after 1sec");
					break;
				}
			}

			// If queued config
			//if (ConfigQueue.Count > 0)
			//	Console.WriteLine("Queued Config: " + ConfigQueue.Count.ToString());
			DateTimeOffset ConfigStartTime = DateTimeOffset.UtcNow;
			while (ConfigQueue.Count > 0)
			{
				UpdateCount++; // Include config changes in this count
				if (ConfigQueue.TryDequeue(out ObjectUpdateEventArgs objupdate))
				{
					if (objupdate.UpdateType == ObjectUpdateType.Created)
					{
						// This is run for all new points - use filter to include user's desired points, otherwise everything new would be added
						var newpoint = AdvConnection.LookupObject(new ObjectId(objupdate.ObjectId));
						if (newpoint != null && FilterNewPoint(newpoint))
						{
							PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, newpoint.FullName, UpdateIntervalHisSec, UpdateIntervalCurSec, DateTimeOffset.MinValue, AdvConnection, ProcessNewData));
							NextKey++;
							Console.WriteLine("Added new point: " + newpoint.FullName);
							ProcessNewConfig("Created", objupdate.ObjectId, newpoint.FullName);
						}
					}
					else
					{
						// Find point - will need to look at the full list
						foreach (var Info in PointDictionary)
						{
							if (Info.Value.PointId == objupdate.ObjectId)
							{
								// We have the point which changed
								switch (objupdate.UpdateType)
								{
									case ObjectUpdateType.Modified:
										// Could have historic enabled/removed, so we need to unsub and resub
										if (!PointDictionary.TryRemove(Info.Key, out PointInfo modified))
										{
											Console.WriteLine("Error removing modified point: " + objupdate.ObjectId);
										}
										Info.Value.Dispose();
										var modpoint = AdvConnection.LookupObject(new ObjectId(objupdate.ObjectId));
										if (modpoint != null)
										{
											PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, modpoint.FullName, UpdateIntervalHisSec, UpdateIntervalCurSec, modified.LastChange, AdvConnection, ProcessNewData));
											NextKey++;
											Console.WriteLine("Replaced point: " + modpoint.FullName + ", from: " + modified.LastChange.ToString());
											ProcessNewConfig( "Modified", objupdate.ObjectId, modpoint.FullName);
										}
										break;
									case ObjectUpdateType.Deleted:
										if (!PointDictionary.TryRemove(Info.Key, out PointInfo removed))
										{
											Console.WriteLine("Error removing deleted point: " + objupdate.ObjectId);
										}
										Info.Value.Dispose();
										break;
									case ObjectUpdateType.Renamed:
										var renpoint = AdvConnection.LookupObject(new ObjectId(objupdate.ObjectId));
										if (renpoint != null)
										{
											Info.Value.Rename(renpoint.FullName);
											Console.WriteLine("Renamed to: " + renpoint.FullName);
											ProcessNewConfig("Renamed", objupdate.ObjectId, renpoint.FullName);
										}
										break;
									default:
										break;
								}
								break;
							}
						}
					}
					if ((DateTimeOffset.UtcNow - ConfigStartTime).TotalSeconds > 1)
					{
						//Console.WriteLine("End after 1sec");
						break;
					}

				}
			}
			return UpdateCount;
		}

		// Callbacks - Tag Update
		static void TagUpdateEvent(object sender, TagsUpdatedEventArgs EventArg)
		{
			foreach (var Update in EventArg.Updates)
			{
				// We create a copy, as we need to create our own, and TagUpdate fields are read-only
				var ThisUpdate = new CustomTagUpdate
				{
					Id = Update.Id,
					Status = Update.Status,
					Value = Update.Value,
					Quality = (long)Update.Quality,
					Timestamp = Update.Timestamp
				};
				UpdateQueue.Enqueue(ThisUpdate);
				//Console.WriteLine("Tag Update: " + Update.Id.ToString() + ", " + Update.Value.ToString());
			}
		}
		// Object Update (configuration)
		static void ObjUpdateEvent(object sender, ClearScada.Client.ObjectUpdateEventArgs EventArg)
		{
			ConfigQueue.Enqueue(EventArg);
			//Console.WriteLine("Object Config: " + EventArg.ObjectId.ToString() + "  " + EventArg.ObjectName + "  " + EventArg.UpdateType + "  " + EventArg.ObjectClass);
		}

	}

}
