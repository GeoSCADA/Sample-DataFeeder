using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClearScada.Client;
using ClearScada.Client.Advanced;
using System.IO;
using System.Collections.Concurrent;

namespace DataFeederApp
{


	class Program
	{
		// Global node and connection
		static ClearScada.Client.ServerNode node;
		//static ClearScada.Client.Simple.Connection connection;
		static IServer AdvConnection;

		// Dict of PointInfo
		static ConcurrentDictionary<int, PointInfo> PointDictionary = new ConcurrentDictionary<int, PointInfo>();
		static int NextKey = 1;
		// Dict of Updates
		static ConcurrentQueue<TagUpdate> UpdateQueue = new ConcurrentQueue<TagUpdate>();
		// Dict of Config changes
		static ConcurrentQueue<ObjectUpdateEventArgs> ConfigQueue = new ConcurrentQueue<ObjectUpdateEventArgs>();
		// Counters
		public static long UpdateCount = 0;
		// Current Update Rate
		public static int UpdateRateSec = 10;

		async static Task Main(string[] args)
		{
			node = new ClearScada.Client.ServerNode(ClearScada.Client.ConnectionType.Standard, "127.0.0.1", 5481);
			//connection = new ClearScada.Client.Simple.Connection("Utility");
			//connection.Connect(node);
			AdvConnection = node.Connect("Utility", false);

			var spassword = new System.Security.SecureString();
			foreach (var c in "SnoopySnoopy")
			{
				spassword.AppendChar(c);
			}
			AdvConnection.LogOn("Serck", spassword);

			// Set event callback
			AdvConnection.TagsUpdated += TagUpdateEvent;
			
			// Set config change callback
			var WaitFor = new string[] { "CDBPoint", "CAccumulatorBase" };
			AdvConnection.ObjectUpdated += ObjUpdateEvent;
			AdvConnection.AdviseObjectUpdates(false, WaitFor);

			// Bulk test - all points
			AddAllPoints(ObjectId.Root, AdvConnection );
			// Single test
			//PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, "Test.A1b", 2, AdvConnection));
			//NextKey++;
			Console.WriteLine("Points Watched: " + PointDictionary.Count.ToString());

			do
			{
				// If queued updates
				if (UpdateQueue.Count>0) 
					Console.WriteLine("Queued Updates: " + UpdateQueue.Count.ToString());
				while (UpdateQueue.Count > 0)
				{
					if (UpdateQueue.TryDequeue(out TagUpdate update))
					{
						// Find point
						if (PointDictionary.TryGetValue(update.Id, out PointInfo info))
						{
							UpdateCount += info.ReadHistoric(AdvConnection, update);
						}
					}
				}

				// If queued config
				if (ConfigQueue.Count > 0) 
					Console.WriteLine("Queued Config: " + ConfigQueue.Count.ToString());
				while (ConfigQueue.Count > 0)
				{
					if (ConfigQueue.TryDequeue(out ObjectUpdateEventArgs objupdate))
					{
						// Find point - will need to look at the full list
						foreach (var Info in PointDictionary)
						{
							if (Info.Value.PointId == objupdate.ObjectId)
							{
								// We have the point which changed
								switch (objupdate.UpdateType)
								{
									case ObjectUpdateType.Created:
										var newpoint = AdvConnection.LookupObject(new ObjectId(objupdate.ObjectId));
										if (newpoint != null)
										{
											PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, newpoint.FullName, UpdateRateSec, AdvConnection));
											NextKey++;
											Console.WriteLine("Added new point: " + newpoint.FullName);
										}
										break;
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
											PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, modpoint.FullName, UpdateRateSec, AdvConnection));
											NextKey++;
											Console.WriteLine("Replaced point: " + modpoint.FullName);
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
										}
										break;
									default:
										break;
								}
								break;
							}
						}
					}
				}

				Console.WriteLine("Total Updates: " + UpdateCount.ToString());
				await Task.Delay(100);
			} while (true);
		}

		public static void AddAllPoints(ObjectId group, IServer AdvConnection)
		{
			var points = AdvConnection.ListObjects("CDBPoint", "", group, true);
			AddObjects(points, AdvConnection);
			var accumulators = AdvConnection.ListObjects("CAccumulatorBase", "", group, true);
			AddObjects(accumulators, AdvConnection);

			// Recurse groups
			var groups = AdvConnection.ListObjects("CGroup", "", group, true);
			foreach (var childgroup in groups)
			{
				AddAllPoints(childgroup.Id, AdvConnection);
			}
		}

		public static void AddObjects( ObjectDetails[] objects, IServer AdvConnection)
		{
			foreach (var point in objects)
			{
				//if (point.Id == 405073)
				//{
				//	Console.WriteLine("Add Point: " + point.FullName + ", " + point.ClassName + ", " + NextKey.ToString());
				//}
				if (!PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, point.FullName, UpdateRateSec, AdvConnection)))
				{
					Console.WriteLine("Error adding point.");
				}
				else
				{
					NextKey++;
					if (NextKey % 1000 == 0)
					{
						Console.WriteLine("Points Watched: " + NextKey.ToString());
					}
				}
			}
		}

		// Callbacks - Tag Update
		static void TagUpdateEvent(object sender, TagsUpdatedEventArgs EventArg)
		{
			foreach (var Update in EventArg.Updates)
			{
				UpdateQueue.Enqueue(Update);
			}
		}
		// Object Update (configuration)
		static void ObjUpdateEvent(object sender, ClearScada.Client.ObjectUpdateEventArgs EventArg)
		{
			ConfigQueue.Enqueue(EventArg);
			Console.WriteLine("\nObject Config: " + EventArg.ObjectId.ToString() + "  " + EventArg.ObjectName + "  " + EventArg.UpdateType + "  " + EventArg.ObjectClass);
		}
	}

	public class PointInfo
	{
		private int TagId { get; set; }
		private string PointName { get; set; }
		private bool IsAccumulator { get; set; }
		private bool IsHistoric { get; set; }
		private string HistoricAggregateName { get; set; }
		private int ReadSeconds { get; set; }
		private DateTimeOffset LastChange;
		public int PointId;

		// Single connection
		static IServer AdvConnection;
		static bool ServerConnect = false;

		// Constructor
		public PointInfo(int _TagId, string _PointName, int _ReadSeconds, IServer _AdvConnection)
		{
			IsAccumulator = false;
			IsHistoric = false;
			TagId = _TagId;
			ReadSeconds = _ReadSeconds;
			PointName = _PointName;
			LastChange = DateTime.Now.AddSeconds(-60);  // Default time to wind back on start lastchange

			// One time setup server
			if (!ServerConnect)
			{
				AdvConnection = _AdvConnection;
			}

			// Get point object from id
			var Point = AdvConnection.FindObject(PointName);
			PointId = Point.Id.ToInt32();

			if (Point.ClassName.Contains("Accumulator") || Point.ClassName.Contains("CBooleanCounter"))
			{
				IsAccumulator = true;
			}
			// Check for a historic aggregate
			foreach (AggregateDetails aggregate in Point.Aggregates)
			{
				if (aggregate.IsHistoric)
				{
					HistoricAggregateName = aggregate.Name;
					var HisEnabled = AdvConnection.GetObjectFields(PointName, new string[] { HistoricAggregateName });
					if ((bool)HisEnabled[0])
					{
						IsHistoric = true;
						break;
					}
				}
			}

			if (IsHistoric)
			{
				AdvConnection.AddTags(new TagDetails(TagId, PointName + "." + HistoricAggregateName + ".LastUpdateTime",
																				new TimeSpan(0, 0, ReadSeconds)));
			}
			else
			{
				if (IsAccumulator)
				{
					AdvConnection.AddTags(new TagDetails(TagId, PointName + ".CurrentTotal", new TimeSpan(0, 0, ReadSeconds)));
				}
				else
				{
					AdvConnection.AddTags(new TagDetails(TagId, PointName + ".CurrentValue", new TimeSpan(0, 0, ReadSeconds)));
				}
			}
		}

		// This is done manually to remove subscription immediately (implement IDisposable, but delays subscription)
		public void Dispose()
		{
			Console.WriteLine("Remove point: " + PointId.ToString());
			AdvConnection.RemoveTags(new int[] { TagId });
		}

		// Read historic or real-time data
		public int ReadHistoric(IServer AdvConnection, TagUpdate Update)
		{
			int UpdateCount = 0;
			if (IsHistoric) // UpdateValue is the historic update time
			{
				// Get historic data from previous update to this one
				//																										No filter, aggregate or offset
				HistoricTag tag = new HistoricTag(PointName + "." + HistoricAggregateName, 0, 0, 0);
				var tags = new HistoricTag[1];
				tags[0] = tag;
				// add a milli to last time to prevent read of sample already sent					
				// add refresh interval to update time to read forwards
				// false = don't read boundaries (null values at each end)
				try
				{
					var historicItems = AdvConnection.ReadRawHistory(LastChange.AddMilliseconds(1), ((DateTimeOffset)Update.Value).AddSeconds(2), 100, false, tags);
					foreach (var hi in historicItems)
					{
						foreach (var s in hi.Samples)
						{
							LastChange = s.Timestamp; // Write time of sample last sent
							Console.WriteLine("His> " + PointName + ", " + s.Value.ToString() + ", " + s.Timestamp.ToString("F") + "." + s.Timestamp.ToString("FFF") + ", " + ((int)(s.Quality) & 255) + ", " + ((int)(s.Quality) / 256) );
							UpdateCount++;
						}
					}
				}
				catch (Exception e)
				{
					Console.WriteLine("*** Error getting historic, point gone? " + e.Message);
				}

			}
			else // UpdateValue is the numeric value of the point
			{
				UpdateCount++;
				// Not historic
				Console.WriteLine("Cur> " + PointName + ", " + Update.Value.ToString() + ", " + Update.Timestamp.ToString("F") + "." + Update.Timestamp.ToString("FFF") + ", " + ((int)(Update.Quality) & 255) + ", " + ((int)(Update.Quality) / 256));
			}
			return UpdateCount;
		}
		public void Rename(string NewName)
		{
			PointName = NewName;
		}
	}

}
