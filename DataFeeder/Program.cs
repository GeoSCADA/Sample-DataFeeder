using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClearScada.Client;
using ClearScada.Client.Advanced;
using System.IO;
using System.Collections.Concurrent;

namespace ConsoleApp1
{
	class PointInfo
	{
		private int TagId { get; set; }
		private string PointName { get; set; }
		private bool IsAccumulator { get; set; }
		private bool IsHistoric { get; set; }
		private string HistoricAggregateName { get; set; }
		private int ReadSeconds { get; set; }
		private List<HistoricSample> DataList;
		private DateTimeOffset LastChange;
		public int PointId;
		// Constructor
		public PointInfo( int _TagId, string _PointName, int _ReadSeconds, ClearScada.Client.Advanced.IServer AdvConnection)
		{
			IsAccumulator = false;
			IsHistoric = false;
			TagId = _TagId;
			ReadSeconds = _ReadSeconds;
			PointName = _PointName;
			LastChange = DateTime.Now.AddSeconds(-60);  // Default time to wind back on start lastchange

			DataList = new List<ClearScada.Client.Advanced.HistoricSample>();

			// Get point object from id
			var Point = AdvConnection.FindObject(PointName);
			PointId = Point.Id.ToInt32();

			if (Point.ClassName.Contains( "Accumulator") || Point.ClassName.Contains("CBooleanCounter"))
			{
				IsAccumulator = true;
			}
			// Check for a historic aggregate
			foreach( ClearScada.Client.Advanced.AggregateDetails aggregate in Point.Aggregates)
			{
				if (aggregate.IsHistoric)
				{
					HistoricAggregateName = aggregate.Name;
					IsHistoric = true;
				}
			}

			if (IsHistoric)
			{
				AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(TagId, PointName + "." + HistoricAggregateName + ".LastUpdateTime", 
																				new TimeSpan(0, 0, ReadSeconds)));
			}
			else
			{
				if (IsAccumulator)
				{
					AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(TagId, PointName + ".CurrentValue", new TimeSpan(0, 0, ReadSeconds)));
				}
				else
				{
					AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(TagId, PointName + ".CurrentTotal", new TimeSpan(0, 0, ReadSeconds)));
				}
			}
		}
		// Read historic data
		public void ReadHistoric(ClearScada.Client.Advanced.IServer AdvConnection, ClearScada.Client.Advanced.TagUpdate Update)
		{
			if (IsHistoric) // UpdateValue is the historic update time
			{
				// Get historic data from previous update to this one
				//																												No filter, aggregate or offset
				ClearScada.Client.Advanced.HistoricTag tag = new ClearScada.Client.Advanced.HistoricTag(PointName + "." + HistoricAggregateName, 0, 0, 0);
				var tags = new ClearScada.Client.Advanced.HistoricTag[1];
				tags[0] = tag;
				// add a milli to last time to prevent read of sample already sent					
				// add refresh interval to update time to read forwards
				// false = don't read boundaries (null values at each end)
				//try
				//{
					var historicItems = AdvConnection.ReadRawHistory(LastChange.AddMilliseconds(1), ((DateTimeOffset)Update.Value).AddSeconds(2), 100, false, tags);
					foreach (var hi in historicItems)
					{
						foreach (var s in hi.Samples)
						{
							LastChange = s.Timestamp; // Write time of sample last sent
							if (PointId == 405073)
							{
								Console.WriteLine("His> " + s.Value.ToString() + ", " + s.Timestamp.ToString("F") + "." + s.Timestamp.ToString("FFF"));
							}
						}
					}
				//}
				//catch (Exception e)
				//{
				//	Console.WriteLine("*** Error getting historic: " + e.Message);
				//}

			}
			else // UpdateValue is the numeric value of the point
			{
				// Not historic
				Console.WriteLine("Cur> " + Update.Value.ToString() + ", " + Update.Timestamp.ToString("F") + "." + Update.Timestamp.ToString("FFF"));
			}
		}
	}

	

	class Program
	{
		// Global node and connection
		static ClearScada.Client.ServerNode node;
		//static ClearScada.Client.Simple.Connection connection;
		static IServer AdvConnection;

		// Dict of PointInfo
		static ConcurrentDictionary<int, PointInfo> PointDictionary = new ConcurrentDictionary<int, PointInfo>();
		static int NextKey = 1;

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
			//connection.LogOn("Serck", spassword);
			AdvConnection.LogOn("Serck", spassword);


			//ClearScada.Client.Advanced.TagDetails tags = new ClearScada.Client.Advanced.TagDetails(1, "Test.A1.CurrentTime");
			//AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(1, "Test.A1.CurrentTime", new TimeSpan(0,0,2)));
			//AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(2, "Test.A1.CurrentValue"));
			//AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(3, "Test.A1.Historic.LastUpdateTime"));
			//AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(4, "Test.A1 Ch.LastExecution"));

			//var t = AdvConnection.ParseTag("Test.A1.CurrentValue");
			//Console.WriteLine($"{t.ObjectName}, {t.PropertyName}, {t.PropertyType}, {t.PropertyDispId}");

			//var r = AdvConnection.ReadTagVqt(2);
			//Console.WriteLine($"{r.Value}, {r.Quality}, {r.Timestamp}, {r.HasTimestamp}");

			//PointDictionary.Add(NextKey, new PointInfo(NextKey, "Test.A1", 2, AdvConnection));
			//NextKey++;


			var points = AdvConnection.ListObjects("CDBPoint", "*b*", ObjectId.Root, false);
			foreach( var child in points)
			{
				Console.WriteLine("Add Point: " + child.FullName + ", " + child.ClassName + ", " + NextKey.ToString());
				if (!PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, child.FullName, 2, AdvConnection)))
				{
					Console.WriteLine("Error");
				}
				else
				{
					NextKey++;
				}
			}

			AdvConnection.TagsUpdated += TagUpdate;

			// Bulk test - all points
			//AddAllPoints( connection.GetObject( "$Root"), AdvConnection );


			//var TestGroup = connection.GetObject("Test");
			//var ObjectId = new ClearScada.Client.ObjectId(TestGroup.Id);
			//FileStream Stream = File.OpenRead("C:\\Users\\sesa170272\\Desktop\\New Analog Point (2).sde");
			//int result = AdvConnection.ImportConfiguration(ObjectId, Stream, ClearScada.Client.Advanced.ImportOptions.Merge);
			//Console.WriteLine($"Import status: {result}");

			// Waiting for config change
			//string[] WaitFor = new string[1];
			//WaitFor[0] = "CDBObject"; // "CDBPoint";
			//connection.ObjectUpdated += ObjUpdate;
			//connection.RegisterForObjectUpdates(WaitFor);

			// And Events
			//AdvConnection.AddEventSubscription("E", ""); // Filter string needed

			do
			{
				await Task.Delay(1000);
				Console.Write(".");
			} while (true);
		}

		public static void AddAllPoints(ClearScada.Client.Simple.DBObject group, ClearScada.Client.Advanced.IServer AdvConnection)
		{
			var children = group.GetChildren("","");
			foreach( var child in children)
			{
				if (child.IsGroup)
				{
					AddAllPoints(child, AdvConnection);
				}
				else
				{
					string t = (string)child["TypeDesc"];
					if (t.Contains("Point"))
					{
						Console.WriteLine("Add Tag: " + child.FullName + ", " + child.ClassDefinition.Name + ", " + NextKey.ToString());

						//AdvConnection.AddTags(new ClearScada.Client.Advanced.TagDetails(child.Id.ToInt32(), child.FullName, new TimeSpan(0,0,1)));
						PointDictionary.TryAdd(NextKey, new PointInfo(NextKey, child.FullName, 2, AdvConnection));
						NextKey++;
					}
				}
			}
		}


		static void TagUpdate(object sender, ClearScada.Client.Advanced.TagsUpdatedEventArgs EventArg)
		{
			foreach (var Update in EventArg.Updates)
			{
				if (PointDictionary.TryGetValue(Update.Id, out PointInfo ThisPoint))
				{
					if (ThisPoint.PointId == 405073)
					{
						Console.WriteLine("Tag: " + Update.Id.ToString() + "  " + Update.Value.ToString());
					}
					ThisPoint.ReadHistoric(AdvConnection, Update);
				}
				else
				{
					Console.WriteLine("Invalid update key: " + Update.Id);
				}


				//if (Update.Id == 3) // Historic last update change
				//{
				//	// Get historic data from previous update to this one
				//	ClearScada.Client.Advanced.HistoricTag[] tags = new ClearScada.Client.Advanced.HistoricTag[1];
				//	ClearScada.Client.Advanced.HistoricTag tag = new ClearScada.Client.Advanced.HistoricTag( "Test.A1.Historic", 0, 0, 0);
				//	tags[0] = tag;
				//	// add a milli to last time to prevent read of sample already sent					
				//	// add refresh interval to update time to read forwards
				//	// false = don't read boundaries (null values at each end)
				//	var historicItems = AdvConnection.ReadRawHistory(lastchange.AddMilliseconds(1), ((DateTimeOffset)Update.Value).AddSeconds(2), 100, false, tags);
				//	foreach( var hi in historicItems)
				//	{
				//		foreach (var s in hi.Samples)
				//		{
				//			lastchange = s.Timestamp; // Write time of sample last sent
				//			Console.WriteLine("> " + s.Value.ToString() + ", " + s.Timestamp.ToString("F") + "." + s.Timestamp.ToString("FFF"));
				//		}
				//	}
				//}
			}
		}

		// Unused
		static void ObjUpdate(object sender, ClearScada.Client.ObjectUpdateEventArgs EventArg)
		{
			Console.WriteLine("\nObject: " + EventArg.ObjectId.ToString() + "  " + EventArg.ObjectName + "  " + EventArg.UpdateType + "  " + EventArg.ObjectClass);
		}
		static void EventUpdate(object sender, ClearScada.Client.Advanced.EventsUpdatedEventArgs EventArg)
		{
			Console.WriteLine("\nEvent: " + EventArg.ClientId.ToString() + "  " + EventArg.Updates.Count + " :"  );
			foreach( var e in EventArg.Updates)
			{
				Console.WriteLine( e.Time.ToString() + " " + e.Message);
			}
		}
	}
}
