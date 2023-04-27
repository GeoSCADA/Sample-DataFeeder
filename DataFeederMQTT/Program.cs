using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using ClearScada.Client; // Find ClearSCADA.Client.dll in the Program Files\Schneider Electric\ClearSCADA folder
using ClearScada.Client.Advanced;
using System.IO;
using Newtonsoft.Json; // Bring in with nuget
using FeederEngine;
using System.Collections.Concurrent;
using uPLibrary.Networking.M2Mqtt; // Bring in with nuget

// Test and demo app for the FeederEngine and PointInfo classes (added from the DataFeeder Project)
// This app writes output data in JSON format to an MQTT server
// It can easily be modified to change the format or output destination.

// Note that if Geo SCADA server state changes (e.g. Main to Fail or Standby to Main) then this program will stop.
// If the MQTT connection fails then the program also stops.
// You can add more advanced behaviours as you wish, for example creating this as a service.
namespace DataFeederApp
{

	class Program
	{
		// Current Update Rates. Please read carefully:
		// ============================================
		// These are the change detection intervals for points. 
		// Two separate intervals for Historic and Non-Historic points can be set.
		// You are not recommended to speed these up much, as performance is impacted if this time is short. 
		// Historic interval: UpdateIntervalHisSec
		// If you are feeding mostly points with historic data, then the PointInfo class will fill in the gaps
		// with data queries and you can increase the historic detection interval (longer) for better performance. 
		// If you can wait up to 5 minutes or more to receive data, then you should. Set this to 300, or longer.
		// Current data interval: UpdateIntervalCurSec
		// If you are feeding points with current data (i.e. their History is not enabled), then that interval
		// is the minimum time interval that changes will be detected. If you are scanning data sources every
		// 30 seconds with NO history and you want every update, then set this interval to match. 
		// Setting this shorter will impact performance. Do not use less than 30 seconds, that is not sensible.
		private static int UpdateIntervalHisSec = 300;
		private static int UpdateIntervalCurSec = 60;

		// This limits the period searched for historic data on start-up.
		// It's useful to have this to prevent large historic queries.
		// When the AddSubscription method is called with a Start Time, that time will be adjusted if the time period
		// (from then to now) is greater than this maximum age. Therefore when this export is restarted after a time
		// gap of more than this age, then data will be missing from the export.
		private static int MaxDataAgeDays = 1;

		// Stop signal - FeederEngine will set this to False when the SCADA server stops or changes state.
		private static bool Continue;

		// Performance counts
		public static long UpdateCount = 0;
		public static long BatchCount = 0;

		// Test data file for output of historic, current or configuration data
		private static string FileBaseName = @"Feeder\ExportData.txt";

		// last update times list
		private static Dictionary<int, DateTimeOffset> UpdateTimeList;

		// Global node and Geo SCADA server connection -- using ClearScada.Client.Advanced;
		private static ServerNode node;
		private static IServer AdvConnection;

		// The Mqtt Library object
		private static MqttClient Client;

		// Connection parameters
		private static string MQTTServerName = "192.168.86.123";
		private static int MQTTServerPort = 1883;
		private static string MQTTClientName = "Geo SCADA Data Feeder 1";
		private static string MQTTTopicName = "Geo SCADA";

		// In-memory queue of messages to be sent out
		private static ConcurrentQueue<string> OutputQueue = new ConcurrentQueue<string>();

		/// <summary>
		/// Demo program showing simple data subscription and feed
		/// </summary>
		/// <param name="args"></param>
		/// <returns></returns>
		async static Task Main(string[] args)
		{
			// MQTT Connection
			// This uses no security, you are recommended to implement connection security
			try
			{
				Client = new MqttClient(MQTTServerName, MQTTServerPort, false, MqttSslProtocols.None, null, null);
				Client.ProtocolVersion = MqttProtocolVersion.Version_3_1_1;
				// Events
				Client.ConnectionClosed += Client_MqttConnectionClosed;
				// Connect
				Client.Connect(MQTTClientName, "", "", true, 3600);
				// No username and password above, you can use Client.Connect(MQTTClientName, username, password ...);
			}
			catch
			{
				Console.WriteLine("Cannot connect to MQTT");
				return;
			}

			// Geo SCADA Connection
			// Older Geo SCADA node = new ServerNode(ClearScada.Client.ConnectionType.Standard, "127.0.0.1", 5481);
			node = new ServerNode("127.0.0.1", 5481);
			try
			{
				// Older Geo SCADA: AdvConnection = node.Connect("Utility", false);
				var ConSet = new ClientConnectionSettings();
				AdvConnection = node.Connect("Utility", ConSet);
			}
			catch
			{
				Console.WriteLine("Cannot connect to Geo SCADA");
				return;
			}
			// Good practice means storing credentials with reversible encryption, not adding them to code or using parameters as here.
			if (args.Length != 2)
			{
				Console.WriteLine("Requires user name and password as command line arguments.");
			}
			var spassword = new System.Security.SecureString();
			foreach (var c in args[1])
			{
				spassword.AppendChar(c);
			}
			try
			{
				AdvConnection.LogOn(args[0], spassword);
			}
			catch
			{
				Console.WriteLine("Cannot log on to Geo SCADA");
				return;
			}
			Console.WriteLine("Logged on.");
			
			// Set up connection, read rate and the callback function/action for data processing
			if (!Feeder.Connect(AdvConnection, true, true, UpdateIntervalHisSec, UpdateIntervalCurSec, MaxDataAgeDays, ProcessNewData, ProcessNewConfig, EngineShutdown, FilterNewPoint))
			{
				Console.WriteLine("Not connected");
				return;
			}
			Console.WriteLine("Connect to Feeder Engine.");

			// For writing data
			CreateExportString();

			// Read file of last update times
			UpdateTimeList = ReadUpdateTimeList( FileBaseName);

			// This is a bulk test - all points. Either for all using ObjectId.Root, or a specified group id such as MyGroup.Id
			// Gentle reminder - only watch and get what you need. Any extra is a waste of performance.
			// Use "$Root" for all points in the system, or customise to a specific group
			var MyGroup = AdvConnection.FindObject("$Root"); // This group id could be used to monitor a subgroup of points
			await AddAllPointsInGroup(MyGroup.Id, AdvConnection);
			// For a single point test, use this.
			//Feeder.AddSubscription( "Test.A1b", DateTimeOffset.MinValue);

			Console.WriteLine("Points Watched: " + Feeder.SubscriptionCount().ToString());
			Continue = true; // Set to false by a shutdown event/callback
							 // Stats during the data feed:
			long UpdateCount = 0;
			DateTimeOffset StartTime = DateTimeOffset.UtcNow;
			DateTimeOffset FlushUpdateFileTime = DateTimeOffset.UtcNow;
			double ProcTime = 0;
			int LongestQueue = 0;
			int LastQueue = 0;
			do
			{
				// Check time and cause processing/export
				DateTimeOffset ProcessStartTime = DateTimeOffset.UtcNow;

				UpdateCount += Feeder.ProcessUpdates(); // Keep calling to pull data out. It returns after one second of process time. Adjust as needed.

				ProcTime = (DateTimeOffset.UtcNow - ProcessStartTime).TotalMilliseconds;

				// Output stats
				Console.WriteLine($"Updates: {UpdateCount} Rate: {(int)(UpdateCount / (DateTimeOffset.UtcNow - StartTime).TotalSeconds)} /sec Process Time: {ProcTime}mS, Queued: {Feeder.ProcessQueueCount()}, Out Q: {OutputQueue.Count}, Pubs: {BatchCount}");

				// Flush UpdateTimeList file every minute
				if (FlushUpdateFileTime.AddSeconds(60) < DateTimeOffset.UtcNow)
				{
					Console.Write("Flush UpdateTime File - start...");
					WriteUpdateTimeList(FileBaseName, UpdateTimeList);
					FlushUpdateFileTime = DateTimeOffset.UtcNow;
					Console.WriteLine("End");

					// Also flush data regularly
					CloseOpenExportString();
				}

				await Task.Delay(1000); // You must pause to allow the database to serve other tasks. 
										// Consider the impact on the server, particularly if it is Main or Standby. A dedicated Permanent Standby could be used for this export.

				// Check if we are falling behind - and recommend longer scan interval
				int PC = Feeder.ProcessQueueCount();
				if (PC > LongestQueue)
				{
					// Gone up, and previous count wasn't low
					if (LastQueue > 100)
					{
						Console.WriteLine("*** Queue size increasing, queue not being processed quickly, consider increasing update interval.");
					}
					LongestQueue = PC;
				}
				LastQueue = PC;

				SendToMQTTServer();

			} while (Continue);

			// Finish by writing time list
			WriteUpdateTimeList(FileBaseName, UpdateTimeList);
			// Flush remaining data.
			CloseOpenExportString();
			SendToMQTTServer();
			// Close MQTT Connection
			Client.Disconnect();
		}

		private static void SendToMQTTServer()
		{
			// Dequeue to MQTT
			while (OutputQueue.Count > 0)
			{
				if (OutputQueue.TryDequeue(out String update))
				{
					try
					{
						// Using QoS level 1 - receive at least once
						Client.Publish(MQTTTopicName, System.Text.Encoding.UTF8.GetBytes(update), 1, false);
						BatchCount++;
						//if (BatchCount % 100 == 0)
						//	Console.WriteLine("Published: " + BatchCount.ToString() + " " + update.Substring(0,90) + "...");
					}
					catch (Exception e)
					{
						Console.WriteLine("*** Error sending message: " + e.Message);
						// Some handling here should persist the dequeued and queued data and retry
					}
				}
			}
		}

		/// <summary>
		/// List recursively all point objects in all sub-groups
		/// Declared with async to include a delay letting other database tasks work
		/// </summary>
		/// <param name="group"></param>
		/// <param name="AdvConnection"></param>
		public static async Task AddAllPointsInGroup(ObjectId group, IServer AdvConnection)
		{
			// Add the two data value sources
			var points = AdvConnection.ListObjects("CDBPoint", "", group, true);
			AddPoints(points, AdvConnection);
			var accumulators = AdvConnection.ListObjects("CAccumulatorBase", "", group, true);
			AddPoints(accumulators, AdvConnection);

			// Recurse into groups
			var groups = AdvConnection.ListObjects("CGroup", "", group, true);
			foreach (var childgroup in groups)
			{
				await AddAllPointsInGroup(childgroup.Id, AdvConnection);
				await Task.Delay(1); // You must pause to allow the database to serve other tasks, especially if many points are being added. 
			}
		}

		/// <summary>
		/// Add subscriptions to all points to be monitored
		/// </summary>
		/// <param name="objects"></param>
		/// <param name="AdvConnection"></param>
		public static void AddPoints(ObjectDetails[] objects, IServer AdvConnection)
		{
			foreach (var point in objects)
			{
				// Only add points of type analog and digital - you can customise this
				if (FilterNewPoint( point ))
				{
					// Reading and use the LastChange parameter from our persistent store.
					// This will ensure gap-free historic data.
					DateTimeOffset StartTime = DateTimeOffset.MinValue;
					if (UpdateTimeList.ContainsKey(point.Id))
					{
						StartTime = UpdateTimeList[point.Id];
						// Console.WriteLine("Add '" + point.FullName + "' from: " + StartTime.ToString() );
					}
					if (!Feeder.AddSubscription(point.FullName, StartTime))
					{
						Console.WriteLine("Error adding point. " + point.FullName);
					}
					else
					{
						int SubCount = Feeder.SubscriptionCount();
						if (SubCount % 5000 == 0)
						{
							Console.WriteLine("Points Watched: " + SubCount.ToString());
						}
					}
				}
			}
		}

		/// <summary>
		/// Function to return a dictionary of last-updated times per object Id
		/// </summary>
		/// <param name="FileBase">A filename - only the folder part is used to create our point/time tracking file</param>
		/// <returns></returns>
		static Dictionary<int, DateTimeOffset> ReadUpdateTimeList(string FileBase)
		{
			Dictionary<int, DateTimeOffset> UpdateTimeList = new Dictionary<int, DateTimeOffset>();

			Directory.CreateDirectory(Path.GetDirectoryName(FileBase));
			if (File.Exists(Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList.csv"))
			{
				StreamReader UpdateTimeListFile = new StreamReader(Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList.csv");
				string line;
				while ((line = UpdateTimeListFile.ReadLine()) != null)
				{
					string[] fields = line.Split(',');
					int id;
					DateTimeOffset date;
					if (fields.Length == 2 && int.TryParse(fields[0], out id) && DateTimeOffset.TryParse(fields[1], out date))
					{
						UpdateTimeList.Add(id, date);
					}
				}
				UpdateTimeListFile.Close();
			}
			return UpdateTimeList;
		}

		/// <summary>
		/// Write the CSV list of point names and update times
		/// </summary>
		/// <param name="FileBase">A filename - only the folder part is used to create our point/time tracking file</param>
		/// <param name="UpdateTimeList">Dictionary of points and their times</param>
		static void WriteUpdateTimeList(string FileBase, Dictionary<int, DateTimeOffset> UpdateTimeList)
		{
			// First write to a temporary file
			StreamWriter UpdateTimeListFile = new StreamWriter(Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList new.csv");
			foreach( KeyValuePair<int, DateTimeOffset> entry in UpdateTimeList)
			{
				UpdateTimeListFile.WriteLine(entry.Key.ToString() + "," + entry.Value.ToString());
			}
			UpdateTimeListFile.Close();
			// Then switch files
			if (File.Exists(Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList.csv"))
			{
				File.Replace(Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList new.csv",
								Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList.csv",
								Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList old.csv");
			}
			else
			{
				File.Move(Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList new.csv",
								Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList.csv");
			}
		}

		private static void Client_MqttConnectionClosed(object sender, System.EventArgs e)
		{
			Console.WriteLine("Callback to MqttConnectionClosed");
			// Stop the export - could add handling of this with buffering and re-attempts to connect
			Continue = false;
		}

		// For MQTT we have a simple string, appended for new data 

		static string ExportString = "";
		static int StringCount = 0;
		static int ExportStringInitialLength = 0;
		public static void CreateExportString()
		{
			StringCount++;
			ExportString = "{\"dataTime\":\"" + DateTimeOffset.UtcNow.ToString() + "." + DateTimeOffset.UtcNow.ToString("fff") + "\",\"data\":["; // Wrap individual items as an array
			ExportStringInitialLength = ExportString.Length;
		}

		public static void CloseOpenExportString()
		{
			// If data exists to export
			if (ExportString.Length > ExportStringInitialLength)
			{
				ExportString += "]}";

				// Console.WriteLine(ExportString.Substring(0, 120) + "...");
				OutputQueue.Enqueue(ExportString);

				// Start gathering next
				CreateExportString();
			}
		}

		public static void ExportString_WriteLine(string Out)
		{
			// Create a new file after <80K (e.g. set by behaviour of target MQTT subscriber)
			if (ExportString.Length > 80000)
			{
				CloseOpenExportString();
			}
			// Don't write a comma in the first entry
			if (ExportString.Length > ExportStringInitialLength)
			{
				ExportString += ",";
			}
			ExportString += Out;
		}

		/// <summary>
		/// Callback Function to read and process incoming data
		/// </summary>
		/// <param name="UpdateType">"Cur" or "His" to indicate type of update</param>
		/// <param name="Id"></param>
		/// <param name="PointName"></param>
		/// <param name="Value"></param>
		/// <param name="Timestamp"></param>
		/// <param name="Quality"></param>
		public static void ProcessNewData(string UpdateType, int Id, string PointName, double Value, DateTimeOffset Timestamp, long Quality)
		{
			// If the data retrieval fails, call for shutdown
			// Consider making UpdateType an enumeration
			if (UpdateType == "Shutdown")
			{
				EngineShutdown();
				return;
			}
			var DataUpdate = new DataVQT
			{
				PointId = Id,
				UpdateType = UpdateType,
				PointName = PointName,
				Value = Value,
				Timestamp = Timestamp, // For your application, periodically buffer this to use as the LastChange parameter on restarting.
									   // This enables gap-free data historic data.
				OPCQuality = ((int)(Quality) & 255),
				ExtendedQuality = ((int)(Quality) / 256)
			};
			string json = JsonConvert.SerializeObject(DataUpdate);

			ExportString_WriteLine(json);

			// Update id and Date in the list
			if (UpdateTimeList.ContainsKey( Id) )
			{
				UpdateTimeList[ Id ] = Timestamp;
			}
			else
			{
				UpdateTimeList.Add(Id, Timestamp);
			}
		}
		
		/// <summary>
		/// Write out received configuration data
		/// </summary>
		/// <param name="UpdateType"></param>
		/// <param name="Id"></param>
		/// <param name="PointName"></param>
		public static void ProcessNewConfig(string UpdateType, int Id, string PointName)
		{
			// Could add further code to get point configuration fields and types to export, 
			// e.g. GPS locations, analogue range, digital states etc.
			// Note that this would increase start time and database load during start, 
			//  if the Connect method has 'UpdateConfigurationOnStart' parameter set.
			// Get Point properties, these will depend on type
			object[] PointProperties = { "", 0, 0, "", 0, 0, 0};
			PointProperties = AdvConnection.GetObjectFields(PointName, new string[] { "TypeName", "FullScale", "ZeroScale", "Units", "BitCount", "GISLocation.Latitude", "GISLocation.Longitude" });
			var ConfigUpdate = new ConfigChange();
			try
			{
				ConfigUpdate.PointId = Id;
				ConfigUpdate.UpdateType = UpdateType;
				ConfigUpdate.PointName = PointName;
				ConfigUpdate.Timestamp = DateTimeOffset.UtcNow;
				ConfigUpdate.ClassName = (string)PointProperties[0];
				if (PointProperties[1] is float)
				{
					ConfigUpdate.FullScale = (float)(PointProperties[1] ?? (float)0);
					ConfigUpdate.ZeroScale = (float)(PointProperties[2] ?? (float)0);
				}
				if (PointProperties[1] is double)
				{
					ConfigUpdate.FullScale = (double)(PointProperties[1] ?? (double)0);
					ConfigUpdate.ZeroScale = (double)(PointProperties[2] ?? (double)0);
				}
				ConfigUpdate.Units = (string)(PointProperties[3] ?? "");
				if (PointProperties[1] is byte)
				{
					ConfigUpdate.BitCount = (byte)(PointProperties[4] ?? (byte)0);
				}
				if (PointProperties[1] is ushort)
				{
					ConfigUpdate.BitCount = (ushort)(PointProperties[4] ?? (ushort)0);
				}
				ConfigUpdate.Latitude = (double)(PointProperties[5] ?? (double)0);
				ConfigUpdate.Longitude = (double)(PointProperties[6] ?? (double)0);
			}
			catch (Exception e)
			{
				Console.WriteLine("Error reading properties for configuration: " + e.Message);
			}
			string json = JsonConvert.SerializeObject(ConfigUpdate);
			ExportString_WriteLine(json);
		}

		/// <summary>
		/// Called when server state changes from Main or Standby, this stops all monitoring
		/// </summary>
		public static void EngineShutdown()
		{
			Console.WriteLine("Engine Shutdown");
			Continue = false;
		}

		/// <summary>
		/// Callback used to filter new points being added to configuration.
		/// Also used to filter points added to monitored list on startup.
		/// In this example case we filter newly configured points to allow analog and digital (not string, time points).
		/// Also remove template points.
		/// </summary>
		/// <param name="NewObject">Of the point (or accumulator)</param>
		/// <returns>True to start watching this point</returns>
		public static bool FilterNewPoint(ObjectDetails NewObject)
		{
			if (NewObject.TemplateId == -1 &&
				(NewObject.ClassName.ToLower().Contains("analog") ||
				NewObject.ClassName.ToLower().Contains("alg") ||
				NewObject.ClassName.ToLower().Contains("digital") ||
				NewObject.ClassName.ToLower().Contains("binary") ||
				NewObject.ClassName.ToLower().Contains("accumulator")))
			{
				//Console.WriteLine("Add: " + NewObject.FullName);
				return true;
			}
			else
			{
				//Console.WriteLine("Ignore: " + NewObject.FullName);
				return false;
			}
		}
	}

	// Data structure of exported data
	public class DataVQT
	{
		public int PointId;
		public string UpdateType;
		public string PointName;
		public double Value;
		public DateTimeOffset Timestamp;
		public int OPCQuality;
		public int ExtendedQuality;
	}
	// Data structure of configuration data
	public class ConfigChange
	{
		public int PointId;
		public string UpdateType;
		public string PointName;
		public DateTimeOffset Timestamp;
		public string ClassName;
		public Double FullScale;
		public Double ZeroScale;
		public string Units;
		public int BitCount;
		public Double Latitude;
		public Double Longitude;
	}
}
