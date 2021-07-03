using System;
using System.Threading.Tasks;
using ClearScada.Client; // Find ClearSCADA.Client.dll in the Program Files\Schneider Electric\ClearSCADA folder
using ClearScada.Client.Advanced;
using Newtonsoft.Json; // Bring in with nuget
using Azure.Messaging.EventHubs.Producer;
using System.Collections.Concurrent;
using FeederEngine;

// Test and demo app for the FeederEngine and PointInfo classes (added from the DataFeeder Project)
// This app writes output data in JSON format to Azure Event Hub.
// It can easily be modified to change the format or output destination.

// Azure Event Hubs: https://docs.microsoft.com/en-us/samples/azure/azure-sdk-for-net/azuremessagingeventhubs-samples/

namespace DataFeederApp
{

	class Program
	{
		// Current Update Rate. Please read carefully:
		// ===========================================
		// This is the change detection interval for points.
		// You are not recommended to speed this up much, as performance is impacted if this time is short. 
		// If you are feeding mostly points with historic data, then the PointInfo class will fill in the gaps
		// with data queries and you can increase this detection interval for better performance. 
		// If you can wait up to 5 minutes or more to receive data, then you should. Set this to 300, or longer.
		// If you are feeding points with current data (i.e. their History is not enabled), then this interval
		// is the minimum time interval that changes will be detected. If you are scanning data sources every
		// 30 seconds with NO history and you want every update, then set this interval to match. 
		// Setting this shorter will impact performance. Do not use less than 30 seconds, that is not sensible.
		// If you have a mix of historic and non-historic points then consider modifying the library to read
		// from each at different intervals.
		private static int UpdateIntervalSec = 300;
		// Stop signal - FeederEngine will set this to False when the SCADA server stops or changes state.
		private static bool Continue;

		// Performance counts
		public static long UpdateCount = 0;
		public static long BatchCount = 0;

		// Global node and Geo SCADA server connection -- using ClearScada.Client.Advanced;
		private static ServerNode node;
		private static IServer AdvConnection;

		// connection string to the Event Hubs namespace - replace this with your key
		private const string connectionString = "Endpoint=sb://mygscada.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hUiuhIuhUIhNHOFiTYrVrTyirTRYirRtiyTr=";

		// name of the event hub - replace this with your hub name
		private const string eventHubName = "geoscadaeh";

		private static ConcurrentQueue<string> OutputQueue = new ConcurrentQueue<string>();

		// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
		// of the application, which is best practice when events are being published or read regularly.
		static EventHubProducerClient producerClient;

		/// <summary>
		/// Demo program showing simple data subscription and feed
		/// </summary>
		/// <param name="args"></param>
		/// <returns></returns>
		async static Task Main(string[] args)
		{
			// Azure Connection
			// Create a producer client that you can use to send events to an event hub
			producerClient = new EventHubProducerClient(connectionString, eventHubName);

			// Geo SCADA Connection
			node = new ServerNode(ClearScada.Client.ConnectionType.Standard, "127.0.0.1", 5481);
			AdvConnection = node.Connect("Utility", false);

			// Good practice means storing credentials with reversible encryption, not adding them to code as here.
			var spassword = new System.Security.SecureString();
			foreach (var c in "SnoopySnoopy")
			{
				spassword.AppendChar(c);
			}
			AdvConnection.LogOn("Serck", spassword);
			Console.WriteLine("Logged on.");

			// Set up connection, read rate and the callback function/action for data processing
			if (!Feeder.Connect(AdvConnection, true, UpdateIntervalSec, ProcessNewData, ProcessNewConfig, EngineShutdown, FilterNewPoint))
			{
				Console.WriteLine("Not connected");
				return;
			}
			Console.WriteLine("Connect to Feeder Engine.");

			// For writing data
			CreateExportString();

			// This is a bulk test - all points. Either for all using ObjectId.Root, or a specified group id such as MyGroup.Id
			// Gentle reminder - only watch and get what you need. Any extra is a waste of performance.
			var MyGroup = AdvConnection.FindObject("SA"); // This group id could be used to monitor a subgroup of points
			await AddAllPointsInGroup(MyGroup.Id, AdvConnection);
			// For a single point test, use this.
			//FeederEngine.AddSubscription( "Test.A1b", DateTimeOffset.MinValue);

			Console.WriteLine("Points Watched: " + Feeder.SubscriptionCount().ToString());
			Continue = true; // Set to false by a shutdown event/callback
							 // Stats during the data feed:
			long UpdateCount = 0;
			DateTimeOffset StartTime = DateTimeOffset.UtcNow;
			double ProcTime = 0;
			int LastQueue = 0;
			int HighWaterQueue = 0;
			do
			{
				// Check time and cause processing/export
				DateTimeOffset ProcessStartTime = DateTimeOffset.UtcNow;

				UpdateCount += Feeder.ProcessUpdates(); // Keep calling to pull data out. It returns after one second of process time. Adjust as needed.

				ProcTime = (DateTimeOffset.UtcNow - ProcessStartTime).TotalMilliseconds;

				// Output stats
				Console.WriteLine($"Total Updates: {UpdateCount} Rate: {(UpdateCount / (DateTimeOffset.UtcNow - StartTime).TotalSeconds)} /sec Process Time: {ProcTime}mS, Queued: {Feeder.ProcessQueueCount()}");

				await Task.Delay(1000); // You must pause to allow the database to serve other tasks. 
										// Consider the impact on the server, particularly if it is Main or Standby. A dedicated Permanent Standby could be used for this export.

				// Check if we are falling behind - and recommend longer scan interval
				int PC = Feeder.ProcessQueueCount();
				if (PC > LastQueue)
				{
					// Gone up - more than last time?
					if (PC > HighWaterQueue && PC > 100 && HighWaterQueue != 0)
					{
						Console.WriteLine("*** High water mark increasing, queue not being processed quickly, consider increasing update interval.");
						HighWaterQueue = PC;
					}
				}
				LastQueue = PC;

				await SendToAzureEventHub();

			} while (Continue);

			// Flush remaining data.
			CloseOpenExportString();
			await SendToAzureEventHub();
			await producerClient.DisposeAsync(); // May execute this when terminating
		}

		private static async Task SendToAzureEventHub()
		{
			// Dequeue to Azure
			while (OutputQueue.Count > 0)
			{
				if (OutputQueue.TryDequeue(out String update))
				{
					// Create a batch of events 
					var eventBatch = await producerClient.CreateBatchAsync();
					//EventDataBatch eventBatch = EventBatchTask.Result;

					if (!eventBatch.TryAdd(new Azure.Messaging.EventHubs.EventData(System.Text.Encoding.UTF8.GetBytes(update))))
					{
						// if it is too large for the batch
						throw new Exception($"Event is too large for the batch and cannot be sent.");
					}
					try
					{
						// Use the producer client to send the batch of events to the event hub
						await producerClient.SendAsync(eventBatch);
						BatchCount++;
						Console.WriteLine("Batch of data published: " + BatchCount.ToString());
					}
					catch (Exception e)
					{
						Console.WriteLine("*** Error sending events: " + e.Message);
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
				// For your application, consider reading and using the LastChange parameter from your persistent store.
				// This will ensure gap-free historic data.
				if (!Feeder.AddSubscription(point.FullName, DateTimeOffset.MinValue))
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

		// For Azure EventHub we have a simple string, appended for new data and 
		static string ExportString = "";
		static int StringCount = 0;
		public static void CreateExportString()
		{
			StringCount++;
			ExportString = "{\"dataTime\":\"" + DateTimeOffset.UtcNow.ToString() + "." + DateTimeOffset.UtcNow.ToString("fff") + "\",\"data\":["; // Wrap individual items as an array
		}

		public static void CloseOpenExportString()
		{
			ExportString += "]}";

			Console.WriteLine(ExportString.Substring(0, 80) + "...");
			OutputQueue.Enqueue(ExportString);

			// Start gathering next
			CreateExportString();
		}

		public static void ExportString_WriteLine(string Out)
		{
			// Create a new file after <4K (e.g. billed message size of Azure)
			if (ExportString.Length > 80000)
			{
				CloseOpenExportString();
			}
			// Don't write a comma in the first entry
			if (ExportString.Length > 55)
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
		public static void ProcessNewData(string UpdateType, int Id, string PointName, double Value, DateTimeOffset Timestamp, int Quality)
		{
			// If the data retrieval fails, call for shutdown
			// Consider making UpdateType an enumeration
			if (UpdateType == "Shutdown")
			{
				EngineShutdown();
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
			// Note that this would increase start time and database load during start.
			var ConfigUpdate = new ConfigChange
			{
				PointId = Id,
				UpdateType = UpdateType,
				PointName = PointName,
				Timestamp = DateTimeOffset.UtcNow
			};
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
		/// Callback used to filter new points being added to configuration
		/// In this case we say yes to all newly configured points
		/// </summary>
		/// <param name="FullName">Of the point (or accumulator)</param>
		/// <returns>True to start watching this point</returns>
		public static bool FilterNewPoint(string FullName)
		{
			return true;
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
	}
}
