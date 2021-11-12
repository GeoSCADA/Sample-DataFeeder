using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using ClearScada.Client; // Find ClearSCADA.Client.dll in the Program Files\Schneider Electric\ClearSCADA folder
using ClearScada.Client.Advanced;
using System.IO;
using Newtonsoft.Json; // Bring in with nuget
using Newtonsoft.Json.Linq; // For JObject
using FeederEngine;
using System.Collections.Concurrent;
// Adding for OMF Interfacing
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;


// Test and demo app for the FeederEngine and PointInfo classes (added from the DataFeeder Project)
// This app writes output data in JSON format to an OSI Pi server or OCS Cloud server
// It can easily be modified to change the format or output destination.

// Note that this export design has made specific choices which may or may not align with your needs.
// The arrangement of Type, Container and Data are generalised here, but you may wish to restructure
// them around templates and plant.
// The Pi interface is not fully tested. Please feed back on the Exchange Forums.

// Note that if Geo SCADA server state changes (e.g. Main to Fail or Standby to Main) then this program will stop.
// If the Pi connection fails then the program also stops.
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
		private static readonly int MaxDataAgeDays = 1;

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

		// The version of the OMFmessages
		private const string OmfVersion = "1.1";
		// HTTP client for OMF
		private static readonly HttpClient _client = new HttpClient();

		// In-memory queue of messages to be sent out
		// A message has a type and a JSON string
		public class OutputMessage
		{
			public string Type { get; set; }
			public string Content { get; set; }
		}
		private static ConcurrentQueue<OutputMessage> OutputQueue = new ConcurrentQueue<OutputMessage>();
		// Set this to limit packet size of requests to the target
		private static readonly int TargetMaxBufferSize = 80000;
		// List (usually 1) of endpoints to send to
		private static IList<Endpoint> endpoints;

		/// <summary>
		/// Demo program showing simple data subscription and feed
		/// </summary>
		/// <param name="args"></param>
		/// <returns></returns>
		async static Task Main(string[] args)
		{
			// OSI PI or OCS Connection
			// Step 1 - Read endpoint configurations from config.json
			AppSettings settings = GetAppSettings();
			endpoints = settings.Endpoints;

			// A Type is like a point type - you could have different ones for analog, digital, time
			// Our example has a simple numeric type. "FloatDynamicType"
			// It's defined in a file in this code, you could define programmatically.
			dynamic omfTypes = GetJsonFile("OMF-Types.json");

			// Send messages and check for each endpoint in config.json
			try
			{
				// Send out the messages that only need to be sent once
				foreach (var endpoint in endpoints)
				{
					if ((endpoint.VerifySSL is bool boolean) && boolean == false)
						Console.WriteLine("You are not verifying the certificate of the end point.  This is not advised for any system as there are security issues with doing this.");

					// Send OMF Types
					foreach (var omfType in omfTypes)
					{
						string omfTypeString = $"[{JsonConvert.SerializeObject(omfType)}]";
						SendMessageToOmfEndpoint(endpoint, "type", omfTypeString);
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Cannot connect to Pi: " + e.Message);
				return;
			}

			// Geo SCADA Connection
			node = new ServerNode(ClearScada.Client.ConnectionType.Standard, "127.0.0.1", 5481);
			try
			{
				AdvConnection = node.Connect("Utility", false);
			}
			catch
			{
				Console.WriteLine("Cannot connect to Geo SCADA");
				return;
			}
			// Good practice means storing credentials with reversible encryption, not adding them to code as here.
			var spassword = new System.Security.SecureString();
			foreach (var c in "AdminExample")
			{
				spassword.AppendChar(c);
			}
			try
			{
				AdvConnection.LogOn("AdminExample", spassword);
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
			CreateExportBuffer();

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
					CloseOpenExportBuffer();
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

				SendToPiServer();

			} while (Continue);

			// Finish by writing time list
			WriteUpdateTimeList(FileBaseName, UpdateTimeList);
			// Flush remaining data.
			CloseOpenExportBuffer();
			SendToPiServer();
			// Close Pi web Connection
			_client.Dispose();
		}

		/// <summary>
		/// Gets the application settings
		/// </summary>
		public static AppSettings GetAppSettings()
		{
			AppSettings settings = JsonConvert.DeserializeObject<AppSettings>(File.ReadAllText(Directory.GetCurrentDirectory() + "/appsettings.json"));

			// check for optional/nullable parameters and invalid endpoint types
			foreach (var endpoint in settings.Endpoints)
			{
				if (endpoint.VerifySSL == null)
					endpoint.VerifySSL = true;
				if (!(string.Equals(endpoint.EndpointType, "OCS", StringComparison.OrdinalIgnoreCase) || 
					  string.Equals(endpoint.EndpointType, "EDS", StringComparison.OrdinalIgnoreCase) || 
					  string.Equals(endpoint.EndpointType, "PI", StringComparison.OrdinalIgnoreCase)))
					throw new Exception($"Invalid endpoint type {endpoint.EndpointType}");
			}

			return settings;
		}

		/// <summary>
		/// Gets a json file in the current directory of name filename.
		/// </summary>
		public static dynamic GetJsonFile(string filename)
		{
			dynamic dynamicJson = JsonConvert.DeserializeObject(File.ReadAllText($"{Directory.GetCurrentDirectory()}/{filename}"));

			return dynamicJson;
		}

		private static void SendToPiServer()
		{
			// Dequeue to MQTT
			while (OutputQueue.Count > 0)
			{
				if (OutputQueue.TryDequeue(out OutputMessage Message))
				{
					try
					{
						foreach (var endpoint in endpoints)
						{
							// send data
							SendMessageToOmfEndpoint(endpoint, Message.Type, Message.Content);
						}
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
		/// Gets the token for auth for connecting
		/// </summary>
		/// <param name="endpoint">The endpoint to retieve a token for</param>
		public static string GetToken(Endpoint endpoint)
		{
			if (endpoint == null)
			{
				throw new ArgumentNullException(nameof(endpoint));
			}

			// PI and EDS currently require no auth
			if (endpoint.EndpointType != "OCS")
				return null;

			// use cached version
			if (!string.IsNullOrWhiteSpace(endpoint.Token))
				return endpoint.Token;

			var request = new HttpRequestMessage()
			{
				Method = HttpMethod.Get,
				RequestUri = new Uri(endpoint.Resource + "/identity/.well-known/openid-configuration"),
			};
			request.Headers.Add("Accept", "application/json");

			string res = Send(request).Result;
			var objectContainingURLForAuth = JsonConvert.DeserializeObject<JObject>(res);

			var data = new Dictionary<string, string>
			{
			   { "client_id", endpoint.ClientId },
			   { "client_secret", endpoint.ClientSecret },
			   { "grant_type", "client_credentials" },
			};

			var request2 = new HttpRequestMessage()
			{
				Method = HttpMethod.Post,
				RequestUri = new Uri(objectContainingURLForAuth["token_endpoint"].ToString()),
				Content = new FormUrlEncodedContent(data),
			};
			request2.Headers.Add("Accept", "application/json");

			string res2 = Send(request2).Result;

			var tokenObject = JsonConvert.DeserializeObject<JObject>(res2);
			endpoint.Token = tokenObject["access_token"].ToString();
			return endpoint.Token;
		}

		/// <summary>
		/// Send message using HttpRequestMessage
		/// </summary>
		public static async Task<string> Send(HttpRequestMessage request)
		{
			var response = await _client.SendAsync(request).ConfigureAwait(false);

			var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

			if (!response.IsSuccessStatusCode)
				throw new Exception($"Error sending OMF response code:{response.StatusCode}.  Response {responseString}");
			return responseString;
		}

		/// <summary>
		/// Actual async call to send message to omf endpoint
		/// </summary>
		public static string Send(WebRequest request)
		{
			if (request == null)
			{
				throw new ArgumentNullException(nameof(request));
			}

			try
			{
				var resp = request.GetResponse();
				var response = (HttpWebResponse)resp;
				var stream = resp.GetResponseStream();
				var code = (int)response.StatusCode;

				var reader = new StreamReader(stream);

				// Read the content.  
				string responseString = reader.ReadToEnd();

				// Display the content.
				return responseString;
			}
			catch (WebException e)
			{
				WebResponse response = e.Response;
				var httpResponse = (HttpWebResponse)response;

				// catch 409 errors as they indicate that the Type already exists
				if (httpResponse.StatusCode == HttpStatusCode.Conflict)
					return string.Empty;
				throw;
			}
		}

		/// <summary>
		/// Sends message to the preconfigured omf endpoint
		/// </summary>
		/// <param name="endpoint">The endpoint to send an OMF message to</param>
		/// <param name="messageType">The OMF message type</param>
		/// <param name="dataJson">The message payload in a string format</param>
		/// <param name="action">The action for the OMF endpoint to conduct</param>
		public static void SendMessageToOmfEndpoint(Endpoint endpoint, string messageType, string dataJson, string action = "create")
		{
			//Console.WriteLine($"{messageType} ({dataJson.Length}) {dataJson.Substring(0,200)}");
			if (endpoint == null)
			{
				throw new ArgumentNullException(nameof(endpoint));
			}

			// create a request
			var request = WebRequest.Create(new Uri(endpoint.OmfEndpoint));
			request.Method = "post";

			// add headers to request
			request.Headers.Add("messagetype", messageType);
			request.Headers.Add("action", action);
			request.Headers.Add("messageformat", "JSON");
			request.Headers.Add("omfversion", OmfVersion);
			if (string.Equals(endpoint.EndpointType, "OCS", StringComparison.OrdinalIgnoreCase))
			{
				request.Headers.Add("Authorization", "Bearer " + GetToken(endpoint));
			}
			else if (string.Equals(endpoint.EndpointType, "PI", StringComparison.OrdinalIgnoreCase))
			{
				request.Headers.Add("x-requested-with", "XMLHTTPRequest");
				request.Credentials = new NetworkCredential(endpoint.Username, endpoint.Password);
			}

			// compress dataJson if configured for compression
			byte[] byteArray;

			if (!endpoint.UseCompression)
			{
				request.ContentType = "application/json";
				byteArray = Encoding.UTF8.GetBytes(dataJson);
			}
			else
			{
				request.ContentType = "application/x-www-form-urlencoded";
				using (var msi = new MemoryStream(Encoding.UTF8.GetBytes(dataJson)))
				using (var mso = new MemoryStream())
				{
					using (var gs = new GZipStream(mso, CompressionMode.Compress))
					{
						// copy bytes from msi to gs
						byte[] bytes = new byte[4096];

						int cnt;

						while ((cnt = msi.Read(bytes, 0, bytes.Length)) != 0)
						{
							gs.Write(bytes, 0, cnt);
						}
					}

					byteArray = mso.ToArray();
				}

				request.Headers.Add("compression", "gzip");
			}

			request.ContentLength = byteArray.Length;

			Stream dataStream = request.GetRequestStream();

			// Write the data to the request stream.  
			dataStream.Write(byteArray, 0, byteArray.Length);

			// Close the Stream object.  
			dataStream.Close();

			Send(request);
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
						//Console.WriteLine("Add: " + point.FullName);
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

		// For Pi we have a simple string, appended for new data 
		// Each message bulks individual JSON objects into a JSON array. 
		// Within a given array, all objects must be of the same message type: "type", "container", or "data".
		// So, we can only fill the string with one type, so we monitor this with ExportMessageType and send buffer whenever type changes.
		// BUT - while data messages from Geo SCADA are single Data messages to Pi, a Configuration message from Geo SCADA will
		// consist of a Container message and a Data message. For many points, alternating these messages will be very inefficient.
		// So we have to retain a separate ExportBufferConfig for the Data messages and send it straight after any Containers
		static string ExportMessageType = "";
		static string ExportBuffer = "";
		static string ExportBufferConfig = "";
		public static void CreateExportBuffer()
		{
			ExportBuffer = "{"; // Wrap individual items as an array
			ExportBufferConfig = "{";
		}

		// Empty the ExportBuffer and ExportBufferConfig into the OutputQueue
		public static void CloseOpenExportBuffer()
		{
			// If data exists to export
			if (ExportBuffer.Length > 1)
			{
				ExportBuffer += "}";

				// Console.WriteLine(ExportBuffer.Substring(0, 120) + "...");
				var Message = new OutputMessage
				{
					Type = ExportMessageType,
					Content = ExportBuffer
				};
				OutputQueue.Enqueue(Message);
			}
			if (ExportBufferConfig.Length > 1)
			{
				ExportBufferConfig += "}";

				// Console.WriteLine(ExportBufferConfig.Substring(0, 120) + "...");
				var Message = new OutputMessage
				{
					Type = "data",	// The Config buffer is treated by Pi as a Data message
					Content = ExportBufferConfig
				};
				OutputQueue.Enqueue(Message);
			}
			// Start gathering next
			CreateExportBuffer();
			// Ready for a new message type
			ExportMessageType = "";
		}

		// 3 types of message are added: "container", "configdata" and "data"
		// "configdata" is actually "data", but relates to point configuration
		// We can only send arrays of one type in a buffer, so have to close the buffer when the type changes
		// But "container" and "configdata" will alternate, so be very inefficient unless we cache "configdata" separately
		// and output "container" and then "configdata" when buffers are full or we receive "data"
		public static void ExportBuffer_WriteLine(string Out, string MessageType)
		{
			// If "configdata" then add to different buffer
			if (MessageType == "configdata")
			{
				// Create a new file after <TargetMaxBufferSize (e.g. set by behaviour of target Pi system)
				if (ExportBufferConfig.Length > TargetMaxBufferSize )
				{
					CloseOpenExportBuffer();
				}
				// Don't write a comma in the first entry
				if (ExportBufferConfig.Length > 1)
				{
					ExportBufferConfig += ",";
				}
				ExportBufferConfig += Out;
			}
			else
			{
				// If first message, set the type
				if (ExportMessageType == "")
				{
					ExportMessageType = MessageType;
				}
				// Create a new file after <TargetMaxBufferSize (e.g. set by behaviour of target Pi system), OR when message type changes
				if (ExportBuffer.Length > TargetMaxBufferSize ||
					ExportMessageType != MessageType)
				{
					CloseOpenExportBuffer();
				}
				// Don't write a comma in the first entry
				if (ExportBuffer.Length > 1)
				{
					ExportBuffer += ",";
				}
				ExportBuffer += Out;
			}
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
			// Construct JSON
			var DataUpdateItem = new OMF_DataValueItem
			{
				Timestamp = Timestamp,
				Value = Value,
				Quality = Quality
			};
			OMF_DataValueItem [] DataUpdateItemArray = { DataUpdateItem };
			var DataUpdate = new OMF_DataValue
			{
				containerid = PointName,
				values = DataUpdateItemArray
			};
			string json = JsonConvert.SerializeObject(DataUpdate);

			ExportBuffer_WriteLine(json, "data");

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
			// Output of configuration to Pi is in two messages, a Container and a Data message we'll call "configdata"
			var Container = new OMF_Container
			{
				id = PointName,
				typeVersion = "1.0.0",
				typeid = "FloatDynamicType"
			};
			string json = JsonConvert.SerializeObject(Container);
			ExportBuffer_WriteLine(json, "container");

			// Now for the ConfigData

			// Could add further code to get point configuration fields and types to export, 
			// e.g. GPS locations, analogue range, digital states etc.
			// Note that this would increase start time and database load during start, 
			//  if the Connect method has 'UpdateConfigurationOnStart' parameter set.
			// Get Point properties, these will depend on type
			object[] PointProperties = { "", 0, 0, "", 0, 0, 0};
			PointProperties = AdvConnection.GetObjectFields(PointName, new string[] { "TypeName", "FullScale", "ZeroScale", "Units", "BitCount", "GISLocation.Latitude", "GISLocation.Longitude" });
			var ConfigUpdate = new OMF_DataObjectItem();
			try
			{
				ConfigUpdate.Id = Id;
				ConfigUpdate.UpdateType = UpdateType;
				ConfigUpdate.FullName = PointName;
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
			OMF_DataObjectItem[] DataObjectItemArray = { ConfigUpdate };
			var DataUpdate = new OMF_DataObject
			{
				containerid = PointName,
				values = DataObjectItemArray
			};
			json = JsonConvert.SerializeObject(DataUpdate);
			// Use "configdata" to indicate this message is related to configuration, interleaved with "container"
			// but sent in separate blocks of "container" and "data".
			ExportBuffer_WriteLine(json, "configdata");
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
}
