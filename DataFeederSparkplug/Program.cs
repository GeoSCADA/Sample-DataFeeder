using System;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using ClearScada.Client; // Find ClearSCADA.Client.dll in the Program Files\Schneider Electric\ClearSCADA folder
using ClearScada.Client.Advanced;
using System.IO;
using Newtonsoft.Json; // Bring in with nuget
using FeederEngine;
using System.Collections.Concurrent;
using uPLibrary.Networking.M2Mqtt; // Bring in with nuget
using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;
using System.Security;

// Test and demo app for the FeederEngine and PointInfo classes (added from the DataFeeder Project)
// This app writes output data as a Sparkplug Server to an MQTT server
// It can easily be modified to change settings for the output.

// This uses no MQTT server login or encryption, you are recommended to implement connection security

namespace DataFeederApp
{
	class ProgramSettings
	{
		// Settings variables with default values
		// Geo SCADA Server Names and Ports
		public string GeoSCADAServerName = "127.0.0.1";
		public string GeoSCADABackupServerName = "127.0.0.1";
		public int GeoSCADAServerPort = 5481;

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
		public int UpdateIntervalHisSec = 300;
		public int UpdateIntervalCurSec = 60;
		// When a point is added, queue a data update to catch up history or get current value
		// i.e. don't wait until the first data change to read data.
		public bool CatchUpDataOnStart = true;

		// This limits the period searched for historic data on start-up.
		// It's useful to have this to prevent large historic queries.
		// When the AddSubscription method is called with a Start Time, that time will be adjusted if the time period
		// (from then to now) is greater than this maximum age. Therefore when this export is restarted after a time
		// gap of more than this age, then data will be missing from the export.
		public int MaxDataAgeDays = 1;

		// Connection parameters
		public string MQTTServerName = "127.0.0.1"; // Mosquitto running locally, or "192.168.86.123";
		public int MQTTServerPort = 1883;
		public string MQTTClientName = "SpClient1";
		// Backup server
		public string MQTTBackupServerName = "127.0.0.1"; // Leave the same if no backup needed

		// Sparkplug Version 2 or 3 - affects how STATE works. Geo SCADA 2022 uses V2.
		public int SparkplugVersion = 2;

		// Sparkplug identification
		// This is the Group ID
		public string SpGroupId = "SCADA";
		// This is the EoN Node ID
		public string SpNode = "System1";

		// We are not using Devices in this implementation
		// private static string SpDevice = "Device1"; // And the next level group name for this

		// Specify the group containing points for export.
		// Either for all using $Root, or a specified group name such as "MyGroup" or "East.Plant A"
		// Gentle reminder - only watch and get what you need. Any extra is a waste of performance.
		// This example "Demo Items.Historic Demo" will export all point data in this group and any subgroups
		public string ExportGroup = "Example Projects.Water and Wastewater";

		// Metric names will be created from point Full Names. To remove levels before sending, set this parameter
		// For example, when set to 1 with ExportGroup = "Demo Items.Historic Demo", the "Demo Items." name will be removed
		public int ExportGroupLevelTrim = 1;

		// List of string filters on the Geo SCADA ClassName of points/objects to be exported
		// New points for export will have their class name compared with this list
		// A default set of filters is found in 
		public List<string> ObjectClassFilter = new List<string>();

		// Set the host's ID here so we can know it's STATE
		// You need to configure your host's ID here
		public string SCADAHostId = "GeoSCADAExpertA123456A0";

		// Allow an option to ignore the server state and publish anyway
		// Set to false to obey the Sparkplug standard
		// You may need this to be 'true' when using Geo SCADA 2022 Initial or March 2023 Release Sparkplug Client Driver
		// The fault should fixed in Geo SCADA 2022 May 2023 Release.
		public bool SparkplugIgnoreServerState = false;

		// An option for test use which will restrict sent messages to Birth messages only, no data
		// This is a test and development option - set to true if you only want to send Birth messages
		// for example it could cause the receiving system to create points before any data is sent.
		public bool SendBirthMessagesOnly = false;
	}

	class Program
	{
		// Data file location for settings, output of date/time file of export progress, Sparkplug bdSeq. 
		// If you want to run another instance of this on the same PC, this file path needs to be different.
		public static string FileBaseName = @"c:\ProgramData\Schneider Electric\SpDataFeeder\Settings.json";
		// Credentials files
		public static string GSCredentialsFile = Path.GetDirectoryName(FileBaseName) + "\\" + "Credentials.csv";
		public static string MQTTCredentialsFile = Path.GetDirectoryName(FileBaseName) + "\\" + "MQTTCredentials.csv";

		// ********************************************************************************************************
		// Settings variables - we read these from a JSON settings file stored alongside the program exe
		private static ProgramSettings Settings = new ProgramSettings();


		// ********************************************************************************************************
		// State and performance variables

		// Geo SCADA connection state
		private static bool GeoSCADAConnectionState = false;
		// Always start with an old time here so that the first loop we try to connect
		private static DateTimeOffset GeoSCADALastConnectionTry = DateTimeOffset.UtcNow.AddSeconds(-600);
		// Connections will alternate on failure
		private static bool GeoSCADABackupServerActive = false;

		// Geo SCADA Feeder Continue signal - FeederEngine will set this to False when the Geo SCADA server stops or changes state.
		private static bool FeederContinue;

		// Performance counts
		public static long UpdateCount = 0;
		public static long BatchCount = 0;

		// last update times list
		private static Dictionary<int, DateTimeOffset> UpdateTimeList;

		// Global node and Geo SCADA server connection -- using ClearScada.Client.Advanced;
		private static ServerNode node;
		private static IServer AdvConnection;

		// The Mqtt Library object
		private static MqttClient Client;

		// Sparkplug Message Sequence Number
		private static ulong Seq = 0; // Message Sequence, Range 0..255

		// Status of Sparkplug Connection
		private static bool SparkplugConnectionState = false;
		// Always start with an old time here so that the first loop we try to connect
		private static DateTimeOffset SparkplugLastConnectionTry = DateTimeOffset.UtcNow.AddSeconds(-600);
		// Is the backup server the next one to use
		private static bool MQTTBackupServerActive = false;

		// In-memory queues of messages to be sent out
		private static ConcurrentQueue<Payload> BirthQueue = new ConcurrentQueue<Payload>();
		private static ConcurrentQueue<Payload> DataQueue = new ConcurrentQueue<Payload>();

		// Sparkplug Birth Message - accumulates all points, gets sent when values come in
		private static Payload BirthMessage = new Payload();
		private static bool BirthMessageChanged = false;

		// Sparkplug Data Message - buffers data changes until full, then gets sent
		private static Payload DataMessage = new Payload();

		// Logging setup with NLog - got from NuGet - match same version as Geo SCADA
		private static readonly NLog.Logger Logger = NLog.LogManager.GetLogger("Sparkplug");

		/// <summary>
		/// Sparkplug Server program for Geo SCADA data subscription.
		/// Command line parameters are a Username and Password for Geo SCADA and optional user/pass for the MQTT Broker.
		/// If they are not passed in then a file is read for encrypted credentials.
		/// Run this with parameters once to write the Username and Password into the file, 
		/// then clear the parameters for future runs.
		/// 
		/// The same approach is taken for settings. Default settings are written to a JSON file 
		/// on first run. Edit this file for further runs.
		/// 
		/// File location is: c:\ProgramData\Schneider Electric\SpDataFeeder\
		/// </summary>
		/// <param name="args">[0] Geo SCADA User, [1] Geo SCADA Password, [2] MQTT User, [3] MQTT Password</param>
		/// <returns></returns>
		async static Task Main(string[] args)
		{
			var config = new NLog.Config.LoggingConfiguration();
			var logfile = new NLog.Targets.FileTarget("logfile") 
			{ 
				FileName = Path.GetDirectoryName(FileBaseName) + "\\Log\\" + "SpDataFeeder.log",
				MaxArchiveFiles = 10,
				ArchiveNumbering = NLog.Targets.ArchiveNumberingMode.Sequence,
				ArchiveAboveSize = 20000000
			};
			var logconsole = new NLog.Targets.ConsoleTarget("logconsole");
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logconsole, "Sparkplug");
			config.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, logfile, "Sparkplug");
			NLog.LogManager.Configuration = config;

			Logger.Info("Startup");

			// Start by reading settings, or saving them if there are none
			if (File.Exists(FileBaseName) )
			{
				try
				{
					StreamReader UpdateSetFile = new StreamReader(FileBaseName);
					string SettingsText = UpdateSetFile.ReadToEnd();
					Settings = JsonConvert.DeserializeObject<ProgramSettings>(SettingsText);
					UpdateSetFile.Close();
					Logger.Info("Read settings from file: " + FileBaseName + "\n" + SettingsText);
				}
				catch
				{
					Logger.Error("Unable to read settings file.");
					return;
				}
			}
			else
			{
				// Save current settings for a user to edit.
				// These are the defaults for this filter.
				var DefaultFilter = new List<string> { "analog", "algmanual", "digital", "binary", "accumulator" };
				Settings.ObjectClassFilter.AddRange( DefaultFilter);
				string SetString = JsonConvert.SerializeObject(Settings, Formatting.Indented);
				StreamWriter UpdateSetFile = new StreamWriter( FileBaseName);
				UpdateSetFile.WriteLine(SetString);
				UpdateSetFile.Close();
				Logger.Info("Wrote settings file");
			}


			// Good practice means storing credentials with reversible encryption, not adding them to code or using command parameters.
			// This implementation allows a first use in command line parameters, which it then stores encrypted in a file.
			// So you can run once with parameters and then run subsequent times without.

			// If we have command line arguments, write Username and Passwords out in locally encrypted files
			if (args.Length >= 2 ) 
			{
				// Write Geo SCADA Credentials
				if (!DataFeederSparkplug.UserCredStore.FileWriteCredentials(GSCredentialsFile, args[0], args[1]))
				{
					Logger.Error("Cannot write Geo SCADA credentials.");
					return;
				}
			}
			if (args.Length >= 4)
			{
				// Write MQTT Credentials
				if (!DataFeederSparkplug.UserCredStore.FileWriteCredentials(MQTTCredentialsFile, args[2], args[3]))
				{
					Logger.Error("Cannot write MQTT credentials.");
					return;
				}
			}

			// For writing data, we store/maintain a store of metrics in Sparkplug Payload format
			// One store is of birth metric info, the other is of data items for metrics
			ClearMetricBirth();
			ClearMetric();

			// Read file of last update times
			UpdateTimeList = ReadUpdateTimeList(FileBaseName);

			// Stats during the data feed:
			long UpdateCount = 0;
			DateTimeOffset StartTime = DateTimeOffset.UtcNow;
			DateTimeOffset FlushUpdateFileTime = DateTimeOffset.UtcNow;
			double ProcTime = 0;
			int LongestQueue = 0;
			int LastQueue = 0;

			// Main Loop
			do
			{
				if (!GeoSCADAConnectionState)
				{
					// Attempt to make Geo SCADA Connection every 30 seconds - you could customise this
					if ((DateTimeOffset.UtcNow - GeoSCADALastConnectionTry).TotalSeconds > 30)
					{
						FeederContinue = true; // Set to false by a Geo SCADA shutdown event/callback
						GeoSCADAConnectionState = ConnectToGeoSCADA();
						GeoSCADALastConnectionTry = DateTimeOffset.UtcNow;
						if (!GeoSCADAConnectionState)
						{
							// Failed, so flip the connection for next time
							GeoSCADABackupServerActive = !GeoSCADABackupServerActive;
						}
					}
				}
				else
				{
					// Check time and cause processing/export
					DateTimeOffset ProcessStartTime = DateTimeOffset.UtcNow;

					UpdateCount += Feeder.ProcessUpdates(); // Keep calling to pull data out. It returns after one second of process time. Adjust as needed.

					ProcTime = (DateTimeOffset.UtcNow - ProcessStartTime).TotalMilliseconds;

					// Flush UpdateTimeList file every minute, saving the progress of data received
					// [This could be synced partially with sending of data to Sparkplug]
					// [If Sparkplug is down and we still have data, you could persist data to memory in case of a shutdown]
					// {Also only write this if we're sending Data messages, not if only sending Birth messages}
					if ((FlushUpdateFileTime.AddSeconds(60) < DateTimeOffset.UtcNow) && !Settings.SendBirthMessagesOnly)
					{
						Logger.Info("Flush UpdateTime File - start...");
						if (! WriteUpdateTimeList(FileBaseName, UpdateTimeList) )
						{
							Logger.Error("Error writing the update time list, stopping.");
							break;
						}
						FlushUpdateFileTime = DateTimeOffset.UtcNow;
						Logger.Info("End");

						// Also flush gathered metric data regularly
						FlushMetric();
					}

					// If the FeederContinue flag is cleared then there's a Geo SCADA stop or connection fail
					if (!FeederContinue && GeoSCADAConnectionState)
					{
						Logger.Info("Stop and Disconnect.");
						GeoSCADAConnectionState = false;
						try
						{
							AdvConnection.LogOff();
							AdvConnection.Dispose();
						}
						catch
						{
							Logger.Error("Exception disconnecting from Geo SCADA.");
						}
					}
				}
				await Task.Delay(1000); // You must pause to allow the database to serve other tasks. 
										// Consider the impact on the server, particularly if it is Main or Standby. 
										// A dedicated Permanent Standby could be used for this export.

				// Output stats
				Logger.Info($"Tot Updates: {UpdateCount} (Avg {(int)(UpdateCount / (DateTimeOffset.UtcNow - StartTime).TotalSeconds)}/s, GS Proc: {ProcTime}mS, Proc Q: {Feeder.ProcessQueueCount()}, Birth Q: {BirthQueue.Count}, Data Q: {DataQueue.Count}, Tot Pubs: {BatchCount}");

				// Check if we are falling behind - and recommend longer scan interval
				int PC = Feeder.ProcessQueueCount();
				if (PC > LongestQueue)
				{
					// Gone up, and previous count wasn't low
					if (LastQueue > 100)
					{
						Logger.Warn("*** Queue size increasing, queue not being processed quickly, consider increasing update time interval.");
					}
					LongestQueue = PC;
				}
				LastQueue = PC;

				// Attempt MQTT Connection
				if (!SparkplugConnectionState)
				{
					// Make MQTT Connection every 30 seconds - you could customise this
					if ((DateTimeOffset.UtcNow - SparkplugLastConnectionTry).TotalSeconds > 30)
					{
						SparkplugConnectionState = ConnectSparkplug();
						SparkplugLastConnectionTry = DateTimeOffset.UtcNow;
					}
				}
				else
				// If connected, send queued Sparkplug data to MQTT Server
				// Do this the loop after we first connect so if it's unsuccessful we don't send data before.
				{
					SendToMQTTServer();
				}
			} while (true);

			// Finish by writing time list
			WriteUpdateTimeList(FileBaseName, UpdateTimeList);
			// Flush remaining data.
			FlushMetric();
			SendToMQTTServer();
			// Close MQTT Connection
			Client.Disconnect();
		}

		public static bool ConnectToGeoSCADA()
		{
			// Handle server redundancy choice
			string GeoSCADAServer = Settings.GeoSCADAServerName;
			if (GeoSCADABackupServerActive)
			{
				GeoSCADAServer = Settings.GeoSCADABackupServerName;
			}
			Logger.Info("Using Geo SCADA Server: " + GeoSCADAServer);

			// If we can't read credentials, assume they are blank
			string User = "";
			string PasswordStr = "";
			var Password = new SecureString();
			if (DataFeederSparkplug.UserCredStore.FileReadCredentials(GSCredentialsFile, out User, out PasswordStr))
			{
				foreach (var c in PasswordStr)
				{
					Password.AppendChar(c);
				}
			}

			// Using these pragma enables version-independent Geo SCADA connection code
#pragma warning disable 612, 618
			try
			{
				node = new ServerNode(ConnectionType.Standard, Settings.GeoSCADAServerName, Settings.GeoSCADAServerPort);
				AdvConnection = node.Connect("SPFeeder");
			}
			catch
			{
				Logger.Error("Cannot connect to Geo SCADA Server: " + GeoSCADAServer);
				return false;
			}
#pragma warning restore 612, 618

			try
			{
				AdvConnection.LogOn(User, Password);
			}
			catch
			{
				Logger.Error("Cannot log on to Geo SCADA with user: " + User);
				return false;
			}
			Logger.Info("Logged on.");

			// Set up connection, read rate and the callback function/action for data processing
			if (!Feeder.Connect(AdvConnection,
					   true, // Update config on start, must be true for Sparkplug
					   Settings.CatchUpDataOnStart, // Read/catch up data on start
					   Settings.UpdateIntervalHisSec,
					   Settings.UpdateIntervalCurSec,
					   Settings.MaxDataAgeDays,
					   ProcessNewData,
					   ProcessNewConfig,
					   EngineShutdown,
					   FilterNewPoint))
			{
				Logger.Error("Not connected");
				return false;
			}
			Logger.Info("Connected to Feeder Engine.");

			var MyGroup = AdvConnection.FindObject(Settings.ExportGroup);

			AddAllPointsInGroup(MyGroup.Id, AdvConnection);

			Logger.Info("Total Points Watched: " + Feeder.SubscriptionCount().ToString());

			return true;
		}

		/// <summary>
		/// List recursively all point objects in all sub-groups
		/// Declared with async to include a delay letting other database tasks work
		/// </summary>
		/// <param name="group"></param>
		/// <param name="AdvConnection"></param>
		public static void AddAllPointsInGroup(ObjectId group, IServer AdvConnection)
		{
			// Add the two types of data value sources in this group
			var points = AdvConnection.ListObjects("CDBPoint", "", group, true);
			AddPoints(points, AdvConnection);
			var accumulators = AdvConnection.ListObjects("CAccumulatorBase", "", group, true);
			AddPoints(accumulators, AdvConnection);

			// Recurse into child groups
			var groups = AdvConnection.ListObjects("CGroup", "", group, true);
			foreach (var childgroup in groups)
			{
				AddAllPointsInGroup(childgroup.Id, AdvConnection);
				Task.Delay(1); // You must pause to allow the database to serve other tasks, especially if many points are being added. 
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
				// Only add points of type analog, counter and digital, matching the source folder too - you can customise this function
				if (FilterNewPoint(point))
				{
					// Reading and use the LastChange parameter from our persistent store.
					// This will ensure gap-free historic data.
					DateTimeOffset StartTime = DateTimeOffset.MinValue;
					if (UpdateTimeList.ContainsKey(point.Id))
					{
						StartTime = UpdateTimeList[point.Id];
						// Logger.Info("Add '" + point.FullName + "' from: " + StartTime.ToString() );
					}
					if (!Feeder.AddSubscription(point.FullName, StartTime))
					{
						Logger.Error("Error adding point. " + point.FullName);
					}
					else
					{
						int SubCount = Feeder.SubscriptionCount();
						if (SubCount % 5000 == 0)
						{
							Logger.Info("Points Watched: " + SubCount.ToString());
						}
					}
				}
			}
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
			if (NewObject.TemplateId == -1) 
			{
				bool found = false;
				foreach (var PartName in Settings.ObjectClassFilter)
				{
					if (NewObject.ClassName.ToLower().Contains( PartName.ToLower() ) ) 
					{
						found = true;
						break;
					}
				}
				if (found)
				{
					if (Settings.ExportGroup == "$Root")
					{
						return true;
					}
					if (NewObject.FullName.StartsWith(Settings.ExportGroup + "."))
					{
						return true;
					}
				}
			}
			return false;
		}

		// Two functions to read/write the state of how far we exported data
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
				Logger.Info("Read the list of point last update times from: " + Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList.csv");
			}
			return UpdateTimeList;
		}

		/// <summary>
		/// Write the CSV list of point names and update times
		/// </summary>
		/// <param name="FileBase">A filename - only the folder part is used to create our point/time tracking file</param>
		/// <param name="UpdateTimeList">Dictionary of points and their times</param>
		static bool WriteUpdateTimeList(string FileBase, Dictionary<int, DateTimeOffset> UpdateTimeList)
		{
			// If there is an error then return False, we should not run if this file can't be written
			try
			{
				// First write to a temporary file
				StreamWriter UpdateTimeListFile = new StreamWriter(Path.GetDirectoryName(FileBase) + "\\" + "UpdateTimeList new.csv");
				foreach (KeyValuePair<int, DateTimeOffset> entry in UpdateTimeList)
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
				return true;
			}
			catch (Exception e)
			{
				Logger.Error("Error writing update time file: " + e.Message);
			}
			return false;
		}

		// Connect to MQTT with Sparkplug payload
		static bool ConnectSparkplug()
		{
			// Handle server redundancy choice
			string MQTTServer = Settings.MQTTServerName;
			if (MQTTBackupServerActive)
			{
				MQTTServer = Settings.MQTTBackupServerName;
			}
			Logger.Info("Using MQTT Server: " + MQTTServer);

			// If we can't read credentials, assume they are blank
			string User = "";
			string PasswordStr = "";
			DataFeederSparkplug.UserCredStore.FileReadCredentials(MQTTCredentialsFile, out User, out PasswordStr);

			try
			{
				// This uses no MQTT server encryption, you are recommended to implement TLS connection security
				Client = new MqttClient(MQTTServer, Settings.MQTTServerPort, false, MqttSslProtocols.None, null, null);
				Client.ProtocolVersion = MqttProtocolVersion.Version_3_1_1;

				// Events
				Client.ConnectionClosed += Client_MqttConnectionClosed;
				Client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;

				// Create Death payload
				var Death = CreateDeathPayload();
				var DeathTopic = $"spBv1.0/{Settings.SpGroupId}/NDEATH/{Settings.SpNode}";

				// Create Death bytes as a string because the Connect method has no bytes payload for death
				// Experimentation has shown that this does not write the correct bytes, a fix is needed
				// (Though the real fix would be to have a Client.Connect function which accepts bytes in the death message).
				var DeathMessage = Death.ToByteArray();
				string DeathString = System.Text.Encoding.Default.GetString(DeathMessage);

				// Connect with username and password
				Client.Connect(Settings.MQTTClientName, User, PasswordStr, false, 1, true, DeathTopic, DeathString, true, 3600);

				// Subscribe to node control and the server state message - state format depends on Sparkplug version
				var SubTopics = new string[2];
				SubTopics[0] = $"spBv1.0/{Settings.SpGroupId}/NCMD/{Settings.SpNode}/#";
				if (Settings.SparkplugVersion == 2)
				{
					SubTopics[1] = $"STATE/{Settings.SCADAHostId}";
				}
				else
				{
					SubTopics[1] = $"spBv1.0/STATE/{Settings.SCADAHostId}";
				}
				//	   $"spBv1.0/{SpGroupId}/DCMD/{SpNode}/{SpDevice}/#" }; // Not publishing/subscribing for any Device
				byte[] SubQoS = { 1, 1 };
				Client.Subscribe(SubTopics, SubQoS);
			}
			catch (Exception ex)
			{
				Logger.Error("Cannot connect to MQTT " + ex.Message);
				// Flip next try to backup
				MQTTBackupServerActive = !MQTTBackupServerActive;
				return false;
			}
			return true;
		}


		static Payload CreateDeathPayload()
		{
			var Death = new Payload();
			Death.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			var bdSeqMetric = new Payload.Types.Metric();
			bdSeqMetric.Name = "bdSeq";
			bdSeqMetric.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			bdSeqMetric.Datatype = 8; // INT64
			bdSeqMetric.LongValue = ReadSparkplugState(FileBaseName, true); // This should increment from previous connections
			Logger.Info($"Added Birth bdseq={bdSeqMetric.LongValue}");
			Death.Metrics.Add(bdSeqMetric);
			return Death;
		}

		// A function for read/write of the Birth/Death Sequence #
		static ulong ReadSparkplugState(string FileBase, bool isBirth)
		{
			ulong bdSeq = 0;
			Directory.CreateDirectory(Path.GetDirectoryName(FileBase));
			if (File.Exists(Path.GetDirectoryName(FileBase) + "\\" + "SparkplugState.csv"))
			{
				StreamReader SpUpdateFile = new StreamReader(Path.GetDirectoryName(FileBase) + "\\" + "SparkplugState.csv");
				string line;
				line = SpUpdateFile.ReadLine();
				if (line != null)
				{
					if (ulong.TryParse(line, out bdSeq))
					{
						Logger.Info($"Read bdSeq: {bdSeq.ToString()} from {Path.GetDirectoryName(FileBase) + "\\" + "SparkplugState.csv"}");
					}
				}
				SpUpdateFile.Close();
			}
			if (isBirth)
			{
				// Increment for next time we run or create a new Birth message, so when next read for death, one was added
				StreamWriter SpUpdateFile = new StreamWriter(Path.GetDirectoryName(FileBase) + "\\" + "SparkplugState.csv");
				SpUpdateFile.WriteLine(bdSeq+1);
				SpUpdateFile.Close();
			}
			return bdSeq;
		}

		/// <summary>
		/// MQTT Sparkplug Functions, start with the callback for closing the connection
		/// </summary>
		/// <param name="sender">object</param>
		/// <param name="e">EventArgs</param>
		private static void Client_MqttConnectionClosed(object sender, System.EventArgs e)
		{
			Logger.Info("Callback to MqttConnectionClosed");
			// Pauses the export and retries connection
			SparkplugConnectionState = false;
		}

		/// <summary>
		/// Callback for receiving a message
		/// </summary>
		/// <param name="sender">object</param>
		/// <param name="e">MqttMsgPublishEventArgs</param>
		private static void Client_MqttMsgPublishReceived(object sender, uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e)
		{
			// handle message received 
			string t = e.Topic;

			Logger.Info("Publish Received from: " + sender.ToString() + " Topic: " + t + " Message Length: " + e.Message.Length);
			var tokens = t.Split('/');

			// Incoming server state - Sparkplug 2.2
			//$"STATE/{SCADAHostId}"
			if ((Settings.SparkplugVersion == 2) &&
					(tokens.Length == 2) &&
					(tokens[0] == "STATE") &&
					(tokens[1] == Settings.SCADAHostId) )
			{
				string m = System.Text.Encoding.UTF8.GetString(e.Message);
				Logger.Info("Received: " + m);
				// We will receive either "ONLINE" or "OFFLINE"
				if (m == "OFFLINE" && !Settings.SparkplugIgnoreServerState)
				{
					// Here we disconnect from MQTT, buffering data and wait
					Logger.Info("Server Offline, Disconnect from MQTT");
					Client.Disconnect();
					SparkplugConnectionState = false;
				}
				else
				{
					Logger.Info("Server Online");
				}
				return;
			}

			// Incoming server state - Sparkplug 3.0
			//$"spBv1.0/STATE/{SCADAHostId}"
			if ((Settings.SparkplugVersion == 3) &&
					(tokens.Length == 4) &&
					(tokens[0] == "spBv1.0") &&
					(tokens[1] == "STATE") &&
					(tokens[2] == Settings.SCADAHostId) )
			{
				string m = System.Text.Encoding.UTF8.GetString(e.Message);
				Logger.Info("Received: " + m);
				// We will receive JSON { "online": true|false, "timestamp":n }
				try
				{
					OnlineState serverstate = JsonConvert.DeserializeObject<OnlineState>(m);
					if (!serverstate.online && !Settings.SparkplugIgnoreServerState)
					{
						// Here we disconnect from MQTT, buffering data and wait
						Logger.Info("Server Offline, Disconnect from MQTT");
						Client.Disconnect();
						SparkplugConnectionState = false;
					}
					else
					{
						Logger.Info("Server Online");
					}
				}
				catch (Exception ex)
				{
					Logger.Error("Cannot interpret Sparkplug 3 server STATE message. " + ex.Message);
				}
				return;
			}

			// Incoming node control
			if (	tokens.Length == 4 &&
				(tokens[0] == "spBv1.0") && 
				(tokens[1] == Settings.SpGroupId) && 
				( (tokens[2] == "NCMD" ) || (tokens[2] == "DCMD") )&& 
				(tokens[3] == Settings.SpNode) )
			{
				Payload inboundPayload; // Parse SpB Protobuf into object structure
				try
				{
					inboundPayload = Payload.Parser.ParseFrom(e.Message);
				}
				catch (Exception ex)
				{
					Logger.Error("Error interpreting Data Payload. " + ex.Message);
					return;
				}
				foreach (var metric in inboundPayload.Metrics)
				{
					switch (metric.Name)
					{
						case "Node Control/Next Server":
							//# 'Node Control/Next Server' is an NCMD used to tell the device/client application to
							//# disconnect from the current MQTT server and connect to the next MQTT server in the
							//# list of available servers.  This is used for clients that have a pool of MQTT servers
							//# to connect to.
							Logger.Info("'Node Control/Next Server' received");
							// Flip next try to backup
							MQTTBackupServerActive = !MQTTBackupServerActive;
							// Force reconnection
							SparkplugConnectionState = false;
							break;
						case "Node Control/Rebirth":
							//# 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
							//# its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
							//# application if it receives an NDATA or DDATA with a metric that was not published in the
							//# original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
							//# its original NBIRTH and DBIRTH messages.
							Logger.Info("'Node Control/Rebirth' received");
							WriteMetricBirth();
							break;
						case "Node Control/Reboot":
							//# 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
							//# This can be used for devices that need a full application reset via a soft reboot.
							//# In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
							//# messages.
							Logger.Info("'Node Control/Reboot' received");
							var Death = CreateDeathPayload();
							var DeathTopic = $"spBv1.0/{Settings.SpGroupId}/NDEATH/{Settings.SpNode}";
							Client.Publish(DeathTopic, Death.ToByteArray(), 1, false);
							WriteMetricBirth();
							break;
						default:
							Logger.Error("Unknown Metric Command");
							break;
					}
				}
				return;
			}
			else
			{
				Logger.Error("Unknown Topic");
			}
		}

		private static void SendToMQTTServer()
		{
			// Dequeue all Birth to MQTT
			while (BirthQueue.Count > 0)
			{
				if (BirthQueue.TryPeek(out Payload bupdate))
				{
					try
					{
						bupdate.Seq = Seq;
						Seq++;
						if (Seq == 256) Seq = 0;
						// Using QoS level 1 - receive at least once
						string topic = $"spBv1.0/{Settings.SpGroupId}/NBIRTH/{Settings.SpNode}";
						Client.Publish(topic, bupdate.ToByteArray(), 1, false);
						BatchCount++;
						//if (BatchCount % 100 == 0)
						//Logger.Info(bupdate.ToString());
						Logger.Info("Published Birth: " + BatchCount.ToString() + " times, last send: " + bupdate.Metrics.Count + " metrics");
						// Discard successfully sent data
						BirthQueue.TryDequeue(out bupdate);
					}
					catch (Exception e)
					{
						Logger.Error("*** Error sending birth message: " + e.Message);
					}
				}
			}

			// Dequeue all data to MQTT
			while (DataQueue.Count > 0 && !Settings.SendBirthMessagesOnly)
			{
				if (DataQueue.TryPeek(out Payload update))
				{
					try
					{
						update.Seq = Seq;
						Seq++;
						if (Seq == 256) Seq = 0;
						// Using QoS level 0
						string topic = $"spBv1.0/{Settings.SpGroupId}/NDATA/{Settings.SpNode}";
						Client.Publish(topic, update.ToByteArray(), 1, false);
						BatchCount++;
						if (BatchCount % 100 == 0)
						{
							//Logger.Info(update.ToString());
							Logger.Info("Published Data: " + BatchCount.ToString() + " times, last send: " + update.Metrics.Count + " metrics");
						}
						// Discard successfully sent data
						DataQueue.TryDequeue(out update);
					}
					catch (Exception e)
					{
						Logger.Error("*** Error sending data message: " + e.Message);
					}
				}
			}
		}

		// For Sparkplug we have a payload collection, appended for new data 
		// A Birth payload defines metric/properties
		// A collection of Data payloads define metric values

		public static void ClearMetric()
		{
			DataMessage = new Payload();
		}

		// Called on a minute timer in the main loop, as well as when a batch has been accumulated
		public static void FlushMetric()
		{
			// If data exists to export
			if (DataMessage.Metrics.Count > 0)
			{
				// Add timestamp
				DataMessage.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

				DataQueue.Enqueue(DataMessage);

				Logger.Info($"Queued data: {DataMessage.Metrics.Count} metrics, {DataQueue.Count} total in queue.");

				// Start gathering next
				ClearMetric();
			}
		}

		public static void WriteMetric(Payload.Types.Metric Out)
		{
			// We are about to send data, ensure Birth message is sent first
			FlushMetricBirth();

			DataMessage.Metrics.Add(Out);
			// Create a new metric structure after 100 data messages (You should tune this for your setup)
			if (DataMessage.Metrics.Count > 100)
			{
				FlushMetric();
			}
		}

		// Only called once at the start - we accumulate new Birth metric entries but do not remove old entries
		public static void ClearMetricBirth()
		{
			BirthMessage = new Payload();
			BirthMessageChanged = false;
			BirthMessage.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

			// Add BD Sequence (same as Death number)
			var bdSeqMetric = new Payload.Types.Metric();
			bdSeqMetric.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(); 
			bdSeqMetric.Name = "bdSeq";
			bdSeqMetric.Datatype = 8; // INT64
			bdSeqMetric.LongValue = ReadSparkplugState(FileBaseName, false); // This should increment from previous connections
			BirthMessage.Metrics.Add(bdSeqMetric);
			Logger.Info($"Added Birth bdseq={bdSeqMetric.LongValue}");

			// Add Node Control/Rebirth Metric
			var RebirthMetric = new Payload.Types.Metric();
			RebirthMetric.Name = "Node Control/Rebirth";
			RebirthMetric.Datatype = 11; // Boolean
			RebirthMetric.BooleanValue = false;
			BirthMessage.Metrics.Add(RebirthMetric);
		}

		public static void FlushMetricBirth()
		{
			// If data exists to export
			if (BirthMessage.Metrics.Count > 0 && BirthMessageChanged)
			{
				WriteMetricBirth();
			}
		}

		// Output the birth message to the write queue. This is called when we have read/updated metrics or when we are asked for rebirth
		public static void WriteMetricBirth()
		{
			// Add timestamp
			BirthMessage.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

			BirthQueue.Enqueue(BirthMessage);
			BirthMessageChanged = false;
			Logger.Info($"Queued birth: {BirthMessage.Metrics.Count} metrics, {BirthQueue.Count} total in queue.");
		}

		public static void WriteMetricBirth(Payload.Types.Metric Out)
		{
			// Remove old entry for this metric, if it exists
			// This is not efficient for a large system - consider an alternative implementation
			foreach( var Metric in BirthMessage.Metrics)
			{
				if (Metric.Name == Out.Name)
				{
					BirthMessage.Metrics.Remove(Metric);
					break;
				}
			}
			BirthMessage.Metrics.Add(Out);
			BirthMessageChanged = true;
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
				PointName = PointName, // Not used by Sparkplug, we use the point id as the alias
				Value = Value,
				Timestamp = Timestamp, // For your application, periodically buffer this to use as the LastChange parameter on restarting.
									   // This enables gap-free data historic data.
				OPCQuality = ((int)(Quality) & 255),     // Not supported by Sparkplug
				ExtendedQuality = ((int)(Quality) / 256) // Not supported by Sparkplug
			};

			var Out = new Payload.Types.Metric();
			Out.Alias = (ulong)DataUpdate.PointId;
			Out.Datatype = 10; // Double
			Out.DoubleValue = DataUpdate.Value;
			Out.Timestamp = (ulong)DataUpdate.Timestamp.ToUnixTimeMilliseconds();
			Out.IsHistorical = false; // Always send as real-time values (DataUpdate.UpdateType == "His");

			WriteMetric(Out);

			// Update id and Date in the list
			if (UpdateTimeList.ContainsKey(Id))
			{
				UpdateTimeList[Id] = Timestamp;
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
			//  if the Connect method has 'UpdateConfigurationOnStart' parameter set (but it is required for Sparkplug).

			// This function could be restructured to be more table-driven.

			// Get Point properties, these will depend on type
			object[] PointProperties = { "", 0.0, 0.0, "", 0.0, 0.0, 0.0, "", "" };
			PointProperties = AdvConnection.GetObjectFields(PointName, 
				new string[] {	"TypeName",				// 0
								"FullScale",			// 1
								"ZeroScale",			// 2
								"Units",				// 3
								"BitCount",				// 4
								"GISLocation.Latitude", // 5
								"GISLocation.Longitude",// 6
								"State0Desc",			// 7
								"State1Desc" });		// 8
			var ConfigUpdate = new ConfigChange();
			try
			{
				ConfigUpdate.PointId = Id;
				ConfigUpdate.UpdateType = UpdateType;
				ConfigUpdate.PointName = PointName;
				ConfigUpdate.Timestamp = DateTimeOffset.UtcNow;
				ConfigUpdate.ClassName = (string)PointProperties[0];
				ConfigUpdate.Datatype = 0;
				// Imprecise way to pick floating point types, can be improved by looking up type names
				if (PointProperties[0].ToString().Contains("Analog") || 
					PointProperties[0].ToString().Contains("Alg") || 
					PointProperties[0].ToString().Contains("Counter"))
				{
					ConfigUpdate.Datatype = 10; // Double
					ConfigUpdate.Units = (string)(PointProperties[3] ?? "");
					ConfigUpdate.FullScale = (double)(PointProperties[1] ?? (double)0);
					ConfigUpdate.ZeroScale = (double)(PointProperties[2] ?? (double)0);
				}
				else
				{
					ConfigUpdate.BitCount = Convert.ToInt32((PointProperties[4]) ?? 0);
					if (ConfigUpdate.BitCount == 1)
					{
						// We can't force the Geo SCADA Driver to create a 2 or 3 bit digital
						ConfigUpdate.Datatype = 11; // Boolean
						ConfigUpdate.State0Desc = (string)(PointProperties[7] ?? ""); ;
						ConfigUpdate.State1Desc = (string)(PointProperties[8] ?? ""); ;
					}
					else
					{
						ConfigUpdate.Datatype = 6; // UInt16
					}
				}
				ConfigUpdate.Latitude = (double)(PointProperties[5] ?? (double)0);
				ConfigUpdate.Longitude = (double)(PointProperties[6] ?? (double)0);
			}
			catch (Exception e)
			{
				Logger.Error("Error reading properties for configuration: " + e.Message);
			}
			string json = JsonConvert.SerializeObject(ConfigUpdate);

			var Out = new Payload.Types.Metric();
			// Filter out from the point name using ExportGroupLevelTrim
			var GeoSCADANameParts = ConfigUpdate.PointName.Split('.');
			var SparkplugName = "";
			for (int i = Settings.ExportGroupLevelTrim; i < GeoSCADANameParts.Length; i++)
			{
				// Also change Geo SCADA to Sparkplug naming convention
				// Substitute any / found with \
				SparkplugName += GeoSCADANameParts[i].Replace('/', '\\') + "/";
			}
			// Trim the extra '/'
			Out.Name = SparkplugName.Substring(0, SparkplugName.Length-1);
			// We use the Geo SCADA row Id as the unique Sparkplug Alias
			Out.Alias = (ulong)ConfigUpdate.PointId;
			Out.Datatype = ConfigUpdate.Datatype;
			//Out.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			Out.IsNull = true; // We are not sending any data in the Birth message

			// Create/add metric properties
			var PSKeys = new List<string>();
			var PSValues = new List<Payload.Types.PropertyValue>();

			// Units
			if ((ConfigUpdate.Units ?? "") != "")
			{
				PSKeys.Add("Units");
				var PSValueUnits = new Payload.Types.PropertyValue();
				PSValueUnits.Type = 12; //String
				PSValueUnits.StringValue = ConfigUpdate.Units;
				PSValues.Add(PSValueUnits);
			}
			// Scale
			if (ConfigUpdate.FullScale != 0)
			{
				PSKeys.Add("FullScale");
				var PSValueFullScale = new Payload.Types.PropertyValue();
				PSValueFullScale.Type = 10; //Double
				PSValueFullScale.DoubleValue = ConfigUpdate.FullScale;
				PSValues.Add(PSValueFullScale);
			}
			if (ConfigUpdate.ZeroScale != 0)
			{
				PSKeys.Add("ZeroScale");
				var PSValueZeroScale = new Payload.Types.PropertyValue();
				PSValueZeroScale.Type = 10; //Double
				PSValueZeroScale.DoubleValue = ConfigUpdate.ZeroScale;
				PSValues.Add(PSValueZeroScale);
			}
			// Lat and Long
			if (ConfigUpdate.Latitude != 0)
			{
				PSKeys.Add("Latitude");
				var PSValueLatitude = new Payload.Types.PropertyValue();
				PSValueLatitude.Type = 10; //Double
				PSValueLatitude.DoubleValue = ConfigUpdate.Latitude;
				PSValues.Add(PSValueLatitude);
			}
			if (ConfigUpdate.Longitude != 0)
			{
				PSKeys.Add("Longitude");
				var PSValueLongitude = new Payload.Types.PropertyValue();
				PSValueLongitude.Type = 10; //Double
				PSValueLongitude.DoubleValue = ConfigUpdate.Latitude;
				PSValues.Add(PSValueLongitude);
			}
			// Digital state names
			if ((ConfigUpdate.State0Desc ?? "") != "")
			{
				PSKeys.Add("State0Desc");
				var PSValueState0Desc = new Payload.Types.PropertyValue();
				PSValueState0Desc.Type = 12; //String
				PSValueState0Desc.StringValue = ConfigUpdate.State0Desc;
				PSValues.Add(PSValueState0Desc);
			}
			if ((ConfigUpdate.State1Desc ?? "") != "")
			{
				PSKeys.Add("State1Desc");
				var PSValueState1Desc = new Payload.Types.PropertyValue();
				PSValueState1Desc.Type = 12; //String
				PSValueState1Desc.StringValue = ConfigUpdate.State1Desc;
				PSValues.Add(PSValueState1Desc);
			}

			// Add property set to birth metric
			if (PSKeys.Count > 0)
			{
				Out.Properties = new Payload.Types.PropertySet();
				Out.Properties.Keys.Add(PSKeys);
				Out.Properties.Values.Add(PSValues);
			}
			WriteMetricBirth(Out);
		}

		/// <summary>
		/// Called when server state changes from Main or Standby, this stops all Geo SCADA monitoring
		/// </summary>
		public static void EngineShutdown()
		{
			Logger.Info("Engine Shutdown");
			FeederContinue = false;
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
		public string State0Desc;
		public string State1Desc;
		public uint Datatype; // The Sparkplug type number
	}

	// Online state structure for Sparkplug 3 online state
	// We will receive JSON { "online": true|false, "timestamp":n }
	public class OnlineState
	{
		public bool online;
		public ulong timestamp;
	}

}
