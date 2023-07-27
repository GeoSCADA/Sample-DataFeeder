// This is a generic Geo SCADA Historic Data Feeder Service.
// It can be customised by altering the settings below (See comment 'SPECIFIC TO' ) and
// replace the file Export<Sparkplug or something else>.cs with your own private/public
// methods, keeping to the public interface.
//
// This is a Windows Service program. You can run/test it without installing as a service.
//
// To install the service, run CMD as Admin, go to the compiled output folder and enter: 
//  %windir%\Microsoft.Net\Framework64\v4.0.30319\installutil DataFeederService.exe
// (Note: if you have run the program without installing as a service, installation may be
//  blocked because the event log Source already exists. First go to the Event Viewer and
//  right-click DataFeederLog in apps & services logs, then Clear it.)
//
// You could use WIX to create an installer msi which does this.
// The service optionally takes a command parameter - the filename of the setup file.

using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;
using System.Security;
using System.Diagnostics;
using System.ServiceProcess;
using System.Timers;
using System.Linq; // For Dict.Keys.ToArray()
using ClearScada.Client; // Find ClearSCADA.Client.dll in the c:\Program Files\Schneider Electric\ClearSCADA folder
using ClearScada.Client.Advanced;

// Bring in with nuget
using Newtonsoft.Json; 
// Module from this solution
using FeederEngine;

// App for the FeederEngine and PointInfo classes (added from the DataFeeder Project)
// It can easily be modified to change settings for the output.

namespace DataFeederService
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

		// Specify the group containing points for export.
		// Either for all using $Root, or a specified group name such as "MyGroup" or "East.Plant A"
		// Gentle reminder - only watch and get what you need. Any extra is a waste of performance.
		// This example "Demo Items.Historic Demo" will export all point data in this group and any subgroups
		public string ExportGroup = "Example Projects";

		// Exported item/Metric names will be created from point Full Names. To remove levels before sending, set this parameter
		// For example, when set to 1 with ExportGroup = "Demo Items.Historic Demo", the "Demo Items." name will be removed
		// To export full names set this to 0
		public int ExportGroupLevelTrim = 1;

		// List of string filters on the Geo SCADA ClassName of points/objects to be exported
		// New points for export will have their class name compared with this list
		// A default set of filters is found in Line 319 approx: var DefaultFilter = new List<string> ...
		public List<string> ObjectClassFilter = new List<string>();

		// Specify a boolean database field which, when true, will enable data export for the point/accumulator
		// Typically you would leave this setting blank if all points in the group ExportGroup are to be exported
		// (Filtered by the type of object). It's expected that this will be a metadata Boolean field.
		public string ExportIfThisFieldIsTrue = "";

		// Configuration properties to be sent in the birth/configuration messages
		// This is the filename in the folder C:\ProgramData\Schneider Electric\SpDataFeeder
		// For Sparkplug, you can use this file in the Geo SCADA configuration for the EoN Node in the destination database.
		//   If you add properties to this file they will be read from Geo SCADA configuration in the source
		//   database and can be automatically configured in the destination.
		// A default set of properties will be created and added to this file if is does not exist.
		public string ConfigPropertyFile = @"c:\ProgramData\Schneider Electric\SpDataFeeder\PropertyTranslationTable.txt";

		// These credential variables are only used on a first run, then they are deleted from the JSON file
		// To set and encrypt credentials, enter these to the JSON file, run the service,
		// then check that the credentials CSV files are created, then remove these from the JSON file.
		public string GSUser = "";
		public string GSPassword = "";
		public string TargetUser = "";
		public string TargetPassword = "";
		// credentials are encrypted and stored in these files:
		public string GSCredentialsFile = "";
		public string TargetCredentialsFile = "";

		// ************************************************************************************************************
		// SPECIFIC TO SPARKPLUG
		// ************************************************************************************************************
		// Security
		// The above TargetUser and TargetPassword strings are the MQTT Server user and password

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

		// Set the host's ID here so we can know it's STATE
		// You need to configure your host's ID here
		// Right-click your Geo SCADA Broker and select View Status to see it
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

		// ************************************************************************************************************
	}

	public partial class DataFeederService : ServiceBase
	{
		// Data file location for settings, output of date/time file of export progress, Sparkplug bdSeq etc. 
		// If you want to run another instance of this on the same PC, this file path needs to be different.
		// You can supply an alternative to this filename as a parameter to the Service executable.
		public static string FileBaseName = @"c:\ProgramData\Schneider Electric\SpDataFeeder\Settings.json";

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

		// last update times list
		private static Dictionary<int, DateTimeOffset> UpdateTimeList;

		// Global node and Geo SCADA server connection -- using ClearScada.Client.Advanced;
		private static ServerNode node;
		private static IServer AdvConnection;

		// Logging setup with NLog - got from NuGet - match same version as Geo SCADA
		private static readonly NLog.Logger Logger = NLog.LogManager.GetLogger("DataFeeder");

		// Property Translation List
		// This is read from a file in Geo SCADA Sparkplug Driver format:
		//  <Sparkplug Property Name> <tab> <Geo SCADA Table Name> <tab> <Geo SCADA Column Name>
		// This program indexes it by <Geo SCADA Table Name> <tab> <Geo SCADA Column Name>
		private static Dictionary<string, string> PropertyTranslations = new Dictionary<string, string>();

		internal void TestStartupAndStop(string[] args)
		{
			this.OnStart(args);
			Console.WriteLine("Interactive, press q<enter> to quit.");
			// When not run as a Service, the input line, if valid JSON, replaces the Settings file
			// This enables a non-Admin to run the program and change settings.
			// Please protect the executable and settings file with suitable ACLs.
			string SetString = "";
			string input = "";
			do
			{
				input = Console.ReadLine();
				SetString = SetString + "\n" + input;
			} while (input.Trim() != "q" && input.Trim() != "}");
			if (SetString.Length > 100)
			{
				try
				{
					Settings = JsonConvert.DeserializeObject<ProgramSettings>(SetString);
					StreamWriter UpdateSetFile = new StreamWriter(FileBaseName);
					UpdateSetFile.WriteLine(SetString);
					UpdateSetFile.Close();
					Logger.Info("Wrote settings file. " + SetString);
				}
				catch
				{
					Console.WriteLine("Not valid input or cannot write Settings file.");
				}
			}
			this.OnStop();
		}

		private int eventId = 1;
		public DataFeederService()
		{
			InitializeComponent();
			if (!Environment.UserInteractive)
			{
				eventLog1 = this.EventLog;
			}
		}

		// Globals used by OnStart, OnTimer
		Timer timer;
		// Stats during the data feed:
		DateTimeOffset StartTime = DateTimeOffset.UtcNow;
		DateTimeOffset FlushUpdateFileTime = DateTimeOffset.UtcNow;
		double ProcTime = 0;
		int LongestQueue = 0;
		int LastQueue = 0;

		protected override void OnStart(string[] args)
		{
			if (args.Length >= 1)
			{
				if (!Environment.UserInteractive)
				{
					eventLog1.WriteEntry("Filename Argument: " + args[0], EventLogEntryType.Information, eventId++);
				}
				// Argument is the location of the configuration file. Different feeders should have different folder paths.
				FileBaseName = args[0];
			}

			// Set up logging with NLOG
			var config = new NLog.Config.LoggingConfiguration();
			var logfile = new NLog.Targets.FileTarget("logfile")
			{
				FileName = Path.GetDirectoryName(FileBaseName) + "\\Log\\" + "DataFeeder.log",
				MaxArchiveFiles = 10,
				ArchiveNumbering = NLog.Targets.ArchiveNumberingMode.Sequence,
				ArchiveAboveSize = 20000000
			};
			var logconsole = new NLog.Targets.ConsoleTarget("logconsole");
			if (Environment.UserInteractive)
			{
				config.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, logconsole, "DataFeeder");
			}
			config.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, logfile, "DataFeeder");
			NLog.LogManager.Configuration = config;

			Logger.Info("Startup");

			// Tell the export about the file path
			ExportToTarget.WorkingPath = Path.GetDirectoryName(FileBaseName);

			// Start by reading settings, or saving them if there are none
			if (File.Exists(FileBaseName))
			{
				try
				{
					StreamReader UpdateSetFile = new StreamReader(FileBaseName);
					string SettingsText = UpdateSetFile.ReadToEnd();
					Settings = JsonConvert.DeserializeObject<ProgramSettings>(SettingsText);
					UpdateSetFile.Close();
					Logger.Info("Read settings from file: " + FileBaseName + "\n" + SettingsText);
					if (!Environment.UserInteractive)
					{
						eventLog1.WriteEntry("Read settings from file: " + FileBaseName + "\n" + SettingsText, EventLogEntryType.Information, eventId++);
					}
				}
				catch
				{
					Logger.Error("Unable to read settings file.");
					if (!Environment.UserInteractive)
					{
						eventLog1.WriteEntry("Unable to read settings file.", EventLogEntryType.Information, eventId++);
					}
					return;
				}
				// Good practice means storing credentials with reversible encryption, not adding them to code or using command parameters.
				// This implementation allows a first use in the settings file, which it then deletes and stores encrypted in a file.
				// So you can run once with this data and then run subsequent times without.
				// Filenames are in the settings file, or default them if blank
				// Credentials files - these are encrypted and saved from the initial ones in the settings file, see above.
				bool updateSettingsFile = false;
				if (Settings.GSCredentialsFile == "")
				{
					Settings.GSCredentialsFile = Path.GetDirectoryName(FileBaseName) + "\\" + "Credentials.csv";
					updateSettingsFile = true;
				}
				if (Settings.TargetCredentialsFile == "")
				{
					Settings.TargetCredentialsFile = Path.GetDirectoryName(FileBaseName) + "\\" + ExportToTarget.ExportName + "Credentials.csv";
					updateSettingsFile = true;
				}
				if (Settings.GSUser != "")
				{
					// Write Geo SCADA Credentials
					if (!UserCredStore.FileWriteCredentials(Settings.GSCredentialsFile, Settings.GSUser, Settings.GSPassword))
					{
						Logger.Error("Cannot write Geo SCADA credentials.");
						return;
					}
					updateSettingsFile = true;
				}
				if (Settings.TargetUser != "")
				{
					// Write Target Credentials
					if (!UserCredStore.FileWriteCredentials(Settings.TargetCredentialsFile, Settings.TargetUser, Settings.TargetPassword))
					{
						Logger.Error("Cannot write Target credentials.");
						return;
					}
					updateSettingsFile = true;
				}
				// Adding new property
				if (Settings.ConfigPropertyFile == "")
				{
					Settings.ConfigPropertyFile = Path.GetDirectoryName(FileBaseName) + "\\" + "PropertyTranslationTable.txt";
					updateSettingsFile = true;
				}
				// Write out the settings file if anything changed
				if (updateSettingsFile)
				{
					// Write settings back out - without credentials
					Settings.GSUser = "";
					Settings.GSPassword = "";
					Settings.TargetUser = "";
					Settings.TargetPassword = "";
					string SetString = JsonConvert.SerializeObject(Settings, Formatting.Indented);
					StreamWriter UpdateSetFile = new StreamWriter(FileBaseName);
					UpdateSetFile.WriteLine(SetString);
					UpdateSetFile.Close();
					Logger.Info("Wrote default settings file without credentials.");
				}
			}
			else
			{
				// Save current settings for a user to edit.
				// These are the defaults for this filter.
				var DefaultFilter = new List<string> { "analog", "algmanual", "digital", "binary", "accumulator" };
				Settings.ObjectClassFilter.AddRange(DefaultFilter);
				string SetString = JsonConvert.SerializeObject(Settings, Formatting.Indented);
				StreamWriter UpdateSetFile = new StreamWriter(FileBaseName);
				UpdateSetFile.WriteLine(SetString);
				UpdateSetFile.Close();
				Logger.Info("Wrote default settings file");
			}
			// Advise the export class
			ExportToTarget.Settings = Settings;

			// Read the Property Translation Table data so that exports can know the fields to be written to the Birth Messages
			string ConfigPropertyFileName = Settings.ConfigPropertyFile;
			string PropertyTranslation = "";
			try
			{
				StreamReader UpdateSetFile = new StreamReader(ConfigPropertyFileName);
				PropertyTranslation = UpdateSetFile.ReadToEnd();
				UpdateSetFile.Close();
				Logger.Info("Read property translation settings from file OK: " + ConfigPropertyFileName);
			}
			catch
			{
				Logger.Error("Unable to read property translation settings from file: " + ConfigPropertyFileName);
				// Try to write out a default file (this may be inappropriate for your export target)
				PropertyTranslation = "Units	CSparkplugBPointAnalog	Units\n" +
									"FullScale CSparkplugBPointAnalog FullScale\n" +
									"ZeroScale   CSparkplugBPointAnalog ZeroScale\n" +
									"BitCount CSparkplugBPointDigital BitCount\n" +
									"State0Desc CSparkplugBPointDigital State0Desc\n" +
									"State1Desc  CSparkplugBPointDigital State1Desc\n" +
									// Note that Geo SCADA Sparkplug Driver ignores these target fields currently
									"GISLocation.Longitude CSparkplugBPoint    CGISLocationSrcStatic.Longitude\n" +
									"GISLocation.Latitude CSparkplugBPoint    CGISLocationSrcStatic.Latitude\n";
				StreamWriter UpdatePTTFile = new StreamWriter(ConfigPropertyFileName);
				UpdatePTTFile.WriteLine(PropertyTranslation);
				UpdatePTTFile.Close();
				Logger.Info("Wrote default Property Translation Table file");
			}
			// Store Property Translations in a dictionary
			foreach( string entry in PropertyTranslation.Split('\n'))
			{
				string[] elements = entry.Trim().Split('\t');
				if (elements.Length == 3 && elements[0].Length > 1 && elements[1].Length > 1 && elements[2].Length > 1)
				{
					// Ignoring the table name - we retrieve regardless of type and ignore any null fields
					PropertyTranslations.Add(elements[2], elements[0]);
					Logger.Info("Adding Property Translation Table entry: " + entry);
				}
				else
				{
					if (entry.Trim() != "")
					{
						Logger.Info("Property Translation Table file entry invalid: " + entry);
					}
				}
			}
			// Always add TypeName for our purposes
			if (!PropertyTranslations.ContainsKey("TypeName"))
			{
				PropertyTranslations.Add("TypeName", "TypeName");
			}
			// Advise the export class
			ExportToTarget.PropertyTranslations = PropertyTranslations;

			// Read file of last update times so we can recover missing data if the service was not running
			UpdateTimeList = ReadUpdateTimeList(FileBaseName);

			// Stats during the data feed:
			UpdateCount = 0;
			StartTime = DateTimeOffset.UtcNow;
			FlushUpdateFileTime = DateTimeOffset.UtcNow;
			ProcTime = 0;
			LongestQueue = 0;
			LastQueue = 0;

			// Set up a timer that triggers every second.
			timer = new Timer();
			timer.Interval = 1000; // 1 second
			timer.AutoReset = false;
			timer.Elapsed += new ElapsedEventHandler(this.OnTimer);
			timer.Start();
		}


		public void OnTimer(object sender, ElapsedEventArgs args)
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
					if (!WriteUpdateTimeList(FileBaseName, UpdateTimeList))
					{
						Logger.Error("Error writing the update time list, stopping.");
						return;
					}
					FlushUpdateFileTime = DateTimeOffset.UtcNow;
					Logger.Info("End");

					// Also flush gathered metric data regularly
					ExportToTarget.FlushPointData();
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

			// Output stats
			Logger.Info($"Point Updates: {UpdateCount} (Avg {(int)(UpdateCount / (DateTimeOffset.UtcNow - StartTime).TotalMinutes)}/m, " + 
						$"DB Time: {ProcTime}mS, Update Q: {Feeder.ProcessQueueCount()}, " + 
						$"Config Q: {ExportToTarget.ConfigQueueCount}, Data Q: {ExportToTarget.DataQueueCount}, Tot Sends: {ExportToTarget.ExportCount}");

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

			// Attempt Target Connection
			if (!ExportToTarget.TargetConnectionState)
			{
				ExportToTarget.RetryTargetConnection();
			}
			else
			// If connected, send queued data to Target Server
			// Do this the loop after we first connect so if it's unsuccessful we don't send data before.
			{
				ExportToTarget.SendToTargetServer();
			}

			// Reinstate timer
			timer.Start();
		}


		protected override void OnStop()
		{
			Logger.Info("OnStop called.");
			timer.Stop();

			// Finish by writing time list
			WriteUpdateTimeList(FileBaseName, UpdateTimeList);
			// Flush remaining data - doing this twice to attempt to write data to Target.
			ExportToTarget.FlushPointData();
			if (ExportToTarget.TargetConnectionState)
			{
				ExportToTarget.SendToTargetServer();
			}
			if (ExportToTarget.ConfigQueueCount > 0 || ExportToTarget.DataQueueCount > 0)
			{
				Logger.Info($"Queued {ExportToTarget.ConfigQueueCount} Birth and {ExportToTarget.DataQueueCount} Data Messages will be lost.");
			}
			else
			{
				Logger.Info("All data sucessfully sent to the Target.");
			}
			// Close Target Connection
			ExportToTarget.Disconnect();

			Logger.Info("OnStop end.");
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
			if (UserCredStore.FileReadCredentials(Settings.GSCredentialsFile, out User, out PasswordStr))
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
				AdvConnection = node.Connect("DataFeeder");
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
			bool found = false;

			// Exclude Template items
			if (NewObject.TemplateId == -1)
			{
				// Include items matching ClassName filter
				foreach (var PartName in Settings.ObjectClassFilter)
				{
					if (NewObject.ClassName.ToLower().Contains(PartName.ToLower()))
					{
						found = true;
						break;
					}
				}
				if (found)
				{
					found = false;
					// Include items in the correct group heirarchy
					if (Settings.ExportGroup == "$Root")
					{
						found = true;
					}
					if (NewObject.FullName.StartsWith(Settings.ExportGroup + "."))
					{
						found = true;
					}
					// Include all if the settings does not specify ExportIfThisFieldIsTrue
					if (Settings.ExportIfThisFieldIsTrue != "")
					{
						// Get the property value for ExportIfThisFieldIsTrue
						object[] ExportBooleanProperty = new object[1];
						try
						{
							ExportBooleanProperty = AdvConnection.GetObjectFields(NewObject.FullName, 
														new string[] { Settings.ExportIfThisFieldIsTrue });
							found = (bool)ExportBooleanProperty[0];
						}
						catch (Exception e)
						{
							Logger.Error($"Can't read filter boolean field for: {NewObject.FullName}, {e.Message}");
							found = false;
						}
					}
				}
			}
			return found;
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

		/// <summary>
		/// Write out received configuration data
		/// </summary>
		/// <param name="UpdateType"></param>
		/// <param name="Id"></param>
		/// <param name="PointName"></param>
		public static void ProcessNewConfig(string UpdateType, int Id, string PointName)
		{
			// Uses the PropertyTranslations to read properties and write them out to the Birth Certificate, 
			// e.g. GPS locations, analogue range, digital states etc.
			// Note that this would increase start time and database load during start, 
			//  if the Connect method has 'UpdateConfigurationOnStart' parameter set (but it is required for Sparkplug).

			// Get Point properties, these will depend on type, and any null values are ignored
			object[] PointProperties = new object[PropertyTranslations.Count];

			try
			{
				PointProperties = AdvConnection.GetObjectFields(PointName, PropertyTranslations.Values.ToArray());
			}
			catch (Exception e)
			{
				Logger.Error($"Can't read object fields for: {PointName}, {e.Message}");
				return;
			}

			ExportToTarget.ProcessNewConfigExport( UpdateType, Id, PointName, PointProperties);

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
			ExportToTarget.ProcessNewDataExport(UpdateType, Id, PointName, Value, Timestamp, Quality);

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
		/// Called when server state changes from Main or Standby, this stops all Geo SCADA monitoring
		/// </summary>
		public static void EngineShutdown()
		{
			Logger.Info("Engine Shutdown");
			FeederContinue = false;
		}
	}
}
