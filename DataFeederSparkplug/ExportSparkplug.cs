// This implementation of ExportToTarget writes output data as a Sparkplug Server to an MQTT server
// This uses no MQTT server encryption, you are recommended to implement connection security

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;

// Bring in with nuget
using Newtonsoft.Json;
using uPLibrary.Networking.M2Mqtt;
using Google.Protobuf;

// From this solution
using Org.Eclipse.Tahu.Protobuf;

namespace DataFeederService
{
	static class ExportToTarget
	{
		// *********************** Public generic attributes

		// Logger
		private static readonly NLog.Logger Logger = NLog.LogManager.GetLogger("DataFeeder");

		// folder which the target can use
		public static string WorkingPath = "";

		// Status of Sparkplug Connection
		public static bool TargetConnectionState = false;

		// Settings
		public static ProgramSettings Settings;

		// Count of the messages we have sent out
		public static int ExportCount = 0;

		// A reference to the property trans array
		public static Dictionary<string, string> PropertyTranslations;

		// EDIT THIS TO STATE THE EXPORT NAME - it's used to name the credentials file if applicable
		public const String ExportName = "MQTT";

		// *********************** Private attributes specific to this export type

		// The Mqtt Library object
		private static MqttClient Client;

		// Sparkplug Message Sequence Number
		private static ulong Seq = 0; // Message Sequence, Range 0..255

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

		// *********************** Public interfacing functions

		// Expose the sizes of our buffers
		public static int ConfigQueueCount
		{
			get
			{
				return BirthQueue.Count;
			}
		}
		public static int DataQueueCount
		{
			get
			{
				return DataQueue.Count;
			}
		}

		public static bool Connect(ProgramSettings Settings)
		// Connect to MQTT with Sparkplug payload
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
			UserCredStore.FileReadCredentials(Settings.TargetCredentialsFile, out User, out PasswordStr);

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
				string DeathString = System.Text.Encoding.ASCII.GetString(DeathMessage);

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

		public static void RetryTargetConnection()
		{
			// Make MQTT Connection every 30 seconds - you could customise this
			if ((DateTimeOffset.UtcNow - SparkplugLastConnectionTry).TotalSeconds > 30)
			{
				TargetConnectionState = Connect( Settings);
				SparkplugLastConnectionTry = DateTimeOffset.UtcNow;
			}
		}

		public static void Disconnect()
		{
			Client.Disconnect();
		}

		// Only called once at the start - we accumulate new Birth metric entries but do not remove old entries
		public static void ClearPointConfig()
		{
			BirthMessage = new Payload();
			BirthMessageChanged = false;
			BirthMessage.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

			// Add BD Sequence (same as Death number)
			var bdSeqMetric = new Payload.Types.Metric();
			bdSeqMetric.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			bdSeqMetric.Name = "bdSeq";
			bdSeqMetric.Datatype = 8; // INT64
			bdSeqMetric.LongValue = ReadSparkplugState(false); // This should increment from previous connections
			BirthMessage.Metrics.Add(bdSeqMetric);
			Logger.Info($"Added Birth bdseq={bdSeqMetric.LongValue}");

			// Add Node Control/Rebirth Metric
			var RebirthMetric = new Payload.Types.Metric();
			RebirthMetric.Name = "Node Control/Rebirth";
			RebirthMetric.Datatype = 11; // Boolean
			RebirthMetric.BooleanValue = false;
			BirthMessage.Metrics.Add(RebirthMetric);
		}

		public static void SendToTargetServer()
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
						ExportCount++;
						//if (ExportCount % 100 == 0)
						//Logger.Info(bupdate.ToString());
						Logger.Info("Published Birth: " + ExportCount.ToString() + " times, last send: " + bupdate.Metrics.Count + " metrics");
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
						ExportCount++;
						if (ExportCount % 100 == 0)
						{
							//Logger.Info(update.ToString());
							Logger.Info("Published Data: " + ExportCount.ToString() + " times, last send: " + update.Metrics.Count + " metrics");
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

		// Called on a minute timer in the main loop, as well as when a batch has been accumulated
		public static void FlushPointData()
		{
			// If data exists to export
			if (DataMessage.Metrics.Count > 0)
			{
				// Add timestamp
				DataMessage.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

				DataQueue.Enqueue(DataMessage);

				Logger.Info($"Queued data: {DataMessage.Metrics.Count} metrics, {DataQueue.Count} total in queue.");

				// Start gathering next
				DataMessage = new Payload();
			}
		}

		private static void WriteMetric(Payload.Types.Metric Out)
		{
			// We are about to send data, ensure Birth message is sent first
			FlushMetricBirth();

			DataMessage.Metrics.Add(Out);
			// Create a new metric structure after 100 data messages (You should tune this for your setup)
			if (DataMessage.Metrics.Count > 100)
			{
				FlushPointData();
			}
		}

		/// <summary>
		/// Write out received configuration data
		/// </summary>
		/// <param name="UpdateType"></param>
		/// <param name="Id"></param>
		/// <param name="PointName"></param>
		public static void ProcessNewConfigExport(string UpdateType, int Id, string PointName, object[] PointProperties)
		{
			var Out = new Payload.Types.Metric();

			// Filter out from the point name using ExportGroupLevelTrim
			var GeoSCADANameParts = PointName.Split('.');
			var SparkplugName = "";
			// Trim initial parts of the name if the export level trim setting is used
			for (int i = Settings.ExportGroupLevelTrim; i < GeoSCADANameParts.Length; i++)
			{
				// Also change Geo SCADA to Sparkplug naming convention
				// Substitute any / found with \
				SparkplugName += GeoSCADANameParts[i].Replace('/', '\\') + "/";
			}
			// Trim the extra '/'
			Out.Name = SparkplugName.Substring(0, SparkplugName.Length - 1);

			// We use the Geo SCADA row Id as the unique Sparkplug Alias
			Out.Alias = (ulong)Id;

			// The next line is temporary - you can optionally remove it when using Geo SCADA 2022 May 2023 Update or after.
			// It just puts an X day old timestamp on zero data which will be processed even though IsNull is true.
			Out.Timestamp = (ulong)DateTimeOffset.UtcNow.Subtract(TimeSpan.FromDays(Settings.MaxDataAgeDays)).ToUnixTimeMilliseconds();
			Out.IsNull = true; // We are not sending any data in the Birth message

			// Create/add metric properties
			var PSKeys = new List<string>();
			var PSValues = new List<Payload.Types.PropertyValue>();

			// If BitCount is a property, use it to control point type
			int index = 0;
			byte BitCount = 0;
			foreach (string FieldName in PropertyTranslations.Keys)
			{
				if (FieldName == "BitCount" && PointProperties[index] != null)
				{
					var PSType = PointProperties[index].GetType().ToString();
					if (PSType == "System.Byte")
					{
						BitCount = (byte)PointProperties[index];
						break;
					}
				}
				index++;
			}
			index = 0;
			foreach (string FieldName in PropertyTranslations.Keys)
			{
				// If this is the point type
				if (FieldName == "TypeName")
				{
					string ClassName = (String)PointProperties[index];
					// Imprecise way to pick floating point types, can be improved by looking up type names
					if (ClassName.Contains("Analog") ||
						ClassName.Contains("Alg") ||
						ClassName.Contains("Counter"))
					{
						Out.Datatype = 10; // Double
					}
					else
					{
						// Digital types

						// If BitCount is a property and it's 1, make this a boolean, otherwise it's going to be analog
						if (BitCount == 1)
						{
							Out.Datatype = 11; // Boolean
						}
						else
						{
							Out.Datatype = 6; // UInt16
						}
					}
				}
				else
				{
					// Process other field values
					if (PointProperties[index] != null)
					{
						var PSValue = new Payload.Types.PropertyValue();
						var PSType = PointProperties[index].GetType().ToString();
						switch (PSType)
						{
							case "System.String":
								PSValue.Type = 12; // String
								PSValue.StringValue = (String)PointProperties[index];
								PSValues.Add(PSValue);
								PSKeys.Add(FieldName);
								break;
							case "System.Double":
							case "System.Float":
								PSValue.Type = 10; // Double
								PSValue.DoubleValue = (Double)PointProperties[index];
								PSValues.Add(PSValue);
								PSKeys.Add(FieldName);
								break;
							case "System.Int16":
							case "System.UInt16":
							case "System.Int32":
							case "System.UInt32":
							case "System.Int64":
							case "System.UInt64":
								PSValue.Type = 4; // Int64
								PSValue.IntValue = (uint)PointProperties[index];
								PSValues.Add(PSValue);
								PSKeys.Add(FieldName);
								break;
							case "System.Byte":
								PSValue.Type = 5; // UInt8
								PSValue.IntValue = (byte)PointProperties[index];
								PSValues.Add(PSValue);
								PSKeys.Add(FieldName);
								break;
							case "System.Boolean":
								PSValue.Type = 11; // Bool
								PSValue.BooleanValue = (bool)PointProperties[index];
								PSValues.Add(PSValue);
								PSKeys.Add(FieldName);
								break;
							default:
								Logger.Error($"Wrong fields type for Point: {PointName} Field: {FieldName} Type: {PSType}");
								break;
						}
					}
				}
				index++;
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
		/// Callback Function to read and process incoming data
		/// </summary>
		/// <param name="UpdateType">"Cur" or "His" to indicate type of update</param>
		/// <param name="Id"></param>
		/// <param name="PointName"></param>
		/// <param name="Value"></param>
		/// <param name="Timestamp"></param>
		/// <param name="Quality"></param>
		public static void ProcessNewDataExport(string UpdateType, int Id, string PointName, double Value, DateTimeOffset Timestamp, long Quality)
		{
			var Out = new Payload.Types.Metric();
			Out.Alias = (ulong)Id;
			Out.Datatype = 10; // Double
			Out.DoubleValue = Value;
			Out.Timestamp = (ulong)Timestamp.ToUnixTimeMilliseconds();
			Out.IsHistorical = false; // Always send as real-time values (DataUpdate.UpdateType == "His");

			WriteMetric(Out);
		}


		// *********************** Private methods - implemented for this specific export target

		// A function for read/write of the Birth/Death Sequence #
		private static ulong ReadSparkplugState(bool isBirth)
		{
			ulong bdSeq = 0;
			Directory.CreateDirectory(WorkingPath);
			if (File.Exists(WorkingPath + "\\" + "SparkplugState.csv"))
			{
				StreamReader SpUpdateFile = new StreamReader(WorkingPath + "\\" + "SparkplugState.csv");
				string line;
				line = SpUpdateFile.ReadLine();
				if (line != null)
				{
					if (ulong.TryParse(line, out bdSeq))
					{
						Logger.Info($"Read bdSeq: {bdSeq.ToString()} from {WorkingPath + "\\" + "SparkplugState.csv"}");
					}
				}
				SpUpdateFile.Close();
			}
			if (isBirth)
			{
				// Increment for next time we run or create a new Birth message, so when next read for death, one was added
				StreamWriter SpUpdateFile = new StreamWriter(WorkingPath + "\\" + "SparkplugState.csv");
				SpUpdateFile.WriteLine(bdSeq + 1);
				SpUpdateFile.Close();
			}
			return bdSeq;
		}


		private static Payload CreateDeathPayload()
		{
			var Death = new Payload();
			Death.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			var bdSeqMetric = new Payload.Types.Metric();
			bdSeqMetric.Name = "bdSeq";
			bdSeqMetric.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			bdSeqMetric.Datatype = 8; // INT64
			bdSeqMetric.LongValue = ReadSparkplugState(true); // This should increment from previous connections
			Logger.Info($"Added Birth bdseq={bdSeqMetric.LongValue}");
			Death.Metrics.Add(bdSeqMetric);
			return Death;
		}


		private static void FlushMetricBirth()
		{
			// If data exists to export
			if (BirthMessage.Metrics.Count > 0 && BirthMessageChanged)
			{
				QueueMetricBirth();
			}
		}

		// Output the birth message to the write queue. This is called when we have read/updated metrics or when we are asked for rebirth
		private static void QueueMetricBirth()
		{
			// Add timestamp
			BirthMessage.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

			BirthQueue.Enqueue(BirthMessage);
			BirthMessageChanged = false;
			Logger.Info($"Queued birth: {BirthMessage.Metrics.Count} metrics, {BirthQueue.Count} total in queue.");
		}

		private static void WriteMetricBirth(Payload.Types.Metric Out)
		{
			// Remove old entry for this metric, if it exists
			// This is not efficient for a large system - consider an alternative implementation
			foreach (var Metric in BirthMessage.Metrics)
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
		/// MQTT Sparkplug Functions, start with the callback for closing the connection
		/// </summary>
		/// <param name="sender">object</param>
		/// <param name="e">EventArgs</param>
		private static void Client_MqttConnectionClosed(object sender, System.EventArgs e)
		{
			Logger.Info("Callback to MqttConnectionClosed");
			// Pauses the export and retries connection
			TargetConnectionState = false;
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
					(tokens[1] == Settings.SCADAHostId))
			{
				string m = System.Text.Encoding.UTF8.GetString(e.Message);
				Logger.Info("Received: " + m);
				// We will receive either "ONLINE" or "OFFLINE"
				if (m == "OFFLINE" && !Settings.SparkplugIgnoreServerState)
				{
					// Here we disconnect from MQTT, buffering data and wait
					Logger.Info("Server Offline, Disconnect from MQTT");
					Client.Disconnect();
					TargetConnectionState = false;
				}
				else
				{
					if (Settings.SparkplugIgnoreServerState)
					{
						Logger.Info("Setup parameter SparkplugIgnoreServerState means we are ignoring server OFFLINE.");
					}
					else
					{
						Logger.Info("Server Online");
					}
				}
				return;
			}

			// Incoming server state - Sparkplug 3.0
			//$"spBv1.0/STATE/{SCADAHostId}"
			if ((Settings.SparkplugVersion == 3) &&
					(tokens.Length == 4) &&
					(tokens[0] == "spBv1.0") &&
					(tokens[1] == "STATE") &&
					(tokens[2] == Settings.SCADAHostId))
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
						TargetConnectionState = false;
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
			if (tokens.Length == 4 &&
				(tokens[0] == "spBv1.0") &&
				(tokens[1] == Settings.SpGroupId) &&
				((tokens[2] == "NCMD") || (tokens[2] == "DCMD")) &&
				(tokens[3] == Settings.SpNode))
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
							TargetConnectionState = false;
							break;
						case "Node Control/Rebirth":
							//# 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
							//# its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
							//# application if it receives an NDATA or DDATA with a metric that was not published in the
							//# original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
							//# its original NBIRTH and DBIRTH messages.
							Logger.Info("'Node Control/Rebirth' received");
							QueueMetricBirth();
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
							QueueMetricBirth();
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

	}

	// Online state structure for Sparkplug 3 online state
	// We will receive JSON { "online": true|false, "timestamp":n }
	public class OnlineState
	{
		public bool online;
		public ulong timestamp;
	}
}
