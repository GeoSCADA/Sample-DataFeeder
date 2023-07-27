// This implementation of ExportToTarget writes output data as a Sparkplug Server to an MQTT server
// This uses no MQTT server encryption, you are recommended to implement connection security

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq; // For Dict.Keys.ToArray()

// Bring in with nuget
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;

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

		// A reference to the property trans array - target column name vs source column
		public static Dictionary<string, string> PropertyTranslations;
		// A reference to the table trans array - target column name vs source table
		public static Dictionary<string, string> PropertyTypes;

		// EDIT THIS TO STATE THE EXPORT NAME - it's used to name the credentials file if applicable
		public const String ExportName = "BigQuery";

		// *********************** Private attributes specific to this export type

		// The connection client
		private static BigQueryClient Client = null;

		// Always start with an old time here so that the first loop we try to connect
		private static DateTimeOffset LastConnectionTry = DateTimeOffset.UtcNow.AddSeconds(-600);

		// In-memory queues of messages to be sent out
		private static ConcurrentQueue<List<ConfigChange>> ConfigQueue = new ConcurrentQueue<List<ConfigChange>>();
		private static ConcurrentQueue<List<DataVQT>> DataQueue = new ConcurrentQueue<List<DataVQT>>();

		// Table references - the tables are created on first access, allowing configuration to create all columns
		private static Google.Apis.Bigquery.v2.Data.TableReference ConfigTableRef = null;
		private static Google.Apis.Bigquery.v2.Data.TableReference DataTableRef = null;

		// Config Message - accumulates point changes, gets sent when values come in
		private static List<ConfigChange> ConfigMessage = new List<ConfigChange>();

		// Sparkplug Data Message - buffers data changes until full, then gets sent
		private static List<DataVQT> DataMessage = new List<DataVQT>();

		// *********************** Public interfacing functions

		// Expose the sizes of our buffers
		public static int ConfigQueueCount
		{
			get
			{
				return ConfigQueue.Count;
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
		// Connect to BigQuery
		{
			Logger.Info("Using BigQuery Project: " + Settings.ProjectID);

			try
			{
				GoogleCredential credential = GoogleCredential.FromFile(Settings.GoogleCredentialFile);
				Client = BigQueryClient.Create(Settings.ProjectID, credential);

				// Null table references so they are recreated later
				ConfigTableRef = null;
				DataTableRef = null;
			}
			catch (Exception ex)
			{
				Logger.Error("Cannot connect to BigQuery " + ex.Message);
				return false;
			}
			return true;
		}

		public static void RetryTargetConnection()
		{
			// Make MQTT Connection every 30 seconds - you could customise this
			if ((DateTimeOffset.UtcNow - LastConnectionTry).TotalSeconds > 30)
			{
				TargetConnectionState = Connect(Settings);
				LastConnectionTry = DateTimeOffset.UtcNow;
			}
		}

		public static void Disconnect()
		{
			if (Client != null)
			{
				Client.Dispose();
			}
		}

		// Only called once at the start
		// In this export we send config messages but don't accumulate them
		public static void ClearPointConfig()
		{
			ConfigMessage.Clear();
		}

		public static void SendToTargetServer()
		{
			// Dequeue all Config to BigQuery
			while (ConfigQueue.Count > 0)
			{
				if (ConfigQueue.TryPeek(out List<ConfigChange> configs))
				{
					var PropNames = PropertyTranslations.Keys.ToArray();

					// Has table been referenced, if not try to create it
					if (ConfigTableRef == null)
					{
						Logger.Info("Create config table");
						var schema = new TableSchemaBuilder
							{
								{"PointId", BigQueryDbType.Int64, BigQueryFieldMode.Required, "Geo SCADA Id" },
								{"UpdateType", BigQueryDbType.String, BigQueryFieldMode.Nullable, "Type of configuration update" },
								{"PointName", BigQueryDbType.String, BigQueryFieldMode.Required, "Point Full Name" },
								{"Timestamp", BigQueryDbType.DateTime, BigQueryFieldMode.Required, "Time of Configuration Record" },
							}.Build();

						foreach( string PropName in PropertyTypes.Keys)
						{
							var field = new Google.Apis.Bigquery.v2.Data.TableFieldSchema();
							field.Name = PropName;
							switch( PropertyTypes[ PropName])
							{
								case "System.String":
									field.Type = "STRING";
									break;
								case "System.Double":
								case "System.Float":
								case "System.Single":
									field.Type = "FLOAT";
									break;
								case "System.Int16":
								case "System.UInt16":
								case "System.Int32":
								case "System.UInt32":
								case "System.Int64":
								case "System.UInt64":
									field.Type = "INTEGER";
									break;
								case "System.Byte":
									field.Type = "INTEGER";
									break;
								case "System.DateTime":
								case "System.DateTimeOffset":
									field.Type = "DATETIME";
									break;
								case "System.Boolean":
									field.Type = "BOOLEAN";
									break;
								default:
									field.Type = "STRING";
									break;
							}
							field.Mode = "NULLABLE";
							field.Description = PropertyTranslations[PropName];
							schema.Fields.Add(field);
						}
						DefineBigQueryTable(ref ConfigTableRef, Settings.ConfDataTableId, schema);
						// Returns False if not created, but also blanks the table ref
					}

					
					if (ConfigTableRef != null)
					{
						List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
						foreach (var config in configs)
						{
							var row = new BigQueryInsertRow
							{
								{ "PointId", config.PointId },
								{ "UpdateType", config.UpdateType },
								{ "PointName", config.PointName },
								{ "Timestamp", config.Timestamp.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff") },
							};
							for (int i = 0; i < PropNames.Length; i++)
							{
								if (config.Properties[i] != null)
								{
									var PSType = config.Properties[i].GetType().ToString();
									object PSValue;
									switch (PSType)
									{
										case "System.String":
											PSValue = (String)config.Properties[i];
											break;
										case "System.Double":
										case "System.Float":
										case "System.Single":
											PSValue = Convert.ToDouble(config.Properties[i]);
											break;
										case "System.Int16":
										case "System.UInt16":
										case "System.Int32":
										case "System.UInt32":
										case "System.Int64":
										case "System.UInt64":
											PSValue = (Int64)config.Properties[i];
											break;
										case "System.Byte":
											PSValue = Convert.ToInt32( (byte)config.Properties[i]);
											break;
										case "System.DateTime":
										case "System.DateTimeOffset":
											PSValue = (string)((DateTimeOffset)config.Properties[i]).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff");
											break;
										case "System.Boolean":
											PSValue = (bool)config.Properties[i];
											break;
										default:
											PSValue = (string)config.Properties[i];
											break;
									}
									row.Add(PropNames[i], PSValue);
								}
							}
							rows.Add(row);
						}

						// write out data, returns false on failure
						if (WriteBigQueryTable(ref ConfigTableRef, rows))
						{
							ExportCount+= rows.Count;

							//if (ExportCount % 100 == 0)
							{
								//Logger.Info(update.ToString());
								Logger.Info("Published: " + ExportCount.ToString() + " rows");
							}
							// Discard successfully sent data
							ConfigQueue.TryDequeue(out configs);
						}
					}
				}
			}

			// Dequeue all data to BigQuery
			while (DataQueue.Count > 0)
			{
				if (DataQueue.TryPeek(out List<DataVQT> updates))
				{
					// Has table been referenced, if not try to create it
					if (DataTableRef == null)
					{
						Logger.Info("Create historic table");
						var schema = new TableSchemaBuilder
							{
								{"PointId", BigQueryDbType.Int64, BigQueryFieldMode.Required, "Geo SCADA Id" },
								{"UpdateType", BigQueryDbType.String, BigQueryFieldMode.Nullable, "Type of update" },
								{"Value", BigQueryDbType.Float64, BigQueryFieldMode.Nullable, "Point Value" },
								{"Timestamp", BigQueryDbType.DateTime, BigQueryFieldMode.Required, "Time of Data" },
								{"OPCQuality", BigQueryDbType.Int64, BigQueryFieldMode.Nullable, "OPC Quality" },
								{"ExtendedQuality", BigQueryDbType.Int64, BigQueryFieldMode.Nullable, "Extended OPC Quality" }
							}.Build();

						DefineBigQueryTable(ref DataTableRef, Settings.HisDataTableId, schema );
						// Returns False if not created, but also blanks the table ref
					}
					if (DataTableRef != null)
					{
						List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
						foreach (var update in updates)
						{
							var row = new BigQueryInsertRow
							{
								{ "PointId", update.PointId },
								{ "UpdateType", update.UpdateType },
								{ "Value", update.Value },
								{ "Timestamp", update.Timestamp.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff") },
								{ "OPCQuality", update.OPCQuality },
								{ "ExtendedQuality", update.ExtendedQuality },
							};
							rows.Add(row);
						}

						// write out data, returns false on failure
						if (WriteBigQueryTable(ref DataTableRef, rows))
						{
							ExportCount += rows.Count;

							//if (ExportCount % 100 == 0)
							{
								//Logger.Info(update.ToString());
								Logger.Info("Published: " + ExportCount.ToString() + " rows");
							}
							// Discard successfully sent data
							DataQueue.TryDequeue(out updates);
						}
					}
				}
			}
		}

		// Define a table. Returns True if successful, OR the table was already there
		// Returns False if there was an error creating the table
		private static bool DefineBigQueryTable(ref Google.Apis.Bigquery.v2.Data.TableReference TableRef, 
												string DataTableId, 
												Google.Apis.Bigquery.v2.Data.TableSchema schema)
		{
			// try to create data table if not defined

			var options = new CreateTableOptions { }; // None exist currently

			TableRef = Client.GetTableReference(Settings.DataSetId, DataTableId);

			try
			{
				BigQueryTable myTable = Client.CreateTable(TableRef, schema, options);
			}
			catch (Exception e)
			{
				if (e.Message.ToLower().Contains("already exists: table"))
				{
					// Can continue if we assume the table has the right columns
					Logger.Info("Table exists, continuing");
				}
				else
				{
					Logger.Info("Error creating table");
					TableRef = null;
					return false;
				}
			}
			return true;
		}

		private static bool WriteBigQueryTable(ref Google.Apis.Bigquery.v2.Data.TableReference TableRef, List<BigQueryInsertRow> rows)
		{
			try
			{
				Client.InsertRows(TableRef, rows);
			}
			catch (Exception e)
			{
				Logger.Info("Error inserting data " + e.Message);
				return false;
			}
			return true;
		}

		// For BigQuery we have a config collection, appended for new data 
		// A config payload defines properties
		// A collection of Data payloads define point values

		// Called on a minute timer in the main loop, as well as when a batch has been accumulated
		public static void FlushPointData()
		{
			// If data exists to export
			if (DataMessage.Count > 0)
			{
				// Need to copy the Config items individually
				var QueueMessage = new List<DataVQT>();
				foreach (var d in DataMessage)
				{
					QueueMessage.Add(d);
				}
				DataQueue.Enqueue(QueueMessage);
				Logger.Info($"Queued data: {DataMessage.Count} values, {DataQueue.Count} total in queue.");

				// Start gathering next
				DataMessage.Clear();
			}
		}

		private static void WritePointData(DataVQT Out)
		{
			// We are about to send data, ensure Config message is sent first
			FlushPointConfig();

			DataMessage.Add(Out);
			// Create a new point data structure after X data messages (You should tune this for your setup)
			if (DataMessage.Count > Settings.BatchRecordCount)
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
			var Out = new ConfigChange();

			// Filter out from the point name using ExportGroupLevelTrim
			var GeoSCADANameParts = PointName.Split('.');
			var ExportName = "";
			// Trim initial parts of the name if the export level trim setting is used
			for (int i = Settings.ExportGroupLevelTrim; i < GeoSCADANameParts.Length; i++)
			{
				ExportName += GeoSCADANameParts[i] + ".";
			}
			// Trim the extra '.'
			Out.PointName = ExportName.Substring(0, ExportName.Length - 1);

			Out.PointId = Id;

			Out.Timestamp = DateTimeOffset.UtcNow;

			Out.UpdateType = UpdateType;

			Out.Properties = PointProperties;

			WritePointConfig(Out);
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
			var Out = new DataVQT();
			Out.PointId = Id;
			Out.UpdateType = UpdateType;
			Out.Value = Value;
			Out.Timestamp = Timestamp;
			Out.OPCQuality = (Int32)Quality & 0xFF;
			Out.ExtendedQuality = (Int32)Quality;
			WritePointData(Out);
		}


		// *********************** Private methods - implemented for this specific export target

		private static void FlushPointConfig()
		{
			// If data exists to export
			if (ConfigMessage.Count > 0)
			{
				QueuePointConfig();
			}
			ConfigMessage.Clear();
		}

		// Output the config message to the write queue. This is called when we have read/updated configs
		private static void QueuePointConfig()
		{
			// Need to copy the Config items individually
			var QueueMessage = new List<ConfigChange>();
			foreach( var d in ConfigMessage)
			{
				QueueMessage.Add(d);
			}
			ConfigQueue.Enqueue(QueueMessage);
			// Add timestamp
			Logger.Info($"Queued config: {ConfigMessage.Count} points, {ConfigQueue.Count} total in queue.");
		}

		private static void WritePointConfig(ConfigChange Out)
		{
			ConfigMessage.Add(Out);
			// Create a new config structure after X config messages (You should tune this for your setup)
			if (ConfigMessage.Count > Settings.BatchRecordCount)
			{
				FlushPointConfig();
			}
		}
	}

	// Data structure of exported data
	public class DataVQT
	{
		public int PointId;
		public string UpdateType;
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
		public Object[] Properties;
	}
}
