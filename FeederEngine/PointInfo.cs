using System;
using ClearScada.Client.Advanced;

// Class of 'watched' point or accumulator objects
namespace FeederEngine
{
	public class PointInfo
	{
		private int TagId { get; set; }
		public string PointName { get; set; }
		public bool IsAccumulator { get; set; }
		public bool IsHistoric { get; set; } // Make some members public so engine can perform first read
		private string HistoricAggregateName { get; set; }
		private int ReadSecondsHis { get; set; }
		private int ReadSecondsCur { get; set; }

		public DateTimeOffset LastChange;
		public int PointId;

		// Single connection
		static IServer AdvConnection;
		static bool ServerConnect = false;
		Action<string, int, string, double, DateTimeOffset, long> ProcessNewData;

		// Get Historic stats
		public static int HisCallCount = 0;
		public static double HisCallSeconds = 0;
		public static DateTime HisCallLastReported = DateTime.UtcNow;

		// Constructor
		public PointInfo(int _TagId, string _PointName, int _ReadSecondsHis, int _ReadSecondsCur, DateTimeOffset _LastChange, IServer _AdvConnection, Action<string, int, string, double, DateTimeOffset, long> _ProcessNewData)
		{
			IsAccumulator = false;
			IsHistoric = false;
			TagId = _TagId;
			ReadSecondsHis = _ReadSecondsHis;
			ReadSecondsCur = _ReadSecondsCur;
			PointName = _PointName;

			// If the LastChange DateTime is not specified or future, use the current time less read interval.
			// Typically LastChangeSeconds is used first time export runs, then the calling app saves LastChange periodically.
			if (_LastChange == DateTimeOffset.MinValue || _LastChange > DateTimeOffset.UtcNow) 
			{
				LastChange = DateTime.Now.AddSeconds(-_ReadSecondsHis);  // Default time to wind back on start lastchange
			}
			else
			{
				LastChange = _LastChange;
			}

			// One time setup server, and the data callback
			if (!ServerConnect)
			{
				AdvConnection = _AdvConnection;
				ServerConnect = true;
			}
			ProcessNewData = _ProcessNewData;

			// Get point object from id
			ObjectDetails Point;
			try
			{
				Point = AdvConnection.FindObject(PointName);
			}
			catch (ClearScada.Client.CommunicationsException)
			{
				Console.WriteLine("*** Comms exception (FindObject)");
				return;
			}
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
					Object[] HisEnabled;
					try
					{
						HisEnabled = AdvConnection.GetObjectFields(PointName, new string[] { HistoricAggregateName });
					}
					catch (ClearScada.Client.CommunicationsException)
					{
						Console.WriteLine("*** Comms exception (GetObjectFields)");
						return;
					}
					if ((bool)HisEnabled[0])
					{
						IsHistoric = true;
						break;
					}
				}
			}

			try
			{
				if (IsHistoric)
				{
					AdvConnection.AddTags(new TagDetails(TagId, PointName + "." + HistoricAggregateName + ".LastUpdateTime",
																					new TimeSpan(0, 0, ReadSecondsHis)));
				}
				else
				{
					if (IsAccumulator)
					{
						// Accumulator with no history
						AdvConnection.AddTags(new TagDetails(TagId, PointName + ".CurrentTotal", new TimeSpan(0, 0, ReadSecondsCur)));
					}
					else
					{
						AdvConnection.AddTags(new TagDetails(TagId, PointName + ".CurrentValue", new TimeSpan(0, 0, ReadSecondsCur)));
					}
				}
			}
			catch (ClearScada.Client.CommunicationsException)
			{
				Console.WriteLine("*** Comms exception (AddTags)");
				return;
			}
		}

		// This is done manually to remove subscription immediately (implement IDisposable, but delays subscription)
		public void Dispose()
		{
			AdvConnection.RemoveTags(new int[] { TagId });
		}

		// Read historic or real-time data
		public int ReadHistoric(IServer AdvConnection, CustomTagUpdate Update)
		{
			// If this is an initialisation update, only applicable to the first read of a value
			if (Update.Status == "Init")
			{
				if (IsHistoric) // UpdateValue is the historic update time
				{
					Update.Value = DateTimeOffset.UtcNow;
				}
				else // Read current data value
				{
					object[] PointProperties = {  };
					try
					{
						if (IsAccumulator)
						{
							PointProperties = AdvConnection.GetObjectFields(PointName, new string[] { "CurrentTotalTime", "CurrentTotal", "CurrentTotalQuality" });
						}
						else
						{
							PointProperties = AdvConnection.GetObjectFields(PointName, new string[] { "CurrentTime", "CurrentValue", "CurrentQuality" });
						}
						Update.Timestamp = (DateTimeOffset)PointProperties[0];
						Update.Value = (object)PointProperties[1];
						Update.Quality =  (ushort)PointProperties[2];
					}
					catch (Exception e)
					{
						Console.WriteLine("Error reading init data value: " + e.Message);
					}
				}
			}
			int UpdateCount = 0;
			if (IsHistoric) // UpdateValue is the historic update time
			{
				// Get historic data from previous update to this one
				//																No filter, aggregate or offset. Just read raw data.
				HistoricTag tag = new HistoricTag(PointName + "." + HistoricAggregateName, 0, 0, 0);
				var tags = new HistoricTag[1];
				tags[0] = tag;
				try
				{
					int LastReadCount = 1000; // Keep reading until we have fewer than this, then there are no more left.
					do
					{
						// add a millisecond to last time to prevent read of sample already sent					
						// add a millisecond to last change time to include latest value
						// false = don't read boundaries (null values at each end of the interval)
						//Console.Write("Read for: " + PointName);
						var timeNow = DateTime.UtcNow;
						var historicItems = AdvConnection.ReadRawHistory(LastChange.AddMilliseconds(1), ((DateTimeOffset)Update.Value).AddMilliseconds(1), LastReadCount, false, tags);
						
						// Report every minute how many historic calls and in-call process time
						HisCallCount++;
						HisCallSeconds += DateTime.UtcNow.Subtract(timeNow).TotalSeconds;
						if (DateTime.UtcNow.Subtract(HisCallLastReported).TotalSeconds > 60)
						{
							Console.WriteLine($">>Calls to get history: {HisCallCount}, Total call time: {HisCallSeconds}");
							HisCallLastReported = DateTime.UtcNow;
						}

						int historicItemCount = 0;
						foreach (var hi in historicItems)
						{
							foreach (var s in hi.Samples)
							{
								LastChange = s.Timestamp; // Write time of sample last sent
								WriteData("His", PointId, PointName, s.Value, s.Timestamp, (long)s.Quality);
								historicItemCount++;
							}
						}
						//Console.WriteLine(" " + historicItemCount.ToString() + " for " + LastChange.ToString());
						UpdateCount += historicItemCount;
						LastReadCount = historicItemCount;
					} while (LastReadCount == 1000);
				}
				catch (NullReferenceException)
				{
					Console.WriteLine("Null exception, read attempt from deleted point: " + PointName);
				}
				catch (ClearScada.Client.CommunicationsException)
				{
					Console.WriteLine("*** Comms exception (ReadRawHistory)");
					// Stop feeding data, lost server connection, send an uncertain value
					WriteData("Shutdown", PointId, PointName, 0, DateTimeOffset.UtcNow, (long)OpcQuality.Uncertain);
				}

			}
			else // Not Historic - UpdateValue is the numeric value of the point (or time if time point)
			{
				UpdateCount++;
				WriteData( "Cur", PointId, PointName, Update.Value, Update.Timestamp, Update.Quality);
			}
			return UpdateCount;
		}

		// Handle data writes
		private void WriteData( string DataType, int Id, string PointName, object Value, DateTimeOffset Timestamp, long Quality)
		{
			if (Value is DateTimeOffset)
			{
				TimeSpan t = ((DateTimeOffset)(Value)).ToUniversalTime() - new DateTime(1970, 1, 1);
				long millisecondsSinceEpoch = (long)t.TotalSeconds * 1000;
				ProcessNewData(DataType, Id, PointName, millisecondsSinceEpoch, Timestamp, Quality);
				return;
			}
			if (Value is Byte)
			{
				byte b = (byte)(Value);
				double v = (double)(b);
				ProcessNewData( DataType, Id, PointName, v, Timestamp, Quality);
				return;
			}
			// If a point has never been processed it may be null. In this code we will set that to zero. Your solution may require other handling.
			// This also ignores other non-numeric types like String and Time points, setting the value to zero. You may wish another approach.
			if (Value is null || !(Value is byte || Value is sbyte || Value is ushort || Value is uint || Value is ulong || Value is short || Value is int || Value is long || Value is float || Value is double || Value is decimal))
			{
				ProcessNewData(DataType, Id, PointName, 0, Timestamp, Quality);
			}
			else
			{
				ProcessNewData(DataType, Id, PointName, (double)Value, Timestamp, Quality);
			}
		}

		// Point is renamed
		public void Rename(string NewName)
		{
			PointName = NewName;
		}
	}

}
