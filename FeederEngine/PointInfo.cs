using System;
using ClearScada.Client.Advanced;

// Class of 'watched' point or accumulator objects
namespace FeederEngine
{
	public class PointInfo
	{
		private int TagId { get; set; }
		private string PointName { get; set; }
		private bool IsAccumulator { get; set; }
		private bool IsHistoric { get; set; }
		private string HistoricAggregateName { get; set; }
		private int ReadSeconds { get; set; }

		public DateTimeOffset LastChange;
		public int PointId;

		// Single connection
		static IServer AdvConnection;
		static bool ServerConnect = false;
		Action<string, int, string, double, DateTimeOffset, int> ProcessNewData;

		// Constructor
		public PointInfo(int _TagId, string _PointName, int _ReadSeconds, DateTimeOffset _LastChange, IServer _AdvConnection, Action<string, int, string, double, DateTimeOffset, int> _ProcessNewData)
		{
			IsAccumulator = false;
			IsHistoric = false;
			TagId = _TagId;
			ReadSeconds = _ReadSeconds;
			PointName = _PointName;

			// If the LastChange DateTime is not specified or future, use the current time less read interval.
			// Typically LastChangeSeconds is used first time export runs, then the calling app saves LastChange periodically.
			if (_LastChange == DateTimeOffset.MinValue || _LastChange > DateTimeOffset.UtcNow) 
			{
				LastChange = DateTime.Now.AddSeconds(-_ReadSeconds);  // Default time to wind back on start lastchange
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
		public int ReadHistoric(IServer AdvConnection, TagUpdate Update)
		{
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
					int LastReadCount = 100; // Keep reading until we have fewer than this, then there are no more left.
					do
					{
						// add a millisecond to last time to prevent read of sample already sent					
						// add a millisecond to last change time to include latest value
						// false = don't read boundaries (null values at each end of the interval)
						var historicItems = AdvConnection.ReadRawHistory(LastChange.AddMilliseconds(1), ((DateTimeOffset)Update.Value).AddMilliseconds(1), LastReadCount, false, tags);
						int historicItemCount = 0;
						foreach (var hi in historicItems)
						{
							foreach (var s in hi.Samples)
							{
								LastChange = s.Timestamp; // Write time of sample last sent
								WriteData("His", PointId, PointName, s.Value, s.Timestamp, s.Quality);
								historicItemCount++;
							}
						}
						UpdateCount += historicItemCount;
						LastReadCount = historicItemCount;
					} while (LastReadCount == 100);
				}
				catch (NullReferenceException)
				{
					Console.WriteLine("Null exception, read attempt from deleted point: " + PointName);
				}
				catch (ClearScada.Client.CommunicationsException)
				{
					Console.WriteLine("*** Comms exception (ReadRawHistory)");
					// Stop feeding data, lost server connection
					WriteData("Shutdown", PointId, PointName, 0, DateTimeOffset.UtcNow, 0);
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
		private void WriteData( string DataType, int Id, string PointName, object Value, DateTimeOffset Timestamp, OpcQuality Quality)
		{
			if (Value is DateTimeOffset)
			{
				TimeSpan t = ((DateTimeOffset)(Value)).ToUniversalTime() - new DateTime(1970, 1, 1);
				long millisecondsSinceEpoch = (long)t.TotalSeconds * 1000;
				ProcessNewData(DataType, Id, PointName, millisecondsSinceEpoch, Timestamp, (int)Quality);
				return;
			}
			if (Value is Byte)
			{
				byte b = (byte)(Value);
				double v = (double)(b);
				ProcessNewData( DataType, Id, PointName, v, Timestamp, (int)Quality);
				return;
			}
			ProcessNewData( DataType, Id, PointName, (double)Value, Timestamp, (int)(Quality));
		}

		// Point is renamed
		public void Rename(string NewName)
		{
			PointName = NewName;
		}
	}

}
