using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFeederApp
{
	public class OMF_Container
	{
		public string id { get; set; } // Use the Geo SCADA object name
		public string typeid { get; set; } // Refer to the type "FloatDynamicType"
		// public string name { get; set; } // Can add a friendly name, e.g. from a metadata field
		// public string description { get; set; } // Similarly a description
		// public string [] tags { get; set; } // Tags might include the point type
		public string typeVersion { get; set; } // Default to "1.0.0.0"

	}

	// Data value for a point
	public class OMF_DataValue
	{
		public string containerid { get; set; } // Use the Geo SCADA object name
		public OMF_DataValueItem [] values; // Class of Value properties  
	}
	// VTQ
	public class OMF_DataValueItem
	{
		public DateTimeOffset Timestamp { get; set; }
		public Double Value { get; set; }
		public long Quality { get; set; }
	}

	// Configuration properties for a point
	public class OMF_DataObject
	{
		public String containerid { get; set; } // Use the Geo SCADA object name
		public OMF_DataObjectItem [] values; // Class of Object properties  
	}
	// Config fields
	public class OMF_DataObjectItem
	{
		public String FullName { get; set; }
		public Int32 Id { get; set; }
		public String UpdateType { get; set; }
		public DateTimeOffset Timestamp { get; set; }
		public String ClassName { get; set; }
		public Double FullScale { get; set; }
		public Double ZeroScale { get; set; }
		public String Units { get; set; }
		public Int32 BitCount { get; set; }
		public Double Latitude { get; set; }
		public Double Longitude { get; set; }
	}
}
