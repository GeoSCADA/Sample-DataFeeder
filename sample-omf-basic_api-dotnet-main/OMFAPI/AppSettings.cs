using System;
using System.Collections.Generic;

namespace OMFAPI
{
    public class AppSettings
    {
        /// <summary>
        /// The list of endpoints to connect and send to
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2227:Collection properties should be read only", Justification = "Allow in configuration, reading from file")]
        public IList<Endpoint> Endpoints { get; set; }
    }
}
