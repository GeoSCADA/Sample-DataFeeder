using System;
using System.Collections.Generic;

namespace DataFeederApp
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

// Sample:
//{
//    "Endpoints": [
//      {
//        "EndpointType": "OCS",
//      "Resource": "https://dat-b.osisoft.com",
//      "NamespaceName": "REPLACE_WITH_NAMESPACE_NAME",
//      "Tenant": "RPLACE_WITH_TENANT_ID",
//      "ClientId": "REPLACE_WITH_APPLICATION_IDENTIFIER",
//      "ClientSecret": "REPLACE_WITH_APPLICATION_SECRET",
//      "ApiVersion": "v1",
//      "VerifySSL": true,
//      "UseCompression": false,
//      "WebRequestTimeoutSeconds": 30
//      },
//    {
//        "EndpointType": "EDS",
//      "Resource": "http://localhost:5590",
//      "ApiVersion": "v1"
//    },
//    {
//        "EndpointType": "PI",
//      "Resource": "REPLACE_WITH_PI_WEB_API_URL",
//      "DataServerName": "REPLACE_WITH_DATA_ARCHIVE_NAME",
//      "Username": "REPLACE_WITH_USERNAME",
//      "Password": "REPLACE_WITH_PASSWORD",
//      "VerifySSL": true
//    }
//  ]
//}
