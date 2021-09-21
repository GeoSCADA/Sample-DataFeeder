using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace OMFAPI
{
    public static class Program
    {
        // The version of the OMFmessages
        private const string OmfVersion = "1.1";

        // Constant for determining the pause between sending OMF data messages
        private const int SendSleep = 1000;

        private static readonly HttpClient _client = new HttpClient();

        // Holders for the data message values
        private static readonly Random _rnd = new Random();
        private static bool _dynamicBoolHolder = true;
        private static int _dynamicIntHolder;

        public static void Main()
        {
            RunMain();
        }

        /// <summary>
        /// Main function to allow for easy testing.
        /// </summary>
        /// <param name="test">Whether this is a test or not</param>
        public static bool RunMain(bool test = false, Dictionary<string, dynamic> lastSentValues = null)
        {
            bool success = true;

            // Step 1 - Read endpoint configurations from config.json
            AppSettings settings = GetAppSettings();
            IList<Endpoint> endpoints = settings.Endpoints;

            // Step 2 - Get OMF Types
            dynamic omfTypes = GetJsonFile("OMF-Types.json");

            // Step 3 - Get OMF Containers
            dynamic omfContainers = GetJsonFile("OMF-Containers.json");

            // Step 4 - Get OMF Data
            dynamic omfData = GetJsonFile("OMF-Data.json");

            // Send messages and check for each endpoint in config.json
            try
            {
                // Send out the messages that only need to be sent once
                foreach (var endpoint in endpoints)
                {
                    if ((endpoint.VerifySSL is bool boolean) && boolean == false)
                        Console.WriteLine("You are not verifying the certificate of the end point.  This is not advised for any system as there are security issues with doing this.");

                    // Step 5 - Send OMF Types
                    foreach (var omfType in omfTypes)
                    {
                        string omfTypeString = $"[{JsonConvert.SerializeObject(omfType)}]";
                        SendMessageToOmfEndpoint(endpoint, "type", omfTypeString);
                    }

                    // Step 6 - Send OMF Containers
                    foreach (var omfContainer in omfContainers)
                    {
                        string omfContainerString = $"[{JsonConvert.SerializeObject(omfContainer)}]";
                        SendMessageToOmfEndpoint(endpoint, "container", omfContainerString);
                    }
                }

                // Step 7 - Send OMF Data
                int count = 0;

                // send data to all endpoints forever if this is not a test
                while (!test || count < 2)
                {
                    /*
                    * This is where custom loop logic should go.
                    * The getData call should also be customized to populate omfData with relevant data.
                    * */

                    foreach (var omfDatum in omfData)
                    {
                        // retrieve data
                        GetData(omfDatum);

                        foreach (var endpoint in endpoints)
                        {
                            // send data
                            string omfDatumString = $"[{JsonConvert.SerializeObject(omfDatum)}]";
                            SendMessageToOmfEndpoint(endpoint, "data", omfDatumString);
                        }

                        // record the values sent if this is a test
                        if (test && count == 1)
                            lastSentValues.Add((string)omfDatum.containerid, omfDatum);
                    }

                    count++;
                    Thread.Sleep(SendSleep);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Encountered Error: {e}");
                success = false;
                if (test)
                    throw;
            }

            Console.WriteLine("Done");
            return success;
        }

        /// <summary>
        /// Gets a json file in the current directory of name filename.
        /// </summary>
        public static dynamic GetJsonFile(string filename)
        {
            dynamic dynamicJson = JsonConvert.DeserializeObject(File.ReadAllText($"{Directory.GetCurrentDirectory()}/{filename}"));

            return dynamicJson;
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
                if (!(string.Equals(endpoint.EndpointType, "OCS", StringComparison.OrdinalIgnoreCase) || string.Equals(endpoint.EndpointType, "EDS", StringComparison.OrdinalIgnoreCase) || string.Equals(endpoint.EndpointType, "PI", StringComparison.OrdinalIgnoreCase)))
                    throw new Exception($"Invalid endpoint type {endpoint.EndpointType}");
            }

            return settings;
        }

        /// <summary>
        /// Gets the current time
        /// </summary>
        public static string GetCurrentTime()
        {
            return DateTime.UtcNow.ToString("o");
        }

        /// <summary>
        /// Populates data with relevant data depending on the container type
        /// </summary>
        /// <param name="data">The dynamic json data object to populate</param>
        public static void GetData(dynamic data)
        {
            if (data.containerid == "FirstContainer" || data.containerid == "SecondContainer")
            {
                data.values[0].Timestamp = GetCurrentTime();
                data.values[0].IntegerProperty = (int)(_rnd.NextDouble() * 100);
            }
            else if (data.containerid == "ThirdContainer")
            {
                _dynamicBoolHolder = !_dynamicBoolHolder;
                data.values[0].Timestamp = GetCurrentTime();
                data.values[0].NumberProperty1 = _rnd.NextDouble() * 100;
                data.values[0].NumberProperty2 = _rnd.NextDouble() * 100;
                data.values[0].StringEnum = _dynamicBoolHolder.ToString();
            }
            else if (data.containerid == "FourthContainer")
            {
                _dynamicIntHolder = (_dynamicIntHolder + 1) % 2;
                data.values[0].Timestamp = GetCurrentTime();
                data.values[0].IntegerEnum = _dynamicIntHolder;
            }
            else
            {
                Console.WriteLine($"Container {data.containerid} not recognized");
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

            using var request = new HttpRequestMessage()
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

            using var request2 = new HttpRequestMessage()
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
                using var resp = request.GetResponse();
                using var response = (HttpWebResponse)resp;
                var stream = resp.GetResponseStream();
                var code = (int)response.StatusCode;

                using var reader = new StreamReader(stream);

                // Read the content.  
                string responseString = reader.ReadToEnd();

                // Display the content.
                return responseString;
            }
            catch (WebException e)
            {
                using WebResponse response = e.Response;
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
    }
}
