namespace FileWatcherModule
{
    using System;
    using System.IO;
    using System.Runtime.Loader;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Linq;
    using Newtonsoft.Json;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Shared;
    //using Newtonsoft.Json;

    class Program
    {
        private const int DefaultInterval = 10000;
        private const string DefaultSearchPattern = "*.csv";
        private const string DefaultRenameExtension = ".old";

        private readonly static string s_connectionString = "{IOTHUB_CONNECTION_STRING}";

        static void Main(string[] args)
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the DeviceClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            Console.WriteLine("----------iot filewatcher---------");
            Console.WriteLine("");

            // Open a connection to the Edge runtime
            DeviceClient ioTHubDeviceClient = DeviceClient.CreateFromConnectionString(s_connectionString, TransportType.Mqtt);

            // Execute callback method for Twin desired properties updates
            var twin = await ioTHubDeviceClient.GetTwinAsync();
            await OnDesiredPropertiesUpdate(twin.Properties.Desired, ioTHubDeviceClient);

            // Attach a callback for updates to the device twin's desired properties.
            await ioTHubDeviceClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertiesUpdate, ioTHubDeviceClient);

            await ioTHubDeviceClient.OpenAsync();
            Console.WriteLine("IoT Hub device client initialized.");

            var directory = Directory.GetCurrentDirectory();
            System.Console.WriteLine($"Current Directory {directory}");

            if (!Directory.Exists("exchange"))
            {
                System.Console.WriteLine($"No sub folder 'exchange' found");
            }
            else
            {
                System.Console.WriteLine($"Found sub folder 'exchange'");
            }

            var thread = new Thread(() => ThreadBody(ioTHubDeviceClient));
            thread.Start();
        }

         private static async void ThreadBody(object userContext)
        {
            var client = userContext as DeviceClient;

            if (client == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            while (true)
            {
                try
                {                    
                    var files = Directory.GetFiles("exchange", SearchPattern);

                    System.Console.WriteLine($"{DateTime.UtcNow} - Seen {files.Length} files with pattern '{SearchPattern}'" );

                    if (files.Length > 0)
                    {
                        foreach(var fileName in files)
                        {
                            try
                            {
                                var canRead = false;
                                var canWrite = false;

                                using (var fs = new FileStream(fileName, FileMode.Open))
                                {
                                    canRead = fs.CanRead;
                                    canWrite = fs.CanWrite;
                                }

                                if (!canRead
                                        && !canWrite)
                                {
                                    System.Console.WriteLine($"File {fileName}: {(canRead? "is readable": "is not readable")}; {(canWrite? "is writable": "is not writable")}");                                                
                                    continue;   
                                }
                            }
                            catch
                            {
                                System.Console.WriteLine($"{fileName} cannot be opened");
                                continue;
                            }

                            var fi = new FileInfo(fileName);

                            System.Console.WriteLine($"File found: '{fi.FullName}' - Size: {fi.Length} bytes.");

                            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
                            var sr = new StreamReader(fi.FullName, System.Text.Encoding.GetEncoding("shift_jis"));
                            var keys = sr.ReadLine().Split(',');
                            
                            while (!sr.EndOfStream)
                            {
                                var values = sr.ReadLine().Split(',');
                                var dic = keys.Zip(values, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);
                                var messageString = JsonConvert.SerializeObject(dic);
                                var message = new Message(Encoding.UTF8.GetBytes(messageString));

                                // Send the telemetry message
                                await client.SendEventAsync(message);
                                Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, messageString);
                                
                                //await Task.Delay(3 * 1000); // 3 sec interval
                            }
                            sr.Close();

                            var targetFullFilename = fi.FullName + RenameExtension;
                            
                            File.Move(fi.FullName, targetFullFilename);
                            System.Console.WriteLine($"Renamed '{fi.FullName}' to '{targetFullFilename}'");
                        }
                    }
                }
                catch (System.Exception ex)
                {  
                    System.Console.WriteLine($"Exception: {ex.Message}");
                }

                var fileMessage = new FileMessage
                {
                    counter = DateTime.Now.Millisecond
                };

                Thread.Sleep(Interval);
            }
        }

        public static int Interval {get; set;} = DefaultInterval;

        public static string RenameExtension {get; set;} = DefaultRenameExtension;

        public static string SearchPattern {get; set;} = DefaultSearchPattern;

        /// <summary>
        /// Call back function for updating the desired properties
        /// </summary>
        static async Task OnDesiredPropertiesUpdate(TwinCollection desiredProperties, object userContext)
        {
            Console.WriteLine("OnDesiredPropertiesUpdate started");

            var client = userContext as DeviceClient;

            if (desiredProperties == null)
            {
                Console.WriteLine("Empty desired properties ignored.");

                return;
            }

            try
            {
                var reportedProperties = new TwinCollection();

                if (desiredProperties.Contains("interval")) 
                {
                    if (desiredProperties["interval"] != null)
                    {
                        Interval = desiredProperties["interval"];
                    }
                    else
                    {
                        Interval = DefaultInterval;
                    }

                    Console.WriteLine($"Interval changed to '{Interval}'");

                    reportedProperties["interval"] = Interval;
                } 
                else
                {
                    Console.WriteLine($"Interval ignored");
                }

                if (desiredProperties.Contains("renameExtension")) 
                {
                    if (desiredProperties["renameExtension"] != null)
                    {
                        RenameExtension = desiredProperties["renameExtension"];
                    }
                    else
                    {
                        RenameExtension = DefaultRenameExtension;
                    }

                    Console.WriteLine($"RenameExtension changed to '{RenameExtension}'");

                    reportedProperties["renameExtension"] = RenameExtension;
                } 
                else
                {
                    Console.WriteLine($"RenameExtension ignored");
                }

                if (desiredProperties.Contains("searchPattern")) 
                {
                    if (desiredProperties["searchPattern"] != null)
                    {
                        SearchPattern = desiredProperties["searchPattern"];
                    }
                    else
                    {
                        SearchPattern = DefaultSearchPattern;
                    }

                    Console.WriteLine($"SearchPattern changed to '{SearchPattern}'");

                    reportedProperties["searchPattern"] = SearchPattern;
                } 
                else
                {
                    Console.WriteLine($"SearchPattern ignored");
                }

                if (reportedProperties.Count > 0)
                {
                    await client.UpdateReportedPropertiesAsync(reportedProperties).ConfigureAwait(false);
                }
            }
            catch (AggregateException ex)
            {
                Console.WriteLine($"Desired properties change error: {ex.Message}");
                
                foreach (Exception exception in ex.InnerExceptions)
                {
                    Console.WriteLine($"Error when receiving desired properties: {exception}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error when receiving desired properties: {ex.Message}");
            }
        }


    }

    public class FileMessage
    {
        public int counter { get; set; }
    }
}
