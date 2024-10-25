using System;
using System.Threading.Tasks;
using System.Net.Http;
using Azure;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Azure.Messaging.EventGrid;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Azure.Core.Pipeline;
using System.Text;

namespace FactoryTwinIngestFunction
{
    public static class IOTHubToTwinsFunction
    {
        private static readonly string adtInstanceUrl = "https://FactoryDT.api.sea.digitaltwins.azure.net";
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("IOTHubToTwins")]
        public async static Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            if (adtInstanceUrl == null)
            {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            try
            {
                // Create a connection to Azure Digital Twins
                var credential = new ManagedIdentityCredential();
                var client = new DigitalTwinsClient(new Uri(adtInstanceUrl), credential, new DigitalTwinsClientOptions
                {
                    Transport = new HttpClientTransport(httpClient)
                });

                log.LogInformation("ADT service client connection created.");

                if (eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                    // Convert the incoming message into a JSON object
                    JObject? deviceMessage = JsonConvert.DeserializeObject<JObject>(eventGridEvent.Data.ToString());

                    if (deviceMessage != null)
                    {
                        // Get device ID and Base64 encoded body
                        string? deviceId = deviceMessage["systemProperties"]?["iothub-connection-device-id"]?.ToString();
                        string? bodyBase64 = deviceMessage["body"]?.ToString();

                        if (deviceId != null && bodyBase64 != null)
                        {
                            // Decode the Base64 string
                            byte[] data = Convert.FromBase64String(bodyBase64);
                            string decodedString = Encoding.UTF8.GetString(data);

                            // Parse the decoded string as JSON
                            JObject? decodedMessage = JsonConvert.DeserializeObject<JObject>(decodedString);

                            if (decodedMessage != null)
                            {
                                log.LogInformation($"Device ID: {deviceId}, Data: {decodedMessage}");

                                // Create a patch document for updating the digital twin
                                var updateTwinData = new JsonPatchDocument();

                                // Check if the deviceId is for lorry attributes and map it to the respective digital twin lorry
                                if (deviceId.StartsWith("lorry_"))
                                {
                                    string twinId = "";
                                    string propertyToUpdate = "";

                                    // Map the deviceId to the corresponding lorry twin and attribute
                                    if (deviceId.Contains("lorry_1_capacity"))
                                    {
                                        twinId = "lorry_1";
                                        propertyToUpdate = "/lorry_1_capacity";
                                    }
                                    else if (deviceId.Contains("lorry_1_delay"))
                                    {
                                        twinId = "lorry_1";
                                        propertyToUpdate = "/lorry_1_delay";
                                    }
                                    else if (deviceId.Contains("lorry_2_capacity"))
                                    {
                                        twinId = "lorry_2";
                                        propertyToUpdate = "/lorry_2_capacity";
                                    }
                                    else if (deviceId.Contains("lorry_2_delay"))
                                    {
                                        twinId = "lorry_2";
                                        propertyToUpdate = "/lorry_2_delay";
                                    }
                                    else if (deviceId.Contains("lorry_1_2_capacity"))
                                    {
                                        twinId = "lorry_1_2";
                                        propertyToUpdate = "/lorry_1_2_capacity";
                                    }
                                    else if (deviceId.Contains("lorry_1_2_delay"))
                                    {
                                        twinId = "lorry_1_2";
                                        propertyToUpdate = "/lorry_1_2_delay";
                                    }
                                    else if (deviceId.Contains("lorry_2_3_capacity"))
                                    {
                                        twinId = "lorry_2_3";
                                        propertyToUpdate = "/lorry_2_3_capacity";
                                    }
                                    else if (deviceId.Contains("lorry_2_3_delay"))
                                    {
                                        twinId = "lorry_2_3";
                                        propertyToUpdate = "/lorry_2_3_delay";
                                    }

                                    // Check if a twinId and propertyToUpdate were found, then update the twin
                                    if (!string.IsNullOrEmpty(twinId) && !string.IsNullOrEmpty(propertyToUpdate))
                                    {
                                        // Apply the update to the corresponding property
                                        double value = decodedMessage[deviceId].Value<double>();
                                        updateTwinData.AppendReplace(propertyToUpdate, value);

                                        log.LogInformation($"Applying patch to {twinId}, updating {propertyToUpdate}");

                                        // Update the digital twin
                                        await client.UpdateDigitalTwinAsync(twinId, updateTwinData);
                                        log.LogInformation($"Digital twin '{twinId}' updated successfully with {propertyToUpdate}.");
                                    }
                                    else
                                    {
                                        log.LogWarning($"Unrecognized lorry device ID '{deviceId}'");
                                    }
                                }
                                else
                                {
                                    // Handle non-lorry devices
                                    switch (deviceId)
                                    {
                                        case "export_warehouse_1":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["export_warehouse_1"].Value<double>());
                                            log.LogInformation("Applying patch to export_warehouse_1");
                                            break;
                                        case "import_warehouse_1":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["import_warehouse_1"].Value<double>());
                                            log.LogInformation("Applying patch to import_warehouse_1");
                                            break;
                                        case "out_warehouse_2":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["out_warehouse_2"].Value<double>());
                                            log.LogInformation("Applying patch to out_warehouse_2");
                                            break;
                                        case "export_warehouse_2":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["export_warehouse_2"].Value<double>());
                                            log.LogInformation("Applying patch to export_warehouse_2");
                                            break;
                                        case "export_warehouse_2_3":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["export_warehouse_2_3"].Value<double>());
                                            log.LogInformation("Applying patch to export_warehouse_2_3");
                                            break;
                                        case "export_warehouse_1_2":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["export_warehouse_1_2"].Value<double>());
                                            log.LogInformation("Applying patch to export_warehouse_1_2");
                                            break;
                                        case "out_warehouse_3a":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["out_warehouse_3a"].Value<double>());
                                            log.LogInformation("Applying patch to out_warehouse_3a");
                                            break;
                                        case "out_warehouse_3b":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["out_warehouse_3b"].Value<double>());
                                            log.LogInformation("Applying patch to out_warehouse_3b");
                                            break;
                                        case "out_warehouse_4":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["out_warehouse_4"].Value<double>());
                                            log.LogInformation("Applying patch to out_warehouse_4");
                                            break;
                                        case "import_warehouse_1_2":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["import_warehouse_1_2"].Value<double>());
                                            log.LogInformation("Applying patch to import_warehouse_1_2");
                                            break;
                                        case "import_warehouse_2":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["import_warehouse_2"].Value<double>());
                                            log.LogInformation("Applying patch to import_warehouse_2");
                                            break;
                                        case "import_warehouse_2_3":
                                            updateTwinData.AppendReplace("/currentStorage", decodedMessage["import_warehouse_2_3"].Value<double>());
                                            log.LogInformation("Applying patch to import_warehouse_2_3");
                                            break;
                                        case "machine_1":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_1"].Value<double>());
                                            log.LogInformation("Applying patch to machine_1");
                                            break;
                                        case "machine_2":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_2"].Value<double>());
                                            log.LogInformation("Applying patch to machine_2");
                                            break;
                                        case "machine_1_2":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_1_2"].Value<double>());
                                            log.LogInformation("Applying patch to machine_1_2");
                                            break;
                                        case "machine_3":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_3"].Value<double>());
                                            log.LogInformation("Applying patch to machine_3");
                                            break;
                                        case "machine_1_2_3":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_1_2_3"].Value<double>());
                                            log.LogInformation("Applying patch to machine_1_2_3");
                                            break;
                                        case "machine_2_3":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_2_3"].Value<double>());
                                            log.LogInformation("Applying patch to machine_2_3");
                                            break;
                                        case "machine_4":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_4"].Value<double>());
                                            log.LogInformation("Applying patch to machine_4");
                                            break;
                                        case "machine_2_3_4":
                                            updateTwinData.AppendReplace("/productionRate", decodedMessage["machine_2_3_4"].Value<double>());
                                            log.LogInformation("Applying patch to machine_2_3_4");
                                            break;
                                        case "revenueCost":
                                            updateTwinData.AppendReplace("/Cost", decodedMessage["revenueCost"].Value<double>());
                                            log.LogInformation("Applying patch to revenueCost");
                                            break;
                                        case "storageCost":
                                            updateTwinData.AppendReplace("/Cost", decodedMessage["storageCost"].Value<double>());
                                            log.LogInformation("Applying patch to storageCost");
                                            break;
                                        case "energyCost":
                                            updateTwinData.AppendReplace("/Cost", decodedMessage["energyCost"].Value<double>());
                                            log.LogInformation("Applying patch to energyCost");
                                            break;
                                        default:
                                            log.LogWarning($"Device ID '{deviceId}' not recognized.");
                                            break;
                                    }
                                }

                                // Update the digital twin based on the device ID
                                await client.UpdateDigitalTwinAsync(deviceId, updateTwinData);
                                log.LogInformation($"Digital twin '{deviceId}' updated successfully.");
                            }
                            else
                            {
                                log.LogWarning("Decoded message is null or not in expected format.");
                            }
                        }
                        else
                        {
                            log.LogWarning("Device ID or body is null or not in expected format.");
                        }
                    }
                    else
                    {
                        log.LogWarning("Device message is null or not in expected format.");
                    }
                }
            }
            catch (Exception ex)
            {
                log.LogError($"Error in ingest function: {ex.Message}");
            }
        }
    }
}
