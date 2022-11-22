using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mime;
using System.Text;
using System.Text.RegularExpressions;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{
    public class DatabricksController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        public DatabricksController(IConfiguration config)
        {
            _config = config;
            dataLayer = new DataLayer.DataLayer(_config);

        }
        [HttpPost("CreateTable")]
        public async Task<string> CreateTable(string EngagementName, string foldername, string FileName)
        {
            string success = "Failed";
            try
            {
                string schemaname = dataLayer.CreateSchema(EngagementName);
                createNotebook(foldername, FileName, schemaname);
                string respjson = await CreateJob(FileName);
                RunJob(respjson);
                success = "Success";
                return success;
            }
            catch (Exception ex)
            {
                return success;
            }

        }
        public string createNotebook(string foldername, string FileName, string schemaname)
        {
            string success = "Failed";
            try
            {
                string notebookData = "";
                string trim = RemoveSpecialCharacters(Path.GetFileNameWithoutExtension(FileName).Replace(" ", ""));
                string mountpoint = _config.GetValue<string>("DatabricksConnection:Mountpoint");
                string externalTable = _config.GetValue<string>("DatabricksAPI:ExternalTablesPath");
                List<string> Header_Data = GetHeaderData(foldername, FileName);
                string delimitter = GetDelimitter(Header_Data.FirstOrDefault());

                notebookData = notebookData + "from pyspark.sql.functions import expr,col,to_date" + "\n\n";
                notebookData = notebookData + "spark.conf.set(\"spark.databricks.delta.formatCheck.enabled\", False)" + "\n\n";
                notebookData = notebookData + "file_location = \"" + mountpoint + foldername + "/" + FileName + "\"" + "\n\n";
                notebookData = notebookData + "df = spark.read.format(\"csv\").option(\"inferSchema\", \"true\").option(\"header\",\"true\")" +
                                            ".option(\"delimiter\", \"" + delimitter + "\").load(file_location)" + "\n\n";

                string query = PrepareQuery(Header_Data, delimitter);
                notebookData = notebookData + query + "\n\n";
                string ExternalTable = "df1.write.format(\"parquet\").mode(\"overwrite\").option(\"path\",\"" + externalTable + schemaname + "\").saveAsTable(\"" + schemaname + "." + "Tbl" + trim + "\")";
                notebookData = notebookData + ExternalTable;
                string workbookname = createWorkBook(notebookData, FileName);
                string encoded = GetEncodedText(workbookname);
                UploadWorkbook(encoded, FileName);
                success = "Success";
                return success;
            }
            catch (Exception ex)
            {
                return success;
            }

        }

        public List<string> GetHeaderData(string foldername, string FileName)
        {
            List<string> Header_Data = new List<string>();
            try
            {

                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                string filepath = foldername + "\\" + FileName;
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                BlobClient blob = containerClient.GetBlobClient(filepath);
                Stream blobstream = blob.OpenRead();
                StreamReader blobStreamReader = new StreamReader(blobstream);
                string header = blobStreamReader.ReadLine();
                string data = blobStreamReader.ReadLine();
                Header_Data.Add(header);
                Header_Data.Add(data);
                return Header_Data;
            }
            catch (Exception ex)
            {
                return Header_Data;
            }
        }

        public async void UploadWorkbook(string bytestream, string Filename)
        {
            try
            {
                var BaseAPI = _config.GetValue<string>("DatabricksAPI:DatabricksBaseAPI");
                var WorkbookUpload = _config.GetValue<string>("DatabricksAPI:Workbookupload");
                var accessToken = _config.GetValue<string>("DatabricksAPI:AccessToken");
                var workbookpath = _config.GetValue<string>("DatabricksAPI:Workbookpath");
                using (var httpClient = new HttpClient())
                {
                    var content = new MultipartFormDataContent();
                    content.Add(new StringContent(workbookpath + System.IO.Path.GetFileNameWithoutExtension(Filename) + ".py"), "path");
                    content.Add(new StringContent("SOURCE"), "format");
                    content.Add(new StringContent("PYTHON"), "language");
                    content.Add(new StringContent(bytestream), "content");
                    content.Add(new StringContent("true"), "overwrite");

                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);
                    using (var response = await httpClient.PostAsync(BaseAPI + WorkbookUpload, content))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();

                    }
                }
            }
            catch (Exception ex)
            {

            }
        }
        public async Task<string> RunJob(string jobidjson)
        {
            try
            {
                var BaseAPI = _config.GetValue<string>("DatabricksAPI:DatabricksBaseAPI");
                var RunJob = _config.GetValue<string>("DatabricksAPI:RunJob");
                var accessToken = _config.GetValue<string>("DatabricksAPI:AccessToken");

                string body = jobidjson;
                string resp = "";
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);
                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(BaseAPI + RunJob),
                        Content = new StringContent(body, Encoding.UTF8, MediaTypeNames.Application.Json),

                    };
                    using (var response = await httpClient.SendAsync(request))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();
                        resp = apiResponse;
                    }
                }
                return resp;
            }
            catch (Exception ex)
            {
                return string.Empty;
            }
        }
        public async Task<string> CreateJob(string filename)
        {
            string resp = "";
            try
            {
                var BaseAPI = _config.GetValue<string>("DatabricksAPI:DatabricksBaseAPI");
                var CreateJob = _config.GetValue<string>("DatabricksAPI:CreateJob");
                var accessToken = _config.GetValue<string>("DatabricksAPI:AccessToken");
                var workbookpath = _config.GetValue<string>("DatabricksAPI:Workbookpath");

                string body = "{" +
                                "\"name\": \"Job for " + filename + "\"," +
                                "\"tags\": {" +
                                "\"cost-center\": \"API\"," +
                                "\"team\": \"Adhoc jobs\"" +
                                "}," +
                                "\"tasks\": [" +
                                "{" +
                                "\"task_key\": \"job_for_list_path\"," +
                                "\"notebook_task\": {" +
                                "\"notebook_path\": \"/DevScripts/" + System.IO.Path.GetFileNameWithoutExtension(filename) + ".py" + "\"," +
                                "\"source\": \"WORKSPACE\"" +
                                "}," +
                                "\"job_cluster_key\": \"job_for_list_path_cluster\"," +
                                "\"timeout_seconds\": 0," +
                                "\"email_notifications\": { }" +
                                "}" +
                                "]," +
                                "\"job_clusters\": [" +
                                "{" +
                                "\"job_cluster_key\": \"job_for_list_path_cluster\"," +
                                "\"new_cluster\": {" +
                                "\"spark_version\": \"10.4.x-scala2.12\"," +
                                "\"spark_conf\": {" +
                                "\"spark.databricks.delta.preview.enabled\": \"true\"" +
                                "}," +
                                "\"azure_attributes\": {" +
                                "\"first_on_demand\": 1," +
                                "\"availability\": \"ON_DEMAND_AZURE\"," +
                                "\"spot_bid_max_price\": -1" +
                                "}," +
                                "\"node_type_id\": \"Standard_DS3_v2\"," +
                                "\"spark_env_vars\": {" +
                                "\"PYSPARK_PYTHON\": \"/databricks/python3/bin/python3\"" +
                                "}," +
                                "\"enable_elastic_disk\": true," +
                                "\"data_security_mode\": \"LEGACY_SINGLE_USER_STANDARD\"," +
                                "\"runtime_engine\": \"STANDARD\"," +
                                "\"num_workers\": 8" +
                                "}" +
                                "}" +
                                "]," +
                                "\"format\": \"MULTI_TASK\"" +
                                "}";


                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);
                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(BaseAPI + CreateJob),
                        Content = new StringContent(body, Encoding.UTF8, MediaTypeNames.Application.Json),

                    };
                    using (var response = await httpClient.SendAsync(request))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();
                        resp = apiResponse;
                    }
                }
                return resp;
            }
            catch (Exception ex)
            {
                return resp;
            }
        }
        public string GetDelimitter(string header)
        {
            try
            {
                string delimitter = "";
                if (header.Contains('|'))
                {
                    delimitter = "|";
                }
                else if (header.Contains(','))
                {
                    delimitter = ",";
                }
                return delimitter;
            }
            catch (Exception ex)
            {
                return "|";
            }
        }

        public string PrepareQuery(List<string> Data, string Delimitter)
        {
            try
            {
                string query = string.Empty;
                string header = Data[0];
                string tabledata = Data[1];
                string columns = "";

                string[] columnNames = header.Split(Delimitter);
                string[] columnValues = tabledata.Split(Delimitter);
                string[] datatype = new string[columnValues.Length];

                int counter = 0;
                foreach (string value in columnValues)
                {
                    datatype[counter] = ParseString(value);
                    counter++;
                }
                query = query + "df1=df.select(";
                for (int i = 0; i < columnNames.Length; i++)
                {
                    if (datatype[i] != "DateTime")
                    {
                        columns = columns + "col(\"" + columnNames[i] + "\")";
                    }
                    else
                    {
                        columns = columns + "to_date(col(\"" + columnNames[i] + "\"),\"M/dd/yyyy\").alias(\"" + columnNames[i] + "\")";
                    }
                    if (i < columnNames.Length - 1)
                    {
                        columns = columns + ",";
                    }
                }
                query = query + columns + ")";


                return query;
            }
            catch (Exception ex)
            {
                return string.Empty;
            }
        }

        public string createWorkBook(string text, string Filename)
        {
            try
            {
                string workbookname = Filename;
                var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(text);
                var logWriter = new System.IO.StreamWriter(workbookname);
                logWriter.BaseStream.Write(plainTextBytes);
                logWriter.Dispose();
                return workbookname;
            }
            catch (Exception ex)
            {
                return string.Empty;
            }

        }
        public string GetEncodedText(string notebookname)
        {
            try
            {
                string text = System.IO.File.ReadAllText(notebookname);
                var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(text);
                string encoded = System.Convert.ToBase64String(plainTextBytes);
                return encoded;
            }
            catch (Exception ex)
            {
                return string.Empty;
            }
        }
        private string ParseString(string str)
        {
            try
            {
                bool boolValue;
                Int32 intValue;
                Int64 bigintValue;
                double doubleValue;
                DateTime dateValue;

                // Place checks higher in if-else statement to give higher priority to type.

                if (bool.TryParse(str, out boolValue))
                    return "bool";
                else if (Int32.TryParse(str, out intValue))
                    return "int";
                else if (Int64.TryParse(str, out bigintValue))
                    return "int";
                else if (double.TryParse(str, out doubleValue))
                    return "double";
                else if (DateTime.TryParse(str, out dateValue))
                    return "DateTime";
                else return "string";
            }
            catch (Exception ex)
            {
                return string.Empty;
            }

        }
        public static string RemoveSpecialCharacters(string str)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char c in str)
            {
                if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_')
                {
                    sb.Append(c);
                }
            }
            return sb.ToString();
        }

    }
}
