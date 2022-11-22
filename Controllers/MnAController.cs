using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using Deloitte.MnANextGenAnalytics.WebAPI.DataLayer;
using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using Deloitte.MnANextGenAnalytics.WebAPI.Mails;
using Deloitte.MnANextGenAnalytics.WebAPI.Controllers;
using Azure.Storage.Blobs;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MnAController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        public MnAController(IConfiguration config)
        {
            _config = config;
            dataLayer = new DataLayer.DataLayer(config);
        }
        [HttpPost]
        public ActionResult SaveEngagementDetails(EngagementDetails data)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config);

                int result;
                string folderurl = "";

                if ((data.id != null) && (data.id > 0))
                {
                    result = dataLayer.UpdateEngagementDetails(data);
                    if(result != -1)
                        SendEmail(data);
                    return CreatedAtAction(nameof(SaveEngagementDetails), result);
                }
                else
                {
                    result = dataLayer.SaveEngagementDetails(data);
                    if (result != -1)
                        SendEmail(data);
                    return CreatedAtAction(nameof(SaveEngagementDetails), result);
                }
            }
            catch(Exception ex)
            {
                string errorjson = "{" + "\"Name\":\"Error\",\"value\":\"" + ex.Message + "\" }";
                return CreatedAtAction(nameof(SaveEngagementDetails), errorjson);
            }
            
        }
        [HttpPost("InitialsubmitApprove")]
        public ActionResult InitialsubmitApprove(EngagementDetails data)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config);

                int result;
                string folderurl = "";
                string json = "";

                if ((data.id != null) && (data.id > 0))
                {
                    result = dataLayer.UpdateEngagementDetails(data);
                    if (result != -1)
                    {
                        SendEmail(data);
                        ADLSController aDLSController = new ADLSController(_config);
                        folderurl = aDLSController.CreateFolder(data.engagementname, data.wbscode, data.id);
                        json = "{" + "\"Name\":\"" + data.engagementname + "-" + data.wbscode + "\",\"value\":\"" + folderurl + "\" }";
                    }

                }
                return CreatedAtAction(nameof(InitialsubmitApprove), json);
            }
            catch (Exception ex)
            {
                string errorjson = "{" + "\"Name\":\"Error\",\"value\":\"" + ex.Message + "\" }";
                return CreatedAtAction(nameof(InitialsubmitApprove), errorjson);
            }

         }
      
        // GET: api/<MnAController>
        [HttpGet]
        public IEnumerable<EngagementPortfolio> Get()
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config);
                List<EngagementPortfolio> Portfolio = dataLayer.GetOfferingPortfolio();
                return Portfolio;
            }
            catch(Exception ex)
            {
                return null;
            }
            //return new string[] { "HealthCare", "MedicalCare" };
        }

        // GET api/<MnAController>/5
        [HttpGet("{criteria}")]
        public async Task<string> Get(string criteria)
        {
            try
            {
                string result = string.Empty;
                var token = "";
                token = await Token();
                result = (string)await GetProfile(criteria, token);
                return result;
            }
            catch(Exception ex)
            {
                return string.Empty;
            }
        }
        
        [HttpGet("CheckUserAccess")]
        public async Task<bool> CheckUserAccess(int EngagementId,string EmailId)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config);
                var data = dataLayer.CheckUserAccess(EngagementId, EmailId);
                return data;
            }
            catch(Exception ex)
            {
                return false;
            }
        }

        [HttpGet("CheckEngagementAccess")]
        public async Task<bool> EngagementAccessibleToUser(int EngagementId, string EmailId)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config);
                var data = dataLayer.CheckEngagementAccess(EngagementId, EmailId);
                return data;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        [HttpGet("GetEngagementDetails")]
        public async Task<EngagementData> GetEngagementData(int EngagementId)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config);
                EngagementData data = dataLayer.GetEngagementData(EngagementId);
                return data;
            }
            catch(Exception ex)
            {
                return null;
            }
        }

        [HttpGet("GetAllEngagements")]
        public async Task<List<DashboardData>> GetAllEngagements(string EmailId)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config);
                List<DashboardData> data = dataLayer.GetAllEngagements(EmailId);
                return data;
            }
            catch(Exception ex)
            {
                return null;
            }
        }

        private async Task<Object> GetProfile(string criteria, string token)
        {
            string result = string.Empty;
            var graphapi = "";
            var graphApiUrl = _config.GetValue<string>("AzureDetails:GraphApiUrl");
            HttpResponseMessage httpResponseMessage;

            try
            {
                using (HttpClient client = new HttpClient())
                {
                    if (!criteria.Contains("@"))
                    {
                        graphapi = String.Concat(graphApiUrl, "v1.0/users?$search= \"displayName:{0}\" OR \"mail:{0}\" &$filter=endsWith(mail,'@deloitte.com') &$orderby = displayName &$count = true &$top=5");
                    }
                    else
                    {
                        graphapi = String.Concat(graphApiUrl, "v1.0/users?$filter=(mail eq '{0}' or startswith(mail,'{0}') )and endsWith(mail,'@deloitte.com')&$count=true &$top=5");
                    }
                    string url = string.Format(graphapi, criteria);
                    HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, url);
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                    request.Headers.Add("ConsistencyLevel", "eventual");
                    httpResponseMessage = await client.SendAsync(request).ConfigureAwait(false);
                }
                if (httpResponseMessage != null)
                {
                    if (httpResponseMessage.IsSuccessStatusCode)
                    {
                        result = await GetProfileURLs(httpResponseMessage, token).ConfigureAwait(false);
                    }
                }
                else
                {
                    throw new Exception("Graph api service call failed. \n" + httpResponseMessage.ReasonPhrase);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Graph api service call failed. \n" + ex.Message);
            }
            return result;
        }
        private static async Task<string> GetProfileURLs(HttpResponseMessage httpResponse, string token)
        {
            string result = string.Empty;
            try
            {
                result = httpResponse.Content.ReadAsStringAsync().Result;
                ProfileResponse profileResponse = JsonConvert.DeserializeObject<ProfileResponse>(result);
                if (profileResponse.value.Count > 0)
                {
                    List<ProfileBatchRequest> profileBatchRequest = new List<ProfileBatchRequest>();
                    int counter = 1;
                    for (int i = 0; i < profileResponse.value.Count; i++)
                    {
                        ProfileBatchRequest profile = new ProfileBatchRequest();
                        profile.id = Convert.ToString(counter++);
                        profile.method = "GET";
                        profile.url = string.Format("/users/{0}/photos/48x48/$value", profileResponse.value[i].mail);
                        profileBatchRequest.Add(profile);
                    }
                    result = await BindProfileURLs(profileBatchRequest, token, profileResponse).ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                throw new Exception("Graph api service call failed. \n" + httpResponse.ReasonPhrase);
            }
            return result;
        }
        private static async Task<string> BindProfileURLs(List<ProfileBatchRequest> profileBatchRequest, string token, ProfileResponse profileResponse)
        {
            string result = string.Empty;
            var graphProfileApiUrl = _config.GetValue<string>("AzureDetails:GraphProfileApiUrl");
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Add("Authorization", "Bearer " + token);
                    var strrequest = JsonConvert.SerializeObject(profileBatchRequest);
                    var jsonRequest = "{'requests':" + strrequest + " }";
                    StringContent content = new StringContent(jsonRequest, System.Text.Encoding.UTF8, "application/json");
                    using (var response = await client.PostAsync(graphProfileApiUrl, content).ConfigureAwait(false))
                    {
                        var graphProfileResult = response.Content.ReadAsStringAsync().Result;
                        ProfileBatchResponse profileBatchResponse = JsonConvert.DeserializeObject<ProfileBatchResponse>(graphProfileResult);
                        var sortedResponse = profileBatchResponse?.responses?.OrderBy(x => x.id).ToArray();
                        for (int i = 0; i < profileBatchResponse.responses.Count; i++)
                        {
                            profileResponse.value[i].profilepicture = sortedResponse[i].body;
                        }
                        result = JsonConvert.SerializeObject(profileResponse);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Graph api service call failed. \n" + ex.Message);
            }
            return result;
        }
        private async Task<string> Token()
        {
            try
            {
                var client = new HttpClient();
                var clientId = _config.GetValue<string>("AzureDetails:ClientId");
                var clientSecret = _config.GetValue<string>("AzureDetails:ClientSecret");
                var username = _config.GetValue<string>("AzureDetails:ReportServerUsername");
                var graphApiUrl = _config.GetValue<string>("AzureDetails:GraphApiUrl");
                var aadInstance = _config.GetValue<string>("AzureDetails:ida:AADInstanc");
                var Url = String.Concat(aadInstance, "/oauth2/v2.0/token");
                if (!username.Contains("@")) { username = username + _config.GetValue<string>("AzureDetails:DeloitteEmailDomain"); }
                var password = GetKeyVault(_config.GetValue<string>("AzureDetails:ReportServerUsername"));
                List<KeyValuePair<string, string>> request = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("client_id", clientId),
                new KeyValuePair<string, string>("grant_type", "password"),
                new KeyValuePair<string, string>("username",username ),
                new KeyValuePair<string, string>("password",password ),
                new KeyValuePair<string, string>("client_secret", clientSecret),
                new KeyValuePair<string, string>("scope", String.Concat(graphApiUrl,".default"))
            };
                var content = new FormUrlEncodedContent(request);
                var response = await client.PostAsync(Url, content);
                var result = await response.Content.ReadAsStringAsync();
                TokenResponse tokenResponse = JsonConvert.DeserializeObject<TokenResponse>(result);
                return tokenResponse.access_token;
            }
            catch(Exception ex)
            {
                return string.Empty;
            }
        }
        public class TokenResponse
        {
            public string token_type { get; set; }
            public string scope { get; set; }
            public string access_token { get; set; }
        }
        public class ProfileBatchRequest
        {
            public string id { get; set; }
            public string method { get; set; }
            public string url { get; set; }
        }
        public class ProfileResponse
        {
            public string OdataContext { get; set; }
            public int OdataCount { get; set; }
            public List<Value> value { get; set; }
        }
        public class Value
        {
            public List<string> businessPhones { get; set; }
            public string displayName { get; set; }
            public string givenName { get; set; }
            public string jobTitle { get; set; }
            public string mail { get; set; }
            public string mobilePhone { get; set; }
            public string officeLocation { get; set; }
            public object preferredLanguage { get; set; }
            public string surname { get; set; }
            public string userPrincipalName { get; set; }
            public string id { get; set; }
            public string profilepicture { get; set; }
        }
        public class ProfileBatchResponse
        {
            public List<Responses> responses { get; set; }
        }
        public class Headers
        {
            public string CacheControl { get; set; }
            public string XContentTypeOptions { get; set; }
            public string ContentType { get; set; }
            public string ETag { get; set; }
        }

        public class Responses
        {
            public string id { get; set; }
            public int status { get; set; }
            public Headers headers { get; set; }
            public string body { get; set; }
        }
        private static string GetKeyVault(string ServiceAccount)
        {
            try
            {
                string keyVaultURL = _config.GetValue<string>("AzureDetails:AzureKeyVaultURL");
                KeyVaultClient kvc = null;
                kvc = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(GetToken));

                SecretBundle secret = Task.Run(() => kvc.GetSecretAsync(keyVaultURL +
                    @"/secrets/" + ServiceAccount)).ConfigureAwait(false).GetAwaiter().GetResult();
                return secret.Value;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private static async Task<string> GetToken(string authority, string resource, string scope)
        {
            try
            {
                string CLIENTID = _config.GetValue<string>("AzureDetails:ClientId");
                string CLIENTSECRET = _config.GetValue<string>("AzureDetails:ClientSecret");
                var authContext = new Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext(authority);
                Microsoft.IdentityModel.Clients.ActiveDirectory.ClientCredential clientCred = new Microsoft.IdentityModel.Clients.ActiveDirectory.ClientCredential(CLIENTID, CLIENTSECRET);
                Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationResult result = await authContext.AcquireTokenAsync(resource, clientCred);

                if (result == null)
                    throw new InvalidOperationException("Failed to obtain the JWT token");

                return result.AccessToken;
            }
            catch (Exception ex)
            {
              return null;
            }
        }
        // POST api/<MnAController>
        //[HttpPost]
        //public void Post([FromBody] string value)
        //{

        //}

        // PUT api/<MnAController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/<MnAController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        // POST api/<SendEmail>
        [HttpPost("SendEmail")]
        public string SendEmail(EngagementDetails engagementDetails)
        {
            try
            {
                SMTPMail sMTPMail = new SMTPMail();
                sMTPMail.Send(engagementDetails);
                return "Success";
            }
            catch(Exception ex)
            {
                return "failure";
            }
        }

        [HttpGet("GetFiles")]
        public List<string> GetFiles(string EngagementName, string WBSCode, int EngagementId, string username)
        {
            try
            {
                string Foldername = EngagementName + "-" + WBSCode;
                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                var blobs = containerClient.GetBlobs();
                List<string> names = blobs.Where(x => x.Name.Contains(Foldername + "/")).Select(x => x.Name.Replace(Foldername + "/", "")).ToList();
                dataLayer.AddDatasets(names, EngagementId, username);
                return names;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
    }
}
