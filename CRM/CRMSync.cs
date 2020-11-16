using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using MigrateEmailAndPhoneCallWPF.Properties;
using MigrateEmailAndPhoneCallWPF.UserControls;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Resources;
using System.Security;
using System.ServiceModel.Description;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Services.Configuration;

namespace MigrateEmailAndPhoneCallWPF.CRM
{
    public class CRMSync
    {
        private IOrganizationService serviceProxy;
        public CrmServiceClient conn;
        string pEnv = "";
        #region CRM Connection

        public bool StartConnection(string pEnvironment,bool pIsOSC)
        {
            try
            {
                pEnv = pEnvironment;

                ClientCredentials credentials = new ClientCredentials();
                Uri OrganizationUri;
                switch (pEnvironment.ToLower().Trim())
                {
                    case "dev":
                        {
                            if (pIsOSC)
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.CRMOrgServiceDev);

                                string clientId = "db80b2c6-bfdc-49af-9627-f63b73b85ada";
                                string clientSecret = "PBTRXCa6lnE_-G6M5S-e8H5T0ek5.A3E-D";

                                conn = new CrmServiceClient($@"AuthType=ClientSecret;url={OrganizationUri};ClientId={clientId};ClientSecret={clientSecret};
                                RedirectUri={new Uri(Settings.Default.CRMDevURI)}");
                            }
                            else
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.InsideSalesOrgServ);
                                conn = new CrmServiceClient($@"Url={OrganizationUri}; Username={Settings.Default.UsernameIS}; Password={Settings.Default.PasswordIS}; authtype=Office365");
                            }
                            break;
                        }
                    case "test":
                        {       
                            if (pIsOSC)
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.CRMOrgServiceTest);

                                string clientId = "1e2c9ca9-07c3-4c3a-bddd-bebeda6ec422";
                                string clientSecret = "Yo-_h5.1bQxu3-r871PzO7.v~g~t8Z_e1y";

                                conn = new CrmServiceClient($@"AuthType=ClientSecret;url={OrganizationUri};ClientId={clientId};ClientSecret={clientSecret};
                                RedirectUri={new Uri(Settings.Default.CRMTestURI)}");
                            }
                            else
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.InsideSalesOrgServ);

                                conn = new CrmServiceClient($@"Url={OrganizationUri}; Username={Settings.Default.UsernameIS}; Password={Settings.Default.PasswordIS}; authtype=Office365");
                            }
                            break;
                        }
                    case "uat":
                        {
                            if (pIsOSC)
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.CRMOrgServiceUAT);

                                string clientId = "1bd4fd2a-b4da-4129-8983-9ba8cce841ed";
                                string clientSecret = "kz8-.e4Kf.lt~MgMw2p4r5h.BRPCSyUfb8";

                                conn = new CrmServiceClient($@"AuthType=ClientSecret;url={OrganizationUri};ClientId={clientId};ClientSecret={clientSecret};
                                RedirectUri={new Uri(Settings.Default.CRM_UAT_URI)}");
                            }
                            else
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.InsideSalesOrgServ);

                                conn = new CrmServiceClient($@"Url={OrganizationUri}; Username={Settings.Default.UsernameIS}; Password={Settings.Default.PasswordIS}; authtype=Office365");
                            }
                            break;
                        }
                    case "stg":
                        {
                            if (pIsOSC)
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.CRMOrgServiceSTG);

                                string clientId = "09989613-3e13-464e-a674-37a081239d80";
                                string clientSecret = "qwq0Sglg..3iGKulPm9Vx_L5Op~8hk5oif";

                                conn = new CrmServiceClient($@"AuthType=ClientSecret;url={OrganizationUri};ClientId={clientId};ClientSecret={clientSecret};
                                RedirectUri={new Uri(Settings.Default.CRM_STG_URI)}");                               
                            }
                            else
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.InsideSalesOrgServ);

                                conn = new CrmServiceClient($@"Url={OrganizationUri}; Username={Settings.Default.UsernameIS}; Password={Settings.Default.PasswordIS}; authtype=Office365");                               
                            }
                            break;
                        }
                    case "mock":
                        {
                            if (pIsOSC)
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.CRMOrgServiceMock);

                                string clientId = "a4968f5c-6f3d-46ed-bb93-469dfc07756d";
                                string clientSecret = "NNc9I9A92r0_6JmH8F-4rR-0ko-7dy-59N";

                                conn = new CrmServiceClient($@"AuthType=ClientSecret;url={OrganizationUri};ClientId={clientId};ClientSecret={clientSecret};
                                RedirectUri={new Uri(Settings.Default.CRM_MOCK_URI)}");
                            }
                            else
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.InsideSalesOrgServ);

                                conn = new CrmServiceClient($@"Url={OrganizationUri}; Username={Settings.Default.UsernameIS}; Password={Settings.Default.PasswordIS}; authtype=Office365");
                            }
                            break;
                        }
                    case "prod":
                        {
                            if (pIsOSC)
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.CRMOrgServiceProd);

                                string clientId = "5b422160-3c60-46c2-8916-8a77935847dc";
                                string clientSecret = "TS_NbWb_2fRNz_jA8gvfGa60.lr94SfN-3";

                                conn = new CrmServiceClient($@"AuthType=ClientSecret;url={OrganizationUri};ClientId={clientId};ClientSecret={clientSecret};
                                RedirectUri={new Uri(Settings.Default.CRM_Prod_URI)}");
                            }
                            else
                            {
                                //Organization URL
                                OrganizationUri = new Uri(Settings.Default.InsideSalesOrgServProd);

                                conn = new CrmServiceClient($@"Url={OrganizationUri}; Username={Settings.Default.UsernameISProd}; Password={Settings.Default.PasswordISProd}; authtype=Office365");
                            }
                            break;
                        }
                    default:
                        {
                            return false;
                        }
                }

                serviceProxy = conn.OrganizationWebProxyClient != null ? conn.OrganizationWebProxyClient : (IOrganizationService)conn.OrganizationServiceProxy;

                return conn.IsReady;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool EndConnection()
        {
            try
            {
                if (conn.OrganizationServiceProxy != null)
                {
                    conn.OrganizationServiceProxy.ServiceChannel.Close();
                    conn.OrganizationServiceProxy.ServiceChannel.Dispose();
                }
                if (conn.OrganizationWebProxyClient != null)
                {
                    conn.OrganizationWebProxyClient.InnerChannel.Close();
                    conn.OrganizationWebProxyClient.InnerChannel.Dispose();
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        #endregion

        #region CRM Function Calls

        public (ConcurrentBag<UpsertResponse>, ConcurrentBag<ProcessError>, ConcurrentBag<RecordMissingInfo>,
            ConcurrentBag<RecordMissingInfo>) UpsertPhoneCallRecords(CrmServiceClient svc, List<PhoneCallCRM> pPhoneCalls, int maxDegreeOfParallelism)
        {
            var upsertR = new ConcurrentBag<UpsertResponse>();
            var processE = new ConcurrentBag<ProcessError>();
            var recordMI = new ConcurrentBag<RecordMissingInfo>();
            var missingRegarding = new ConcurrentBag<RecordMissingInfo>();

            Parallel.ForEach(pPhoneCalls,
                new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism },
                () =>
                {
                    //Clone the CrmServiceClient for each thread
                    return svc.Clone();
                },
                (pPhoneCall, loopState, index, threadLocalSvc) =>
                {
                    // In each thread, create entities and add them to the ConcurrentBag
                    // as EntityReferences
                    try
                    {
                        RecordMissingInfo r = new RecordMissingInfo() { RecordGuid = pPhoneCall.ActivityID };

                        Entity phoneCallEntity = new Entity("phonecall");
                        Entity temp;
                        Guid tempGuid;
                        List<Entity> tEntityA = new List<Entity>();
                        string tempId = "";

                        if (pPhoneCall.From.Count > 0)
                        {
                            foreach (var item in pPhoneCall.From)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);

                                        //temp["partyid"] = new EntityReference(item.EntityName, "cci_highlevelcompanynumber", item.HighLevelCompany);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing From {item.EntityName} {tempId},"; }
                            }

                            if (tEntityA.Count > 0) { phoneCallEntity["from"] = tEntityA.ToArray(); }
                            //else { r.InfoMissing += "Missing From, "; }
                        }
                        else { r.InfoMissing += "Missing From, "; }
                        tEntityA.Clear();

                        if (pPhoneCall.To.Count > 0)
                        {
                            foreach (var item in pPhoneCall.To)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);

                                        //temp["partyid"] = new EntityReference(item.EntityName, "cci_highlevelcompanynumber", item.HighLevelCompany);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing To {item.EntityName} {tempId},"; }
                            }

                            if (tEntityA.Count > 0)
                            {
                                phoneCallEntity["to"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing To, "; }
                        }
                        else { r.InfoMissing += "Missing To, "; }
                        tEntityA.Clear();

                        phoneCallEntity["subject"] = pPhoneCall.Subject;

                        phoneCallEntity["description"] = pPhoneCall.Description;

                        phoneCallEntity["phonenumber"] = pPhoneCall.PhoneNumber;

                        phoneCallEntity["directioncode"] = pPhoneCall.Direction;

                        if (pPhoneCall.LineOfBusiness != 0)
                        {
                            phoneCallEntity["cci_lineofbusiness"] = new OptionSetValue(pPhoneCall.LineOfBusiness);
                        }

                        if (pPhoneCall.Duration != null)
                        {
                            phoneCallEntity["actualdurationminutes"] = pPhoneCall.Duration;
                        }

                        if (pPhoneCall.CreatedOn != null)
                        {
                            phoneCallEntity["overriddencreatedon"] = pPhoneCall.CreatedOn;
                        }

                        try
                        {
                            if (pPhoneCall.Regarding != null)
                            {
                                if (pPhoneCall.Regarding.HighLevelCompany != "" && pPhoneCall.Regarding.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pPhoneCall.Regarding.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    tempId = pPhoneCall.Regarding.HighLevelCompany;

                                    phoneCallEntity["regardingobjectid"] = new EntityReference(pPhoneCall.Regarding.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);

                                    //phoneCallEntity["partyid"] = new EntityReference(pPhoneCall.Regarding.EntityName, "cci_highlevelcompanynumber", pPhoneCall.Regarding.HighLevelCompany);
                                }
                                else
                                {
                                    tempId = pPhoneCall.Regarding.RecordGuid.ToString();

                                    tempGuid = threadLocalSvc.Retrieve(pPhoneCall.Regarding.EntityName, pPhoneCall.Regarding.RecordGuid, new ColumnSet(new string[] { pPhoneCall.Regarding.EntityName + "id" })).Id;
                                    phoneCallEntity["regardingobjectid"] = new EntityReference(pPhoneCall.Regarding.EntityName, tempGuid);
                                }
                            }
                        }
                        catch (Exception) { r.InfoMissing += $"Missing Regarding: {pPhoneCall.Regarding.EntityName} {tempId}, "; }

                        try
                        {
                            if (pPhoneCall.OwnerId != null)
                            {
                                if (pPhoneCall.OwnerId.HighLevelCompany != "" && pPhoneCall.OwnerId.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pPhoneCall.OwnerId.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    phoneCallEntity["ownerid"] = new EntityReference(pPhoneCall.OwnerId.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                }
                                else
                                {
                                    tempGuid = threadLocalSvc.Retrieve(pPhoneCall.OwnerId.EntityName, pPhoneCall.OwnerId.RecordGuid, new ColumnSet(new string[] { pPhoneCall.OwnerId.EntityName + "id" })).Id;
                                    phoneCallEntity["ownerid"] = new EntityReference(pPhoneCall.OwnerId.EntityName, tempGuid);
                                }
                            }
                            else
                            {
                                phoneCallEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                            }
                        }
                        catch (Exception)
                        {
                            phoneCallEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                        }

                        phoneCallEntity.Id = pPhoneCall.ActivityID;

                        UpsertRequest request = new UpsertRequest()
                        {
                            Target = phoneCallEntity
                        };

                        UpsertResponse rps = (UpsertResponse)threadLocalSvc.Execute(request);
                        upsertR.Add(rps);

                        if (r.InfoMissing.Trim().Length > 0) { recordMI.Add(r); }
                    }
                    catch (Exception ex)
                    {
                        processE.Add(new ProcessError
                        {
                            RecordGuid = pPhoneCall.ActivityID,
                            ErrorMessage = ex.Message
                        });
                    }

                    return threadLocalSvc;
                },
                (threadLocalSvc) =>
                {
                    //Dispose the cloned CrmServiceClient instance
                    if (threadLocalSvc != null)
                    {
                        threadLocalSvc.Dispose();
                    }
                });

            //Return the ConcurrentBag of EntityReferences
            return (upsertR, processE, recordMI, missingRegarding);
        }

        public (ConcurrentBag<UpsertResponse>, ConcurrentBag<ProcessError>, ConcurrentBag<RecordMissingInfo>, 
            ConcurrentBag<RecordMissingInfo>) UpsertEmailRecords(CrmServiceClient svc, List<EmailCRM> pEmails, int maxDegreeOfParallelism)
        {
            var upsertR = new ConcurrentBag<UpsertResponse>();
            var processE = new ConcurrentBag<ProcessError>();
            var recordMI = new ConcurrentBag<RecordMissingInfo>();
            var missingRegarding = new ConcurrentBag<RecordMissingInfo>();

            Parallel.ForEach(pEmails,
                new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism },
                () =>
                {
                    //Clone the CrmServiceClient for each thread
                    return svc.Clone();
                },
                (pEmail, loopState, index, threadLocalSvc) =>
                {
                    try
                    {
                        RecordMissingInfo r = new RecordMissingInfo() { RecordGuid = pEmail.ActivityID };

                        Entity emailEntity = new Entity("email");
                        Entity temp;
                        Guid tempGuid;
                        List<Entity> tEntityA = new List<Entity>();
                        string tempId = "";

                        if (pEmail.From != null && pEmail.From.Count > 0)
                        {
                            foreach (var item in pEmail.From)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing From: {item.EntityName} {tempId}, "; }
                            }

                            if (tEntityA.Count > 0)
                            {
                                emailEntity["from"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing From, "; }
                        }
                        else { r.InfoMissing += "Missing From, "; }
                        tEntityA.Clear();

                        if (pEmail.To != null && pEmail.To.Count > 0)
                        {
                            foreach (var item in pEmail.To)
                            {

                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing To: {item.EntityName} {tempId}, "; }
                            }
                            if (tEntityA.Count > 0)
                            {
                                emailEntity["to"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing To, "; }
                        }
                        else { r.InfoMissing += "Missing To, "; }
                        tEntityA.Clear();

                        if (pEmail.CC != null && pEmail.CC.Count > 0)
                        {
                            foreach (var item in pEmail.CC)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing CC {item.EntityName} {tempId}, "; }
                            }
                            if (tEntityA.Count > 0)
                            {
                                emailEntity["cc"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing CC, "; }
                        }
                        tEntityA.Clear();

                        if (pEmail.BCC != null && pEmail.BCC.Count > 0)
                        {
                            foreach (var item in pEmail.BCC)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing BCC {item.EntityName} {tempId}, "; }
                            }
                            if (tEntityA.Count > 0)
                            {
                                emailEntity["bcc"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing BCC, "; }
                        }
                        tEntityA.Clear();

                        emailEntity["subject"] = pEmail.Subject;

                        emailEntity["description"] = pEmail.Description;

                        if (pEmail.LineOfBusiness != 0)
                        {
                            emailEntity["cci_lineofbusiness"] = new OptionSetValue(pEmail.LineOfBusiness);
                        }

                        if (pEmail.CreatedOn != null)
                        {
                            emailEntity["overriddencreatedon"] = pEmail.CreatedOn;
                        }

                        try
                        {
                            if (pEmail.Regarding != null)
                            {
                                if (pEmail.Regarding.HighLevelCompany != "" && pEmail.Regarding.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pEmail.Regarding.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    tempId = pEmail.Regarding.HighLevelCompany;

                                    emailEntity["regardingobjectid"] = new EntityReference(pEmail.Regarding.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                }
                                else
                                {
                                    tempId = pEmail.Regarding.RecordGuid.ToString();

                                    tempGuid = threadLocalSvc.Retrieve(pEmail.Regarding.EntityName, pEmail.Regarding.RecordGuid, new ColumnSet(new string[] { pEmail.Regarding.EntityName + "id" })).Id;
                                    emailEntity["regardingobjectid"] = new EntityReference(pEmail.Regarding.EntityName, tempGuid);
                                }
                            }
                            //else { r.InfoMissing += $"Missing Regarding: {pEmail.Regarding.EntityName} {tempId}, "; validP = false; }
                        }
                        catch (Exception) { r.InfoMissing += $"Missing Regarding: {pEmail.Regarding.EntityName} {tempId}, "; }


                        try
                        {
                            if (pEmail.OwnerId != null)
                            {
                                if (pEmail.OwnerId.HighLevelCompany != "" && pEmail.OwnerId.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pEmail.OwnerId.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    emailEntity["ownerid"] = new EntityReference(pEmail.OwnerId.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                }
                                else
                                {
                                    tempGuid = threadLocalSvc.Retrieve(pEmail.OwnerId.EntityName, pEmail.OwnerId.RecordGuid, new ColumnSet(new string[] { pEmail.OwnerId.EntityName + "id" })).Id;
                                    emailEntity["ownerid"] = new EntityReference(pEmail.OwnerId.EntityName, tempGuid);
                                }
                            }
                            else
                            {
                                emailEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                            }
                        }
                        catch (Exception)
                        {
                            emailEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                        }

                        emailEntity.Id = pEmail.ActivityID;

                        //var a = threadLocalSvc.Retrieve("email", pEmail.ActivityID, new ColumnSet(new string[] { "activityid", "description", "ownerid", "subject" }));


                        // Execute UpsertRequest and obtain UpsertResponse. 
                        UpsertRequest request = new UpsertRequest()
                        {
                            Target = emailEntity
                        };

                        UpsertResponse rps = (UpsertResponse)threadLocalSvc.Execute(request);
                        upsertR.Add(rps);



                        if (r.InfoMissing.Trim().Length > 0)
                        {
                            recordMI.Add(r);
                        }
                    }
                    catch (Exception ex)
                    {
                        processE.Add(new ProcessError
                        {
                            RecordGuid = pEmail.ActivityID,
                            ErrorMessage = ex.Message
                        });
                    }

                    return threadLocalSvc;
                },
                (threadLocalSvc) =>
                {
                    //Dispose the cloned CrmServiceClient instance
                    if (threadLocalSvc != null)
                    {
                        threadLocalSvc.Dispose();
                    }
                });

            //Return the ConcurrentBag of EntityReferences
            return (upsertR, processE, recordMI, missingRegarding);
        }

        public (ConcurrentBag<UpsertResponse>, ConcurrentBag<ProcessError>, ConcurrentBag<RecordMissingInfo>,
            ConcurrentBag<RecordMissingInfo>) UpsertAppointmentRecords(CrmServiceClient svc, List<AppointmentCRM> pAppointments, int maxDegreeOfParallelism)
        {
            var upsertR = new ConcurrentBag<UpsertResponse>();
            var processE = new ConcurrentBag<ProcessError>();
            var recordMI = new ConcurrentBag<RecordMissingInfo>();
            var missingRegarding = new ConcurrentBag<RecordMissingInfo>();

            Parallel.ForEach(pAppointments,
                new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism },
                () =>
                {
                    //Clone the CrmServiceClient for each thread
                    return svc.Clone();
                },
                (pAppointment, loopState, index, threadLocalSvc) =>
                {
                    try
                    {
                        RecordMissingInfo r = new RecordMissingInfo() { RecordGuid = pAppointment.ActivityID };

                        Entity appointmentEntity = new Entity("appointment");
                        Entity temp;
                        Guid tempGuid;
                        List<Entity> tEntityA = new List<Entity>();
                        string tempId = "";

                        if (pAppointment.Required != null && pAppointment.Required.Count > 0)
                        {
                            foreach (var item in pAppointment.Required)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing From: {item.EntityName} {tempId}, "; }
                            }

                            if (tEntityA.Count > 0)
                            {
                                appointmentEntity["requiredattendees"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing From, "; }
                        }
                        else { r.InfoMissing += "Missing Required Attendees, "; }
                        tEntityA.Clear();

                        if (pAppointment.Optional != null && pAppointment.Optional.Count > 0)
                        {
                            foreach (var item in pAppointment.Optional)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing Optional Attendees: {item.EntityName} {tempId}, "; }
                            }
                            if (tEntityA.Count > 0)
                            {
                                appointmentEntity["optionalattendees"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing To, "; }
                        }
                        tEntityA.Clear();

                        if (pAppointment.Organizer != null && pAppointment.Organizer.Count > 0)
                        {
                            foreach (var item in pAppointment.Organizer)
                            {
                                try
                                {
                                    if (item.HighLevelCompany != "" && item.EntityName == "account")
                                    {
                                        FilterExpression filter = new FilterExpression();
                                        filter.FilterOperator = LogicalOperator.And;
                                        filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, item.HighLevelCompany));
                                        QueryExpression query = new QueryExpression();
                                        query.EntityName = "account";
                                        query.ColumnSet = new ColumnSet();
                                        query.ColumnSet.Columns.Add("accountid");
                                        query.Criteria.AddFilter(filter);

                                        tempId = item.HighLevelCompany;

                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                    }
                                    else
                                    {
                                        tempId = item.RecordGuid.ToString();

                                        tempGuid = threadLocalSvc.Retrieve(item.EntityName, item.RecordGuid, new ColumnSet(new string[] { item.EntityName + "id" })).Id;
                                        temp = new Entity("activityparty");
                                        temp["partyid"] = new EntityReference(item.EntityName, tempGuid);
                                    }

                                    tEntityA.Add(temp);
                                }
                                catch (Exception) { r.InfoMissing += $"Missing Organizer {item.EntityName} {tempId}, "; }
                            }
                            if (tEntityA.Count > 0)
                            {
                                appointmentEntity["organizer"] = tEntityA.ToArray();
                            }
                            //else { r.InfoMissing += "Missing CC, "; }
                        }
                        tEntityA.Clear();

                        appointmentEntity["subject"] = pAppointment.Subject;

                        appointmentEntity["description"] = pAppointment.Description;

                        appointmentEntity["isalldayevent"] = pAppointment.AllDayEvent;

                        appointmentEntity["location"] = pAppointment.Location;

                        if (pAppointment.ScheduleEnd != null && pAppointment.ScheduleEnd.Value.Year > 1)
                        {
                            appointmentEntity["scheduledend"] = pAppointment.ScheduleEnd;
                        }

                        if (pAppointment.Duration != null)
                        {
                            appointmentEntity["scheduleddurationminutes"] = pAppointment.Duration;
                        }

                        if (pAppointment.ScheduleStart != null && pAppointment.ScheduleStart.Value.Year > 1)
                        {
                            appointmentEntity["scheduledstart"] = pAppointment.ScheduleStart;
                        }

                        if (pAppointment.LineOfBusiness != 0)
                        {
                            appointmentEntity["cci_lineofbusiness"] = new OptionSetValue(pAppointment.LineOfBusiness);
                        }

                        if (pAppointment.CreatedOn != null)
                        {
                            appointmentEntity["overriddencreatedon"] = pAppointment.CreatedOn;
                        }

                        try
                        {
                            if (pAppointment.Regarding != null)
                            {
                                if (pAppointment.Regarding.HighLevelCompany != "" && pAppointment.Regarding.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pAppointment.Regarding.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    tempId = pAppointment.Regarding.HighLevelCompany;

                                    appointmentEntity["regardingobjectid"] = new EntityReference(pAppointment.Regarding.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                }
                                else
                                {
                                    tempId = pAppointment.Regarding.RecordGuid.ToString();

                                    tempGuid = threadLocalSvc.Retrieve(pAppointment.Regarding.EntityName, pAppointment.Regarding.RecordGuid, new ColumnSet(new string[] { pAppointment.Regarding.EntityName + "id" })).Id;
                                    appointmentEntity["regardingobjectid"] = new EntityReference(pAppointment.Regarding.EntityName, tempGuid);
                                }
                            }
                            //else { r.InfoMissing += $"Missing Regarding: {pEmail.Regarding.EntityName} {tempId}, "; validP = false; }
                        }
                        catch (Exception) { r.InfoMissing += $"Missing Regarding: {pAppointment.Regarding.EntityName} {tempId}, "; }

                        try
                        {
                            if (pAppointment.OwnerId != null)
                            {
                                if (pAppointment.OwnerId.HighLevelCompany != "" && pAppointment.OwnerId.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pAppointment.OwnerId.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    appointmentEntity["ownerid"] = new EntityReference(pAppointment.OwnerId.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                }
                                else
                                {
                                    tempGuid = threadLocalSvc.Retrieve(pAppointment.OwnerId.EntityName, pAppointment.OwnerId.RecordGuid, new ColumnSet(new string[] { pAppointment.OwnerId.EntityName + "id" })).Id;
                                    appointmentEntity["ownerid"] = new EntityReference(pAppointment.OwnerId.EntityName, tempGuid);
                                }
                            }
                            else
                            {
                                appointmentEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                            }
                        }
                        catch (Exception)
                        {
                            appointmentEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                        }

                        appointmentEntity.Id = pAppointment.ActivityID;

                        // Execute UpsertRequest and obtain UpsertResponse. 
                        UpsertRequest request = new UpsertRequest()
                        {
                            Target = appointmentEntity
                        };

                        UpsertResponse rps = (UpsertResponse)threadLocalSvc.Execute(request);
                        upsertR.Add(rps);


                        if (r.InfoMissing.Trim().Length > 0)
                        {
                            recordMI.Add(r);
                        }
                    }
                    catch (Exception ex)
                    {
                        processE.Add(new ProcessError
                        {
                            RecordGuid = pAppointment.ActivityID,
                            ErrorMessage = ex.Message
                        });
                    }

                    return threadLocalSvc;
                },
                (threadLocalSvc) =>
                {
                    //Dispose the cloned CrmServiceClient instance
                    if (threadLocalSvc != null)
                    {
                        threadLocalSvc.Dispose();
                    }
                });

            //Return the ConcurrentBag of EntityReferences
            return (upsertR, processE, recordMI, missingRegarding);
        }

        public (ConcurrentBag<UpsertResponse>, ConcurrentBag<ProcessError>, ConcurrentBag<RecordMissingInfo>,
            ConcurrentBag<RecordMissingInfo>) UpsertTaskRecords(CrmServiceClient svc, List<TaskCRM> pTasks, int maxDegreeOfParallelism)
        {
            var upsertR = new ConcurrentBag<UpsertResponse>();
            var processE = new ConcurrentBag<ProcessError>();
            var recordMI = new ConcurrentBag<RecordMissingInfo>();
            var missingRegarding = new ConcurrentBag<RecordMissingInfo>();

            Parallel.ForEach(pTasks,
                new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism },
                () =>
                {
                    //Clone the CrmServiceClient for each thread
                    return svc.Clone();
                },
                (pTask, loopState, index, threadLocalSvc) =>
                {
                    try
                    {
                        RecordMissingInfo r = new RecordMissingInfo() { RecordGuid = pTask.ActivityID };

                        Entity taskEntity = new Entity("task");
                        Guid tempGuid;
                        List<Entity> tEntityA = new List<Entity>();
                        string tempId = "";

                        taskEntity["subject"] = pTask.Subject;

                        taskEntity["description"] = pTask.Description;

                        if (pTask.DueDate != null && pTask.DueDate.Year > 1)
                        {
                            taskEntity["scheduledend"] = pTask.DueDate;
                        }

                        if (pTask.Duration != null)
                        {
                            taskEntity["scheduleddurationminutes"] = pTask.Duration;
                        }

                        if (pTask.Duration != null)
                        {
                            taskEntity["actualdurationminutes"] = pTask.Duration;
                        }

                        if (pTask.LineOfBusiness != 0)
                        {
                            taskEntity["cci_lineofbusiness"] = new OptionSetValue(pTask.LineOfBusiness);
                        }

                        if (pTask.CreatedOn != null && pTask.CreatedOn.Year > 1)
                        {
                            taskEntity["overriddencreatedon"] = pTask.CreatedOn;
                        }

                        try
                        {
                            if (pTask.Regarding != null)
                            {
                                if (pTask.Regarding.HighLevelCompany != "" && pTask.Regarding.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pTask.Regarding.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    tempId = pTask.Regarding.HighLevelCompany;

                                    taskEntity["regardingobjectid"] = new EntityReference(pTask.Regarding.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                }
                                else
                                {
                                    tempId = pTask.Regarding.RecordGuid.ToString();

                                    tempGuid = threadLocalSvc.Retrieve(pTask.Regarding.EntityName, pTask.Regarding.RecordGuid, new ColumnSet(new string[] { pTask.Regarding.EntityName + "id" })).Id;
                                    taskEntity["regardingobjectid"] = new EntityReference(pTask.Regarding.EntityName, tempGuid);
                                }
                            }
                            //else { r.InfoMissing += $"Missing Regarding: {pEmail.Regarding.EntityName} {tempId}, "; validP = false; }
                        }
                        catch (Exception) { r.InfoMissing += $"Missing Regarding: {pTask.Regarding.EntityName} {tempId}, "; }

                        try
                        {
                            if (pTask.OwnerId != null)
                            {
                                if (pTask.OwnerId.HighLevelCompany != "" && pTask.OwnerId.EntityName == "account")
                                {
                                    FilterExpression filter = new FilterExpression();
                                    filter.FilterOperator = LogicalOperator.And;
                                    filter.AddCondition(new ConditionExpression("cci_highlevelcompanynumber", ConditionOperator.Equal, pTask.OwnerId.HighLevelCompany));
                                    QueryExpression query = new QueryExpression();
                                    query.EntityName = "account";
                                    query.ColumnSet = new ColumnSet();
                                    query.ColumnSet.Columns.Add("accountid");
                                    query.Criteria.AddFilter(filter);

                                    taskEntity["ownerid"] = new EntityReference(pTask.OwnerId.EntityName, threadLocalSvc.RetrieveMultiple(query).Entities[0].Id);
                                }
                                else
                                {
                                    tempGuid = threadLocalSvc.Retrieve(pTask.OwnerId.EntityName, pTask.OwnerId.RecordGuid, new ColumnSet(new string[] { pTask.OwnerId.EntityName + "id" })).Id;
                                    taskEntity["ownerid"] = new EntityReference(pTask.OwnerId.EntityName, tempGuid);
                                }
                            }
                            else
                            {
                                taskEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                            }
                        }
                        catch (Exception)
                        {
                            taskEntity["ownerid"] = new EntityReference("team", Guid.Parse("55809d01-3da7-ea11-a812-000d3a9b112e"));
                        }

                        taskEntity.Id = pTask.ActivityID;

                        // Execute UpsertRequest and obtain UpsertResponse. 
                        UpsertRequest request = new UpsertRequest()
                        {
                            Target = taskEntity
                        };

                        UpsertResponse rps = (UpsertResponse)threadLocalSvc.Execute(request);
                        upsertR.Add(rps);

                        if (r.InfoMissing.Trim().Length > 0)
                        {
                            recordMI.Add(r);
                        }
                    }
                    catch (Exception ex)
                    {
                        processE.Add(new ProcessError
                        {
                            RecordGuid = pTask.ActivityID,
                            ErrorMessage = ex.Message
                        });
                    }

                    return threadLocalSvc;
                },
                (threadLocalSvc) =>
                {
                    //Dispose the cloned CrmServiceClient instance
                    if (threadLocalSvc != null)
                    {
                        threadLocalSvc.Dispose();
                    }
                });

            //Return the ConcurrentBag of EntityReferences
            return (upsertR, processE, recordMI, missingRegarding);
        }

        public string GetUserEmail_IS(Guid pGuid)
        {
            StringBuilder sbFetchUser = new StringBuilder();
            sbFetchUser.Append("<fetch><entity name='systemuser' ><attribute name='internalemailaddress' />");
            sbFetchUser.Append("<filter type='and' ><condition attribute='systemuserid' operator='eq' value='" + pGuid + "' />");
            sbFetchUser.Append("</filter></entity></fetch>");
            EntityCollection User = serviceProxy.RetrieveMultiple(new FetchExpression(sbFetchUser.ToString()));
            if (User.Entities.Count > 0)
                return User.Entities[0].Attributes["internalemailaddress"].ToString();
            else
                return "";
        }

        public List<Entity> GetAllActivePhoneCall(string pFilterGuid, DateTime pFromDT)
        {
            List<Entity> entities = new List<Entity>();

            int page = 1;
            EntityCollection entityList = new EntityCollection();

            if (pFilterGuid != null && pFilterGuid.Length > 0)
            {
                StringBuilder sbFetchAccount = new StringBuilder();
                sbFetchAccount.Append("<fetch>");
                sbFetchAccount.Append("<entity name='phonecall' >");
                sbFetchAccount.Append("<attribute name = 'ownerid' /><attribute name = 'subject' /><attribute name = 'regardingobjectid' /><attribute name = 'createdon' />");
                sbFetchAccount.Append("<attribute name = 'activityid' /><attribute name = 'from' /><attribute name = 'to' /><attribute name = 'phonenumber' />");
                sbFetchAccount.Append("<attribute name = 'actualdurationminutes' /><attribute name = 'directioncode' /><attribute name = 'description' /> <attribute name = 'ownerid' />");
                sbFetchAccount.Append("<filter type='and' ><condition attribute='activityid' operator='eq' value='{0}'/> ");
                sbFetchAccount.Append("<condition attribute='regardingobjecttypecode' operator='in'><value>1</value><value>2</value><value>3</value><value>4</value> </condition>");
                sbFetchAccount.Append("</filter></entity></fetch>");
                entityList = serviceProxy.RetrieveMultiple(new FetchExpression(string.Format(sbFetchAccount.ToString(), pFilterGuid)));

                foreach (var item in entityList.Entities)
                {
                    try
                    {
                        entities.Add(item);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            else
            {
                string datefilter = "";
                if (pFromDT.Year != 1)
                {
                    datefilter = $"<condition attribute = 'modifiedon' operator= 'on-or-after' value = '{pFromDT.Date}' />";
                }
                else
                {
                    datefilter = "<condition attribute = 'modifiedon' operator= 'on-or-after' value = '1/1/2019 00:00:00' />";
                }

                do
                {
                    entityList = serviceProxy.RetrieveMultiple(new FetchExpression(String.Format(@"<fetch version='1.0' page='{1}' paging-cookie='{0}' count='5000' 
                        output-format='xml-platform' mapping='logical' distinct='false'>
                        <entity name = 'phonecall' >
                        <attribute name = 'ownerid' /><attribute name = 'subject' /><attribute name = 'regardingobjectid' /><attribute name = 'createdon' />
                        <attribute name = 'activityid' /><attribute name = 'from' /><attribute name = 'to' /><attribute name = 'phonenumber' />
                        <attribute name = 'actualdurationminutes' /><attribute name = 'directioncode' /><attribute name = 'description' />
                        <filter type='and' >
                        {2}
                        <condition attribute='regardingobjecttypecode' operator='in'><value>1</value><value>2</value><value>3</value><value>4</value></condition>
                        </filter></entity></fetch>", SecurityElement.Escape(entityList.PagingCookie), page++, datefilter)));


                    foreach (var item in entityList.Entities)
                    {
                        try
                        {
                            entities.Add(item);
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }
                }
                while (entityList.MoreRecords);
            }

            return entities;
         }

        public List<Entity> GetAllActiveEmail(string pFilterGuid, DateTime pFromDT)
        {
            List<Entity> entities = new List<Entity>();

            int page = 1;
            EntityCollection entityList = new EntityCollection();

            if (pFilterGuid != null && pFilterGuid.Length > 0)
            {
                StringBuilder sbFetchAccount = new StringBuilder();
                sbFetchAccount.Append("<fetch>");
                sbFetchAccount.Append("<entity name='email' >");
                sbFetchAccount.Append("<attribute name = 'ownerid' /><attribute name = 'subject' /><attribute name = 'regardingobjectid' /><attribute name = 'createdon' />");
                sbFetchAccount.Append("<attribute name = 'activityid' /><attribute name = 'from' /><attribute name = 'to' /><attribute name = 'cc' />");
                sbFetchAccount.Append("<attribute name = 'bcc' /><attribute name = 'description' /> <attribute name = 'ownerid' />");
                sbFetchAccount.Append("<filter type='and' ><condition attribute='activityid' operator='eq' value='{0}'/>");
                sbFetchAccount.Append("<condition attribute='regardingobjecttypecode' operator='in'><value>1</value><value>2</value><value>3</value><value>4</value></condition>");
                sbFetchAccount.Append("</filter></entity></fetch>");

                entityList = serviceProxy.RetrieveMultiple(new FetchExpression(string.Format(sbFetchAccount.ToString(), pFilterGuid)));

                foreach (var item in entityList.Entities)
                {
                    try
                    {
                        entities.Add(item);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            else
            {
                string datefilter = "";
                if (pFromDT.Year != 1)
                {
                    datefilter = $"<condition attribute = 'modifiedon' operator= 'on-or-after' value = '{pFromDT.Date}' />";
                }
                else
                {
                    datefilter = "<condition attribute = 'modifiedon' operator= 'on-or-after' value = '1/1/2019 00:00:00' />";
                }

                do
                {
                    entityList = serviceProxy.RetrieveMultiple(new FetchExpression(String.Format(@"<fetch version='1.0' page='{1}' paging-cookie='{0}' count='5000' 
                        output-format='xml-platform' mapping='logical' distinct='false'>
                        <entity name = 'email' >
                        <attribute name = 'ownerid' /><attribute name = 'subject' /><attribute name = 'regardingobjectid' /> <attribute name = 'createdon' />
                        <attribute name = 'activityid' /><attribute name = 'from' /><attribute name = 'to' />
                        <attribute name = 'cc' /><attribute name = 'bcc' /><attribute name = 'description' />
                        <filter type='and' >
                        {2}
                        <condition attribute='regardingobjecttypecode' operator='in'><value>1</value><value>2</value><value>3</value><value>4</value></condition>
                        </filter></entity></fetch>", SecurityElement.Escape(entityList.PagingCookie), page++, datefilter)));

                    foreach (var item in entityList.Entities)
                    {
                        try
                        {
                            entities.Add(item);
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }
                }
                while (entityList.MoreRecords);
            }

            return entities;
        }

        public List<Entity> GetAppointmentIS(string pFilterGuid, DateTime pFromDT)
        {
            List<Entity> entities = new List<Entity>();

            int page = 1;
            EntityCollection entityList = new EntityCollection();

            if (pFilterGuid != null && pFilterGuid.Length > 0)
            {
                string query = @"<fetch>
                                    <entity name = 'appointment'> 
                                        <attribute name = 'activityid' />
                                        <attribute name = 'subject' />  
                                        <attribute name = 'description' />   
                                        <attribute name = 'organizer' />    
                                        <attribute name = 'requiredattendees' />     
                                        <attribute name = 'optionalattendees' />      
                                        <attribute name = 'regardingobjectid' />       
                                        <attribute name = 'ownerid' />        
                                        <attribute name = 'scheduleddurationminutes' />         
                                        <attribute name = 'scheduledstart' />          
                                        <attribute name = 'scheduledend' />           
                                        <attribute name = 'location' />           
                                        <attribute name = 'isalldayevent' />             
                                        <filter>             
                                            <condition attribute = 'regardingobjecttypecode' operator= 'in'>
                                                <value> 1 </value>                
                                                <value> 2 </value>                
                                                <value> 3 </value>                
                                                <value> 4 </value>                
                                            </condition>                                   
                                            <condition attribute = 'activityid' operator= 'eq' value = '{0}'/>                        
                                        </filter>
                                    </entity>
                                </fetch> ";

                entityList = serviceProxy.RetrieveMultiple(new FetchExpression(string.Format(query.ToString(), pFilterGuid)));

                foreach (var item in entityList.Entities)
                {
                    try
                    {
                        entities.Add(item);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            else
            {
                string datefilter;
                if (pFromDT.Year != 1)
                {
                    datefilter = $"<condition attribute = 'modifiedon' operator= 'on-or-after' value = '{pFromDT.Date}' />";
                }
                else
                {
                    datefilter = $"<condition attribute = 'modifiedon' operator= 'on-or-after' value = '2019-01-01' />";
                }

                do
                {
                    entityList = serviceProxy.RetrieveMultiple(new FetchExpression(
                        String.Format(@"<fetch version = '1.0' page = '{1}' paging-cookie = '{0}' count = '5000'
                                        output-format = 'xml-platform' mapping = 'logical' distinct = 'false' >
                                            <entity name = 'appointment'> 
                                                <attribute name = 'activityid' />
                                                <attribute name = 'subject' />  
                                                <attribute name = 'description' />   
                                                <attribute name = 'organizer' />    
                                                <attribute name = 'requiredattendees' />     
                                                <attribute name = 'optionalattendees' />      
                                                <attribute name = 'regardingobjectid' />       
                                                <attribute name = 'ownerid' />        
                                                <attribute name = 'scheduleddurationminutes' />         
                                                <attribute name = 'scheduledstart' />          
                                                <attribute name = 'scheduledend' />           
                                                <attribute name = 'location' />           
                                                <attribute name = 'isalldayevent' />             
                                                <filter>             
                                                    <condition attribute = 'regardingobjecttypecode' operator= 'in'>
                                                        <value> 1 </value>                
                                                        <value> 2 </value>                
                                                        <value> 3 </value>                
                                                        <value> 4 </value>                
                                                    </condition>                
                                                    {2}                        
                                                </filter>
                                            </entity>
                                        </fetch> ",
                                SecurityElement.Escape(entityList.PagingCookie), page++, datefilter)));

                    foreach (var item in entityList.Entities)
                    {
                        try
                        {
                            entities.Add(item);
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }
                }
                while (entityList.MoreRecords);
            }

            return entities;
        }

        public List<Entity> GetTaskIS(string pFilterGuid, DateTime pFromDT)
        {
            List<Entity> entities = new List<Entity>();

            int page = 1;
            EntityCollection entityList = new EntityCollection();

            if (pFilterGuid != null && pFilterGuid.Length > 0)
            {
                string query = @"<fetch>
                                      <entity name='task'>
                                        <attribute name='activityid' />
                                        <attribute name='subject' />
                                        <attribute name='description' />
                                        <attribute name='regardingobjectid' />
                                        <attribute name='ownerid' />
                                        <attribute name='scheduledend' />
                                        <attribute name='actualdurationminutes' />
                                        <filter>
                                          <condition attribute='regardingobjecttypecode' operator='in'>
                                            <value>1</value>
                                            <value>2</value>
                                            <value>3</value>
                                            <value>4</value>
                                          </condition>
                                          <condition attribute='activityid' operator='eq' value={0}' />
                                        </filter>
                                      </entity>
                                    </fetch>";

                entityList = serviceProxy.RetrieveMultiple(new FetchExpression(string.Format(query.ToString(), pFilterGuid)));

                foreach (var item in entityList.Entities)
                {
                    try
                    {
                        entities.Add(item);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
            else
            {
                string datefilter = "";
                if (pFromDT.Year != 1)
                {
                    datefilter = $"<condition attribute = 'modifiedon' operator= 'on-or-after' value = '{pFromDT.Date}' />";
                }
                else
                {
                    datefilter = $"<condition attribute = 'modifiedon' operator= 'on-or-after' value = '2019-01-01' />";
                }

                do
                {
                    entityList = serviceProxy.RetrieveMultiple(new FetchExpression(
                        String.Format(@"<fetch version = '1.0' page = '{1}' paging-cookie = '{0}' count = '5000'
                                        output-format = 'xml-platform' mapping = 'logical' distinct = 'false' >
                                            <entity name='task'>
                                                <attribute name='activityid' />
                                                <attribute name='subject' />
                                                <attribute name='description' />
                                                <attribute name='regardingobjectid' />
                                                <attribute name='ownerid' />
                                                <attribute name='scheduledend' />
                                                <attribute name='actualdurationminutes' />            
                                                <filter>             
                                                    <condition attribute = 'regardingobjecttypecode' operator= 'in'>
                                                        <value> 1 </value>                
                                                        <value> 2 </value>                
                                                        <value> 3 </value>                
                                                        <value> 4 </value>                
                                                    </condition>                
                                                    {2}                        
                                                </filter>
                                            </entity>
                                        </fetch> ",
                                SecurityElement.Escape(entityList.PagingCookie), page++, datefilter)));

                    foreach (var item in entityList.Entities)
                    {
                        try
                        {
                            entities.Add(item);
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }
                }
                while (entityList.MoreRecords);
            }

            return entities;
        }

        #endregion
    }

    public class CRMResponseMsg
    {
        private string responsemessage = "";
        private string responseCode = "";
        private int responseCodeNumber = 0;
        private string xEntityID = "";
        private string xEntityGuid = "";

        public string EntityID
        {
            get { return xEntityID; }
            set { xEntityID = value; }
        }

        public string EntityGuid
        {
            get { return xEntityGuid; }
            set { xEntityGuid = value; }
        }

        public string ResponseMessage
        {
            get { return responsemessage; }
            set { responsemessage = value; }
        }

        public int ResponseCodeNumber
        {
            get { return responseCodeNumber; }
            set { responseCodeNumber = value; }
        }

        public string ResponseCode
        {
            get { return responseCode; }
            set { responseCode = value; }
        }
    }

    public class PhoneCallCRM
    {
        private Guid xActivityID;
        private string xSubject = "";
        private List<EntityGuid> xFrom;
        private List<EntityGuid> xTo;
        private string xPhoneNumber = "";
        private bool xDirection = false;
        private int xLineOfBusiness = 0;
        private string xDescription = "";
        private EntityGuid xRegarding;
        private EntityGuid xOwnerId;
        private int? xDuration = 0;
        private string xUpsertStatus = "";
        private string xUpsertComment = "";
        private DateTime xCreatedOn;

        public Guid ActivityID
        {
            get { return xActivityID; }
            set { xActivityID = value; }
        }

        public DateTime CreatedOn
        {
            get { return xCreatedOn; }
            set { xCreatedOn = value; }
        }

        public string Subject
        {
            get { return xSubject; }
            set { xSubject = value; }
        }

        public List<EntityGuid> From
        {
            get { return xFrom; }
            set { xFrom = value; }
        }

        public List<EntityGuid> To
        {
            get { return xTo; }
            set { xTo = value; }
        }

        public string PhoneNumber
        {
            get { return xPhoneNumber; }
            set { xPhoneNumber = value; }
        }

        public bool Direction
        {
            get { return xDirection; }
            set { xDirection = value; }
        }

        public int LineOfBusiness
        {
            get { return xLineOfBusiness; }
            set { xLineOfBusiness = value; }
        }

        public string Description
        {
            get { return xDescription; }
            set { xDescription = value; }
        }

        public EntityGuid Regarding
        {
            get { return xRegarding; }
            set { xRegarding = value; }
        }

        public int? Duration
        {
            get { return xDuration; }
            set { xDuration = value; }
        }

        public EntityGuid OwnerId
        {
            get { return xOwnerId; }
            set { xOwnerId = value; }
        }

        public string UpsertStatus
        {
            get { return xUpsertStatus; }
            set { xUpsertStatus = value; }
        }

        public string UpsertComment
        {
            get { return xUpsertComment; }
            set { xUpsertComment = value; }
        }
    }

    public class EmailCRM
    {
        private Guid xActivityID;
        private string xSubject = "";
        private List<EntityGuid> xFrom;
        private List<EntityGuid> xTo;
        private List<EntityGuid> xCC;
        private List<EntityGuid> xBCC;
        private int xLineOfBusiness = 0;
        private string xDescription = "";
        private EntityGuid xRegarding;
        private EntityGuid xOwnerId;
        private DateTime xCreatedOn;


        public DateTime CreatedOn
        {
            get { return xCreatedOn; }
            set { xCreatedOn = value; }
        }
        public Guid ActivityID
        {
            get { return xActivityID; }
            set { xActivityID = value; }
        }

        public string Subject
        {
            get { return xSubject; }
            set { xSubject = value; }
        }

        public List<EntityGuid> From
        {
            get { return xFrom; }
            set { xFrom = value; }
        }

        public List<EntityGuid> To
        {
            get { return xTo; }
            set { xTo = value; }
        }

        public List<EntityGuid> CC
        {
            get { return xCC; }
            set { xCC = value; }
        }

        public List<EntityGuid> BCC
        {
            get { return xBCC; }
            set { xBCC = value; }
        }
      
        public int LineOfBusiness
        {
            get { return xLineOfBusiness; }
            set { xLineOfBusiness = value; }
        }

        public string Description
        {
            get { return xDescription; }
            set { xDescription = value; }
        }

        public EntityGuid Regarding
        {
            get { return xRegarding; }
            set { xRegarding = value; }
        }

        public EntityGuid OwnerId
        {
            get { return xOwnerId; }
            set { xOwnerId = value; }
        }
    }

    public class TaskCRM
    {
        private Guid xActivityID;
        private string xSubject = "";
        private string xDescription = "";
        private int? xDuration = 0;
        private EntityGuid xRegarding;
        private EntityGuid xOwnerId;
        private DateTime xCreatedOn;
        private int xLineOfBusiness = 0;
        private DateTime xDueDate;

        public int? Duration
        {
            get { return xDuration; }
            set { xDuration = value; }
        }
        public DateTime CreatedOn
        {
            get { return xCreatedOn; }
            set { xCreatedOn = value; }
        }
        public Guid ActivityID
        {
            get { return xActivityID; }
            set { xActivityID = value; }
        }

        public string Subject
        {
            get { return xSubject; }
            set { xSubject = value; }
        }

        public DateTime DueDate
        {
            get { return xDueDate; }
            set { xDueDate = value; }
        }

        public int LineOfBusiness
        {
            get { return xLineOfBusiness; }
            set { xLineOfBusiness = value; }
        }

        public string Description
        {
            get { return xDescription; }
            set { xDescription = value; }
        }

        public EntityGuid Regarding
        {
            get { return xRegarding; }
            set { xRegarding = value; }
        }

        public EntityGuid OwnerId
        {
            get { return xOwnerId; }
            set { xOwnerId = value; }
        }
    }

    public class AppointmentCRM
    {
        private Guid xActivityID;        
        private List<EntityGuid> xRequired;
        private List<EntityGuid> xOptional;
        private List<EntityGuid> xOrganizer;
        private string xSubject = "";
        private string xLocation = "";
        private int xLineOfBusiness = 0;
        private string xDescription = "";
        private EntityGuid xRegarding;
        private EntityGuid xOwnerId;
        private int? xDuration = 0;
        private DateTime? xScheduleStart;
        private DateTime? xScheduleEnd;
        private DateTime xCreatedOn;
        private bool? xAllDayEvent;

        public Guid ActivityID
        {
            get { return xActivityID; }
            set { xActivityID = value; }
        }

        public DateTime CreatedOn
        {
            get { return xCreatedOn; }
            set { xCreatedOn = value; }
        }

        public string Subject
        {
            get { return xSubject; }
            set { xSubject = value; }
        }

        public List<EntityGuid> Required
        {
            get { return xRequired; }
            set { xRequired = value; }
        }

        public List<EntityGuid> Optional
        {
            get { return xOptional; }
            set { xOptional = value; }
        }

        public List<EntityGuid> Organizer
        {
            get { return xOrganizer; }
            set { xOrganizer = value; }
        }

        public string Location
        {
            get { return xLocation; }
            set { xLocation = value; }
        }

        public int LineOfBusiness
        {
            get { return xLineOfBusiness; }
            set { xLineOfBusiness = value; }
        }

        public string Description
        {
            get { return xDescription; }
            set { xDescription = value; }
        }

        public EntityGuid Regarding
        {
            get { return xRegarding; }
            set { xRegarding = value; }
        }

        public int? Duration
        {
            get { return xDuration; }
            set { xDuration = value; }
        }

        public EntityGuid OwnerId
        {
            get { return xOwnerId; }
            set { xOwnerId = value; }
        }

        public DateTime? ScheduleStart
        {
            get { return xScheduleStart; }
            set { xScheduleStart = value; }
        }

        public DateTime? ScheduleEnd
        {
            get { return xScheduleEnd; }
            set { xScheduleEnd = value; }
        }

        public bool? AllDayEvent
        {
            get { return xAllDayEvent; }
            set { xAllDayEvent = value; }
        }
    }

    public class EntityGuid
    {
        private string xEntityName = "";
        private string xHighLevelCompany = "";
        private Guid xRecordGuid;

        public string EntityName
        {
            get { return xEntityName; }
            set { xEntityName = value; }
        }

        public string HighLevelCompany
        {
            get { return xHighLevelCompany; }
            set { xHighLevelCompany = value; }
        }

        public Guid RecordGuid
        {
            get { return xRecordGuid; }
            set { xRecordGuid = value; }
        }
    }

    public class ProcessError
    {
        private string xErrorMessage = "";
        private Guid xRecordGuid;

        public string ErrorMessage
        {
            get { return xErrorMessage; }
            set { xErrorMessage = value; }
        }

        public Guid RecordGuid
        {
            get { return xRecordGuid; }
            set { xRecordGuid = value; }
        }
    }



}
