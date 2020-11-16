using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using MigrateEmailAndPhoneCallWPF.CRM;
using MigrateEmailAndPhoneCallWPF.Properties;
using MigrateEmailAndPhoneCallWPF.Helper;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace MigrateEmailAndPhoneCallWPF.UserControls
{
    /// <summary>
    /// Interaction logic for PhoneCallUC.xaml
    /// </summary>
    public partial class EmailUC : UserControl
    {
        List<EmailCRM> emails;
        private string xEnv = "";
        private string xSource = "";
        CancellationTokenSource xCancelTokenSource;
        private string filterGuid = "";
        private DateTime processDT;
        private DateTime xFromDt;

        List<UserRecord> userList;
        List<PublishXRef> pxref;
        readonly HelperClass hc;

        public EmailUC()
        {
            InitializeComponent();
            hc = new HelperClass();
            emails = new List<EmailCRM>();
        }

        private async void BtnLoadEmail_Click(object sender, RoutedEventArgs e)
        {
            await this.Dispatcher.Invoke(async () =>
             {
                 try
                 {
                     biAzure.IsBusy = true;
                     dgEmails.ItemsSource = null;
                     emails.Clear();
                     tbComment.Text = "";

                     if (FromDT.SelectedDate.HasValue)
                     {
                         xFromDt = FromDT.SelectedDate.Value.Date;
                     }
                     
                     filterGuid = tbID.Text;

                     processDT = DateTime.Now;
                     tbComment.Text += $"Loading Source Started at {processDT}";

                     userList = hc.GetConsolidatedUsers(xEnv, "Migration");
                     pxref = hc.GetPublishXRef(xEnv, xSource);

                     if (xSource == "InsideSales")
                         dgEmails.ItemsSource = await ProcessEmailIS();
                     else
                         dgEmails.ItemsSource = await ProcessEmail();

                     tbComment.Text += $"{Environment.NewLine}Loading Source Finished at {DateTime.Now}";
                     tbTotalRecords.Text = (dgEmails.ItemsSource as List<EmailCRM>).Count.ToString();
                     biAzure.IsBusy = false;
                 }
                 catch (Exception ex)
                 {
                     string msg = ex.Message;

                     if (ex.InnerException != null)
                     {
                         msg += " " + ex.InnerException.Message;
                     }

                     MessageBox.Show(msg, "Error", MessageBoxButton.OK, MessageBoxImage.Error);

                     ResetScreen();
                 }
             });
        }

        private async Task<List<EmailCRM>> ProcessEmail()
        {
            xCancelTokenSource = new CancellationTokenSource();
            await Task.Run(() =>
            {
                List<EmailRecord> pcList;
                List<ActivityPartyRecord> apartyList;
                switch (xSource)
                {
                    case "Fiber":
                        {
                            pcList = GetFiberEmail();
                            apartyList = GetFiberActivityParty();
                            break;
                        }
                    case "Wireless":
                        {
                            pcList = GetWirelessEmail();
                            apartyList = GetWirelessActivityParty();
                            break;
                        }                    
                    default:
                        {
                            pcList = new List<EmailRecord>();
                            apartyList = new List<ActivityPartyRecord>();
                            break;
                        }                        
                }                   

                var crEmail = new ConcurrentBag<EmailCRM>();
                var recordMissingRegarding = new ConcurrentBag<RecordMissingInfo>();

                Parallel.ForEach(pcList, new ParallelOptions() { MaxDegreeOfParallelism = 500 },
                    (item, loopState, index) =>
                    {
                        EmailCRM temp;
                        EntityGuid eg;
                        string userOSCGuid;
                        PublishXRef pXref;

                        temp = new EmailCRM()
                        {
                            Subject = item.Subject,
                            Description = item.Description,
                            CreatedOn = item.CreatedOn.Value,
                            LineOfBusiness = (xSource == "Fiber" ? 803230000 : 803230002),
                            ActivityID = item.ActivityId
                        };

                        temp.From = new List<EntityGuid>();
                        temp.To = new List<EntityGuid>();
                        temp.CC = new List<EntityGuid>();
                        temp.BCC = new List<EntityGuid>();

                        List<ActivityPartyRecord> apList = (from a in apartyList
                                                            where a.ActivityId == temp.ActivityID
                                                            select a).ToList();
                        foreach (var item2 in apList)
                        {
                            eg = new EntityGuid()
                            {
                                RecordGuid = (Guid)item2.PartyId,
                                EntityName = item2.LogicalName == "cc_application" ? "opportunity" : item2.LogicalName
                            };

                            if (item2.LogicalName == "systemuser")
                            {
                                if (xSource == "Fiber")
                                {
                                    userOSCGuid = (from a in userList
                                                   where a.Fiber_crm_guid.ToUpper() == item2.PartyId.ToString().ToUpper()
                                                   orderby a.Cci_aim_prsn_seq_num descending
                                                   select a.Msdyn_systemuser_guid).FirstOrDefault();
                                }
                                else
                                {
                                    userOSCGuid = (from a in userList
                                                   where a.Cci_op_active_directory_upn.ToUpper() == item2.UserEmail.ToString().ToUpper()
                                                   orderby a.Cci_aim_prsn_seq_num descending
                                                   select a.Msdyn_systemuser_guid).FirstOrDefault();
                                }

                                if (userOSCGuid != null && userOSCGuid.Length > 0)
                                {
                                    eg.RecordGuid = new Guid(userOSCGuid);
                                }
                                //else
                                //{
                                //    continue;
                                //}
                            }
                            else
                            {
                                pXref = (from a in pxref
                                         where a.Src_sys_id.ToUpper() == item2.PartyId.ToString().ToUpper() && a.EntityType.ToLower() == item2.LogicalName
                                         select a).FirstOrDefault();

                                if (pXref != null)
                                {
                                    if (item2.LogicalName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                                    {
                                        eg.HighLevelCompany = pXref.Survivor_id;
                                    }
                                    else
                                    {
                                        eg.RecordGuid = new Guid(pXref.Survivor_id);
                                    }
                                }
                            }

                            switch (item2.ParticipationTypeMask)
                            {
                                case 1:
                                    {
                                        temp.From.Add(eg);
                                        break;
                                    }
                                case 2:
                                    {
                                        temp.To.Add(eg);
                                        break;
                                    }
                                case 3:
                                    {
                                        temp.CC.Add(eg);
                                        break;
                                    }
                                case 4:
                                    {
                                        temp.BCC.Add(eg);
                                        break;
                                    }
                                case 8:
                                    {
                                        temp.Regarding = eg;
                                        break;
                                    }
                                case 9:
                                    {
                                        temp.OwnerId = eg;
                                        break;
                                    }
                                default:
                                    break;
                            }
                        }

                        //if (temp.Regarding != null)
                        //{
                            crEmail.Add(temp);
                        //}
                        //else
                        //{
                        //    recordMissingRegarding.Add(new RecordMissingInfo { RecordGuid = temp.ActivityID, InfoMissing = "Missing Regarding" });
                        //}
                    });

                #region MyRegion
                //foreach (var item in pcList)
                //{
                //    temp = new EmailCRM()
                //    {
                //        Subject = item.Subject,
                //        Description = item.Description,
                //        CreatedOn = item.CreatedOn.Value,
                //        LineOfBusiness = (xSource == "Fiber" ? 803230000 : 803230002),
                //        ActivityID = item.ActivityId
                //    };

                //    temp.From = new List<EntityGuid>();
                //    temp.To = new List<EntityGuid>();
                //    temp.CC = new List<EntityGuid>();
                //    temp.BCC = new List<EntityGuid>();

                //    List<ActivityPartyRecord> apList = (from a in apartyList
                //                                        where a.ActivityId == temp.ActivityID
                //                                        select a).ToList();
                //    foreach (var item2 in apList)
                //    {
                //        eg = new EntityGuid()
                //        {
                //            RecordGuid = (Guid)item2.PartyId,
                //            EntityName = item2.LogicalName
                //        };

                //        if (item2.LogicalName == "systemuser")
                //        {
                //            if (xSource == "Fiber")
                //            {
                //                userOSCGuid = (from a in userList
                //                               where a.Fiber_crm_guid.ToUpper() == item2.PartyId.ToString().ToUpper()
                //                               orderby a.Cci_aim_prsn_seq_num descending
                //                               select a.Msdyn_systemuser_guid).FirstOrDefault();
                //            }
                //            else
                //            {
                //                userOSCGuid = (from a in userList
                //                               where a.Cci_op_active_directory_upn.ToUpper() == item2.UserEmail.ToString().ToUpper()
                //                               orderby a.Cci_aim_prsn_seq_num descending
                //                               select a.Msdyn_systemuser_guid).FirstOrDefault();
                //            }

                //            if (userOSCGuid != null && userOSCGuid.Length > 0)
                //            {
                //                eg.RecordGuid = new Guid(userOSCGuid);
                //            }
                //            else
                //            {
                //                continue;
                //            }
                //        }
                //        else
                //        {
                //            pXref = (from a in pxref
                //                     where a.Src_sys_id.ToUpper() == item2.PartyId.ToString().ToUpper() && a.EntityType.ToLower() == item2.LogicalName
                //                     select a).FirstOrDefault();

                //            if (pXref != null)
                //            {
                //                if (item2.LogicalName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                //                {
                //                    eg.HighLevelCompany = pXref.Survivor_id;
                //                }
                //                else
                //                {
                //                    eg.RecordGuid = new Guid(pXref.Survivor_id);
                //                }
                //            }
                //        }                       

                //        switch (item2.ParticipationTypeMask)
                //        {
                //            case 1:
                //                {
                //                    temp.From.Add(eg);
                //                    break;
                //                }
                //            case 2:
                //                {
                //                    temp.To.Add(eg);
                //                    break;
                //                }
                //            case 3:
                //                {
                //                    temp.CC.Add(eg);
                //                    break;
                //                }
                //            case 4:
                //                {
                //                    temp.BCC.Add(eg);
                //                    break;
                //                }
                //            case 8:
                //                {
                //                    temp.Regarding = eg;
                //                    break;
                //                }
                //            case 9:
                //                {
                //                    temp.OwnerId = eg;
                //                    break;
                //                }
                //            default:
                //                break;
                //        }
                //    }

                //    if (temp.Regarding != null)
                //    {
                //        emails.Add(temp);
                //    }
                //    else 
                //    {
                //        rmi.Add(new RecordMissingInfo { RecordGuid = temp.ActivityID, InfoMissing = "Missing Regarding" }); 
                //    }                    
                //}
                #endregion

                emails = crEmail.ToList();

                //if (recordMissingRegarding.Count > 0)
                //{
                //    string path = $"C:\\Users\\jxh0tw1\\Documents\\Migration\\activity\\Email{xSource}{DateTime.Now.Year}{DateTime.Now.Month}{DateTime.Now.Day}_{DateTime.Now.Hour}_{DateTime.Now.Minute}.csv";

                //    using (var w = new StreamWriter(path))
                //    {
                //        foreach (var item in recordMissingRegarding)
                //        {
                //            var line = string.Format("{0},{1}", item.RecordGuid.ToString(), item.InfoMissing);
                //            w.WriteLine(line);
                //            w.Flush();
                //        }
                //    }
                //}

            }, xCancelTokenSource.Token);
            return emails;
        }

        private async Task<List<EmailCRM>> ProcessEmailIS()
        {
            xCancelTokenSource = new CancellationTokenSource();
            await Task.Run(() =>
            {
                List<RecordMissingInfo> rmi = new List<RecordMissingInfo>();

                CRMSync tcrm = new CRMSync();
                tcrm.StartConnection(xEnv.ToLower(), false);
                List<Entity> eIS = tcrm.GetAllActiveEmail(filterGuid, xFromDt);
                tcrm.EndConnection();
               
                EmailCRM temp;
                EntityGuid eg;
                Entity pcentity;
                EntityReference tempER;
                string userOSCGuid;
                PublishXRef pXref;

                foreach (var item in eIS)
                {
                    temp = new EmailCRM()
                    {
                        Subject = item.Attributes.Contains("subject") ? item.Attributes["subject"].ToString() : "",
                        Description = item.Attributes.Contains("description") ? item.Attributes["description"].ToString() : "",
                        LineOfBusiness = (xSource == "Fiber" ? 803230000 : 803230002),
                        ActivityID = item.Id
                    };

                    if (item.Attributes.Contains("createdon"))
                    {
                        temp.CreatedOn = DateTime.Parse(item.Attributes["createdon"].ToString());
                    }

                    pcentity = (from a in eIS
                                where a.Id == temp.ActivityID
                                select a).FirstOrDefault();

                    temp.From = new List<EntityGuid>();
                    foreach (var item2 in (pcentity.Attributes["from"] as EntityCollection).Entities)
                    {
                        if (!item2.Attributes.Contains("partyid"))
                        {
                            continue;
                        }
                        tempER = (EntityReference)item2.Attributes["partyid"];

                        eg = new EntityGuid()
                        {
                            RecordGuid = tempER.Id,
                            EntityName = tempER.LogicalName
                        };

                        if (eg.EntityName == "systemuser")
                        {
                            string[] name = tempER.Name.Split(' ');
                            userOSCGuid = (from a in userList
                                           where a.Cci_op_active_directory_upn.Contains(name[0].ToLower().Trim()) && a.Cci_op_active_directory_upn.Contains(name[1].ToLower().Trim())
                                           orderby a.Cci_aim_prsn_seq_num descending
                                           select a.Msdyn_systemuser_guid).FirstOrDefault();
                            if (userOSCGuid != null && userOSCGuid.Length > 0)
                            {
                                eg.RecordGuid = new Guid(userOSCGuid);
                            }
                        }
                        else
                        {
                            pXref = (from a in pxref
                                     where a.Src_sys_id.ToUpper() == eg.RecordGuid.ToString().ToUpper() && a.EntityType.ToLower() == eg.EntityName
                                     select a).FirstOrDefault();

                            if (pXref != null)
                            {
                                if (item2.LogicalName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                                {
                                    eg.HighLevelCompany = pXref.Survivor_id;
                                }
                                else
                                {
                                    eg.RecordGuid = new Guid(pXref.Survivor_id);
                                }
                            }
                        }

                        temp.From.Add(eg);
                    }
                    temp.To = new List<EntityGuid>();
                    foreach (var item2 in (pcentity.Attributes["to"] as EntityCollection).Entities)
                    {
                        if (!item2.Attributes.Contains("partyid"))
                        {
                            continue;
                        }
                        tempER = (EntityReference)item2.Attributes["partyid"];

                        eg = new EntityGuid()
                        {
                            RecordGuid = tempER.Id,
                            EntityName = tempER.LogicalName
                        };

                        if (eg.EntityName == "systemuser")
                        {
                            //userOSCGuid = GetConsolidatedUsersGuid(tempER.Name);

                            string[] name = tempER.Name.Split(' ');
                            userOSCGuid = (from a in userList
                                           where a.Cci_op_active_directory_upn.Contains(name[0].ToLower().Trim()) && a.Cci_op_active_directory_upn.Contains(name[1].ToLower().Trim())
                                           orderby a.Cci_aim_prsn_seq_num descending
                                           select a.Msdyn_systemuser_guid).FirstOrDefault();
                            if (userOSCGuid != null && userOSCGuid.Length > 0)
                            {
                                eg.RecordGuid = new Guid(userOSCGuid);
                            }
                            //else
                            //{
                            //    continue;
                            //}
                        }
                        else
                        {
                            pXref = (from a in pxref
                                     where a.Src_sys_id.ToUpper() == eg.RecordGuid.ToString().ToUpper() && a.EntityType.ToLower() == eg.EntityName
                                     select a).FirstOrDefault();

                            if (pXref != null)
                            {
                                if (item2.LogicalName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                                {
                                    eg.HighLevelCompany = pXref.Survivor_id;
                                }
                                else
                                {
                                    eg.RecordGuid = new Guid(pXref.Survivor_id);
                                }
                            }
                        }

                        temp.To.Add(eg);
                    }
                    temp.CC = new List<EntityGuid>();
                    foreach (var item2 in (pcentity.Attributes["cc"] as EntityCollection).Entities)
                    {
                        if (!item2.Attributes.Contains("partyid"))
                        {
                            continue;
                        }

                        tempER = (EntityReference)item2.Attributes["partyid"];

                        eg = new EntityGuid()
                        {
                            RecordGuid = tempER.Id,
                            EntityName = tempER.LogicalName
                        };

                        if (eg.EntityName == "systemuser")
                        {
                            //userOSCGuid = GetConsolidatedUsersGuid(tempER.Name);

                            string[] name = tempER.Name.Split(' ');
                            userOSCGuid = (from a in userList
                                           where a.Cci_op_active_directory_upn.Contains(name[0].ToLower().Trim()) && a.Cci_op_active_directory_upn.Contains(name[1].ToLower().Trim())
                                           orderby a.Cci_aim_prsn_seq_num descending
                                           select a.Msdyn_systemuser_guid).FirstOrDefault();
                            if (userOSCGuid != null && userOSCGuid.Length > 0)
                            {
                                eg.RecordGuid = new Guid(userOSCGuid);
                            }
                            else
                            {
                                continue;
                            }
                        }
                        else
                        {
                            pXref = (from a in pxref
                                     where a.Src_sys_id.ToUpper() == eg.RecordGuid.ToString().ToUpper() && a.EntityType.ToLower() == eg.EntityName
                                     select a).FirstOrDefault();

                            if (pXref != null)
                            {
                                if (item2.LogicalName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                                {
                                    eg.HighLevelCompany = pXref.Survivor_id;
                                }
                                else
                                {
                                    eg.RecordGuid = new Guid(pXref.Survivor_id);
                                }
                            }
                        }

                        temp.CC.Add(eg);
                    }
                    temp.BCC = new List<EntityGuid>();
                    foreach (var item2 in (pcentity.Attributes["bcc"] as EntityCollection).Entities)
                    {
                        if (!item2.Attributes.Contains("partyid"))
                        {
                            continue;
                        }
                        tempER = (EntityReference)item2.Attributes["partyid"];

                        eg = new EntityGuid()
                        {
                            RecordGuid = tempER.Id,
                            EntityName = tempER.LogicalName
                        };

                        if (eg.EntityName == "systemuser")
                        {
                            //userOSCGuid = GetConsolidatedUsersGuid(tempER.Name);

                            string[] name = tempER.Name.Split(' ');
                            userOSCGuid = (from a in userList
                                           where a.Cci_op_active_directory_upn.Contains(name[0].ToLower().Trim()) && a.Cci_op_active_directory_upn.Contains(name[1].ToLower().Trim())
                                           orderby a.Cci_aim_prsn_seq_num descending
                                           select a.Msdyn_systemuser_guid).FirstOrDefault();
                            if (userOSCGuid != null && userOSCGuid.Length > 0)
                            {
                                eg.RecordGuid = new Guid(userOSCGuid);
                            }
                            else
                            {
                                continue;
                            }
                        }
                        else
                        {
                            pXref = (from a in pxref
                                     where a.Src_sys_id.ToUpper() == eg.RecordGuid.ToString().ToUpper() && a.EntityType.ToLower() == eg.EntityName
                                     select a).FirstOrDefault();

                            if (pXref != null)
                            {
                                if (item2.LogicalName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                                {
                                    eg.HighLevelCompany = pXref.Survivor_id;
                                }
                                else
                                {
                                    eg.RecordGuid = new Guid(pXref.Survivor_id);
                                }
                            }
                        }

                        temp.BCC.Add(eg);
                    }

                    if (pcentity.Attributes.Contains("ownerid"))
                    {
                        tempER = (EntityReference)pcentity.Attributes["ownerid"];

                        eg = new EntityGuid()
                        {
                            RecordGuid = tempER.Id,
                            EntityName = tempER.LogicalName
                        };

                        if (eg.EntityName == "systemuser")
                        {
                            //userOSCGuid = GetConsolidatedUsersGuid(tempER.Name);

                            string[] name = tempER.Name.Split(' ');
                            userOSCGuid = (from a in userList
                                           where a.Cci_op_active_directory_upn.Contains(name[0].ToLower().Trim()) && a.Cci_op_active_directory_upn.Contains(name[1].ToLower().Trim())
                                           orderby a.Cci_aim_prsn_seq_num descending
                                           select a.Msdyn_systemuser_guid).FirstOrDefault();
                            if (userOSCGuid != null && userOSCGuid.Length > 0)
                            {
                                eg.RecordGuid = new Guid(userOSCGuid);
                            }
                        }
                        else
                        {
                            pXref = (from a in pxref
                                     where a.Src_sys_id.ToUpper() == eg.RecordGuid.ToString().ToUpper() && a.EntityType.ToLower() == eg.EntityName
                                     select a).FirstOrDefault();

                            if (pXref != null)
                            {
                                if (eg.EntityName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                                {
                                    eg.HighLevelCompany = pXref.Survivor_id;
                                }
                                else
                                {
                                    eg.RecordGuid = new Guid(pXref.Survivor_id);
                                }
                            }
                        }

                        temp.OwnerId = eg;
                    }

                    if (pcentity.Attributes.Contains("regardingobjectid"))
                    {
                        tempER = (EntityReference)pcentity.Attributes["regardingobjectid"];

                        eg = new EntityGuid()
                        {
                            RecordGuid = tempER.Id,
                            EntityName = tempER.LogicalName
                        };

                        if (eg.EntityName == "systemuser")
                        {
                            //userOSCGuid = GetConsolidatedUsersGuid(tempER.Name);

                            string[] name = tempER.Name.Split(' ');
                            userOSCGuid = (from a in userList
                                           where a.Cci_op_active_directory_upn.Contains(name[0].ToLower().Trim()) && a.Cci_op_active_directory_upn.Contains(name[1].ToLower().Trim())
                                           orderby a.Cci_aim_prsn_seq_num descending
                                           select a.Msdyn_systemuser_guid).FirstOrDefault();
                            if (userOSCGuid != null && userOSCGuid.Length > 0)
                            {
                                eg.RecordGuid = new Guid(userOSCGuid);
                            }
                        }
                        else
                        {
                            pXref = (from a in pxref
                                     where a.Src_sys_id.ToUpper() == eg.RecordGuid.ToString().ToUpper() && a.EntityType.ToLower() == eg.EntityName
                                     select a).FirstOrDefault();

                            if (pXref != null)
                            {
                                if (eg.EntityName == "account" && (pXref.Survivor_id.Trim().Length < 36))
                                {
                                    eg.HighLevelCompany = pXref.Survivor_id;
                                }
                                else
                                {
                                    eg.RecordGuid = new Guid(pXref.Survivor_id);
                                }
                            }
                        }

                        temp.Regarding = eg;                        
                    }

                    //if (temp.Regarding != null)
                    //{
                        emails.Add(temp);
                    //}
                    //else
                    //{
                    //    rmi.Add(new RecordMissingInfo { RecordGuid = temp.ActivityID, InfoMissing = "Missing Regarding" });
                    //}
                }

                //if (rmi.Count > 0)
                //{
                //    string path = $"C:\\Users\\jxh0tw1\\Documents\\Migration\\activity\\Email{xSource}{DateTime.Now.Year}{DateTime.Now.Month}{DateTime.Now.Day}_{DateTime.Now.Hour}_{DateTime.Now.Minute}.csv";

                //    using (var w = new StreamWriter(path))
                //    {
                //        foreach (var item in rmi)
                //        {
                //            var line = string.Format("{0},{1}", item.RecordGuid.ToString(), item.InfoMissing);
                //            w.WriteLine(line);
                //            w.Flush();
                //        }
                //    }
                //}

            }, xCancelTokenSource.Token);
            return emails;
        }

        private void SourceCB_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            xSource = (sourceCB.SelectedItem as ListBoxItem).Content.ToString();
            if (sourceCB.SelectedItem != null && sourceCB.SelectedItem != null)
            {
                btnLoadEmail.IsEnabled = true;
            }
        }

        private void ResetScreen()
        {
            biAzure.IsBusy = false;
            dgEmails.ItemsSource = null;
        }

        private async void BtMigrate_Click(object sender, RoutedEventArgs e)
        {
            biAzure.IsBusy = true;
            tbComment.Text = "";
            int countPB = 0;
            pbar.Value = 0;
            pbar.Maximum = (dgEmails.ItemsSource as List<EmailCRM>).Count;
            tbCountProcess.Text = $"{countPB}/{pbar.Maximum}";
            tbComment.Text += $"Process Started at {processDT} {Environment.NewLine}";
            CRMSync crm = new CRMSync();

            if (crm.StartConnection(xEnv.ToLower(), true))
            {
                await Task.Run(() =>
                {
                    (ConcurrentBag<UpsertResponse> processResult, ConcurrentBag<ProcessError> processE, ConcurrentBag<RecordMissingInfo> recordMI,
                    ConcurrentBag<RecordMissingInfo> missingRegarding) = crm.UpsertEmailRecords(crm.conn, (dgEmails.ItemsSource as List<EmailCRM>), 300);

                    tbComment.Dispatcher.Invoke(() => tbComment.Text += $"{processResult.Count} Emails Success" + Environment.NewLine, System.Windows.Threading.DispatcherPriority.Background);
                   
                    tbComment.Dispatcher.Invoke(() => tbComment.Text += $"{missingRegarding.Count} Emails do not have Regarding" + Environment.NewLine, System.Windows.Threading.DispatcherPriority.Background);

                    tbComment.Dispatcher.Invoke(() => tbComment.Text += $"{processE.Count} Emails Failure" + Environment.NewLine, System.Windows.Threading.DispatcherPriority.Background);

                    if (processE.Count > 0)
                    {
                        tbComment.Dispatcher.Invoke(() => tbComment.Text += $"Error Detail:" + Environment.NewLine, System.Windows.Threading.DispatcherPriority.Background);

                        int totalCount = 0;
                        Parallel.ForEach(processE, new ParallelOptions { MaxDegreeOfParallelism = 100 }, () => 0, (item, loopState, localCount) =>
                        {
                            tbComment.Dispatcher.Invoke(() => tbComment.Text += $"{item.RecordGuid}: {item.ErrorMessage} {Environment.NewLine}", System.Windows.Threading.DispatcherPriority.Background);

                            localCount++;

                            return localCount;

                        }, localCount => Interlocked.Add(ref totalCount, localCount));
                    }

                    if (recordMI.Count > 0)
                    {
                        string path = $"C:\\Users\\jxh0tw1\\Documents\\Migration\\activity\\Email{xSource}{DateTime.Now.Year}{DateTime.Now.Month}{DateTime.Now.Day}_{DateTime.Now.Hour}_{DateTime.Now.Minute}.csv";

                        using (var w = new StreamWriter(path))
                        {
                            foreach (var item in recordMI)
                            {
                                var line = string.Format("{0},{1}", item.RecordGuid.ToString(), item.InfoMissing);
                                w.WriteLine(line);
                                w.Flush();
                            }
                        }
                    }

                    countPB = processResult.Count;

                    tbComment.Dispatcher.Invoke(() => tbComment.Text += "Finished" + Environment.NewLine, System.Windows.Threading.DispatcherPriority.Background);

                });
            }

            tbCountProcess.Text = $"{countPB}/{pbar.Maximum}";
            pbar.Value = countPB;
            crm.EndConnection();

            tbComment.Text += $"Process Finished at {DateTime.Now}";

            biAzure.IsBusy = false;
        }

        private void EnvCB_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            xEnv = (envCB.SelectedItem as ListBoxItem).Content.ToString();
            if (sourceCB.SelectedItem != null && sourceCB.SelectedItem != null)
            {
                btnLoadEmail.IsEnabled = true;
            }
        }

        private List<EmailRecord> GetFiberEmail()
        {
            FiberCRMDBDC.FiberCRMDBDataContext fiberctx = (FiberCRMDBDC.FiberCRMDBDataContext)hc.CreateDBConn(xEnv, xSource);

            #region MyRegion
            //switch (xEnv)
            //{
            //    case "Dev":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Dev);
            //            break;
            //        }
            //    case "Test":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Test);
            //            break;
            //        }
            //    case "UAT":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_UAT);
            //            break;
            //        }
            //    case "STG":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_STG);
            //            break;
            //        }
            //    case "MOCK":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_UAT);
            //            break;
            //        }
            //    case "PROD":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Prod);
            //            break;
            //        }
            //    default:
            //        {
            //            throw new Exception();
            //        }
            //}

            #endregion

            fiberctx.CommandTimeout = (int)TimeSpan.FromMinutes(10).TotalSeconds;

            List<EmailRecord> pcList = new List<EmailRecord>();

            if (filterGuid.Length > 0)
            {
                pcList = (from a in fiberctx.Emails
                          where a.ActivityId == new Guid(filterGuid.Trim().ToString())
                          select new EmailRecord
                          {
                              ActivityId = a.ActivityId,
                              Subject = a.Subject,
                              Description = a.Description,
                              CreatedOn = a.CreatedOn,
                              RegardingObjectId = a.RegardingObjectId
                          }).ToList();
            }
            else
            {
                if (xFromDt.Year != 1)
                {
                     pcList = (from a in fiberctx.Emails
                              where (a.RegardingObjectTypeCode == 1 || a.RegardingObjectTypeCode == 2
                               || a.RegardingObjectTypeCode == 3 || a.RegardingObjectTypeCode == 4) && a.ModifiedOn >= xFromDt
                              select new EmailRecord
                              {
                                  ActivityId = a.ActivityId,
                                  Subject = a.Subject,
                                  Description = a.Description,
                                  CreatedOn = a.CreatedOn,
                                  RegardingObjectId = a.RegardingObjectId
                              }).ToList();
                }
                else
                {
                    pcList = (from a in fiberctx.Emails
                              where (a.RegardingObjectTypeCode == 1 || a.RegardingObjectTypeCode == 2
                               || a.RegardingObjectTypeCode == 3 || a.RegardingObjectTypeCode == 4) && a.CreatedOn >= DateTime.Parse("2019-01-01")
                              select new EmailRecord
                              {
                                  ActivityId = a.ActivityId,
                                  Subject = a.Subject,
                                  Description = a.Description,
                                  CreatedOn = a.CreatedOn,
                                  RegardingObjectId = a.RegardingObjectId
                              }).ToList();
                }
            }

            fiberctx.Connection.Close();
            fiberctx.Connection.Dispose();

            return pcList;
        }

        private List<ActivityPartyRecord> GetFiberActivityParty()
        {
            FiberCRMDBDC.FiberCRMDBDataContext fiberctx = (FiberCRMDBDC.FiberCRMDBDataContext)hc.CreateDBConn(xEnv, xSource);

            #region MyRegion
            //switch (xEnv)
            //{
            //    case "Dev":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Dev);
            //            break;
            //        }
            //    case "Test":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Test);
            //            break;
            //        }
            //    case "UAT":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_UAT);
            //            break;
            //        }
            //    case "STG":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_STG);
            //            break;
            //        }
            //    case "MOCK":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_UAT);
            //            break;
            //        }
            //    case "PROD":
            //        {
            //            fiberctx = new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Prod);
            //            break;
            //        }
            //    default:
            //        {
            //            throw new Exception();
            //        }
            //}

            #endregion

            fiberctx.CommandTimeout = (int)TimeSpan.FromMinutes(10).TotalSeconds;

            List<ActivityPartyRecord> apList = new List<ActivityPartyRecord>();

            if (xFromDt.Year != 1)
            {
                apList = (from a in fiberctx.ActivityParties
                          join r in fiberctx.EntityViews on a.PartyObjectTypeCode equals r.ObjectTypeCode
                          join b in fiberctx.Emails on a.ActivityId equals b.ActivityId
                          where a.ActivityTypeCode == 4202 && b.ModifiedOn >= xFromDt &&
                          (b.RegardingObjectTypeCode == 1 || b.RegardingObjectTypeCode == 2 ||
                           b.RegardingObjectTypeCode == 3 || b.RegardingObjectTypeCode == 4)
                          select new ActivityPartyRecord
                          {
                              ActivityId = a.ActivityId,
                              PartyId = a.PartyId,
                              PartyObjectTypeCode = a.PartyObjectTypeCode,
                              LogicalName = r.LogicalName,
                              ParticipationTypeMask = a.ParticipationTypeMask
                          }).ToList();
            }
            else
            {
                apList = (from a in fiberctx.ActivityParties
                          join r in fiberctx.EntityViews on a.PartyObjectTypeCode equals r.ObjectTypeCode
                          join b in fiberctx.Emails on a.ActivityId equals b.ActivityId
                          where a.ActivityTypeCode == 4202 && b.CreatedOn >= DateTime.Parse("2019-01-01") &&
                          (b.RegardingObjectTypeCode == 1 || b.RegardingObjectTypeCode == 2 ||
                           b.RegardingObjectTypeCode == 3 || b.RegardingObjectTypeCode == 4)
                          select new ActivityPartyRecord
                          {
                              ActivityId = a.ActivityId,
                              PartyId = a.PartyId,
                              PartyObjectTypeCode = a.PartyObjectTypeCode,
                              LogicalName = r.LogicalName,
                              ParticipationTypeMask = a.ParticipationTypeMask
                          }).ToList();
            }

            fiberctx.Connection.Close();
            fiberctx.Connection.Dispose();

            return apList;
        }

        private List<EmailRecord> GetWirelessEmail()
        { 
            WirelessCRMDBDC.WirelessCRMDBDataContext wctx = (WirelessCRMDBDC.WirelessCRMDBDataContext)hc.CreateDBConn(xEnv, xSource);

            #region MyRegion
            //switch (xEnv)
            //{
            //    case "Dev":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Dev);
            //            break;
            //        }
            //    case "Test":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Test);
            //            break;
            //        }
            //    case "UAT":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_UAT);
            //            break;
            //        }
            //    case "STG":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_STG);
            //            break;
            //        }
            //    case "MOCK":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_UAT);
            //            break;
            //        }
            //    case "PROD":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Prod);
            //            break;
            //        }
            //    default:
            //        {
            //            throw new Exception();
            //        }
            //}

            #endregion

            List<EmailRecord> pcList = new List<EmailRecord>();

            if (filterGuid.Length > 0)
            {
                pcList = (from a in wctx.Emails
                          where a.ActivityId == new Guid(filterGuid.Trim().ToString())
                          select new EmailRecord
                          {
                              ActivityId = a.ActivityId,
                              Subject = a.Subject,
                              Description = a.Description,
                              CreatedOn = a.CreatedOn,
                              RegardingObjectId = a.RegardingObjectId
                          }).ToList();
            }
            else
            {
                if (xFromDt.Year != 1)
                {
                    pcList = (from a in wctx.Emails
                              where (a.RegardingObjectTypeCode == 1 || a.RegardingObjectTypeCode == 2
                               || a.RegardingObjectTypeCode == 3 || a.RegardingObjectTypeCode == 4 || a.RegardingObjectTypeCode == 10007) && a.ModifiedOn >= xFromDt
                              select new EmailRecord
                              {
                                  ActivityId = a.ActivityId,
                                  Subject = a.Subject,
                                  Description = a.Description,
                                  CreatedOn = a.CreatedOn,
                                  RegardingObjectId = a.RegardingObjectId
                              }).ToList();
                }
                else
                {
                    pcList = (from a in wctx.Emails
                              where  (a.RegardingObjectTypeCode == 1 || a.RegardingObjectTypeCode == 2
                               || a.RegardingObjectTypeCode == 3 || a.RegardingObjectTypeCode == 4 || a.RegardingObjectTypeCode == 10007) && a.CreatedOn >= DateTime.Parse("2019-01-01")
                              select new EmailRecord
                              {
                                  ActivityId = a.ActivityId,
                                  Subject = a.Subject,
                                  Description = a.Description,
                                  CreatedOn = a.CreatedOn,
                                  RegardingObjectId = a.RegardingObjectId
                              }).ToList();
                }
            }

            wctx.Connection.Close();
            wctx.Connection.Dispose();

            return pcList;
        }

        private List<ActivityPartyRecord> GetWirelessActivityParty()
        {
            WirelessCRMDBDC.WirelessCRMDBDataContext wctx = (WirelessCRMDBDC.WirelessCRMDBDataContext)hc.CreateDBConn(xEnv, xSource);

            #region MyRegion
            //switch (xEnv)
            //{
            //    case "Dev":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Dev);
            //            break;
            //        }
            //    case "Test":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Test);
            //            break;
            //        }
            //    case "UAT":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_UAT);
            //            break;
            //        }
            //    case "STG":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_STG);
            //            break;
            //        }
            //    case "MOCK":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_UAT);
            //            break;
            //        }
            //    case "PROD":
            //        {
            //            wctx = new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Prod);
            //            break;
            //        }
            //    default:
            //        {
            //            throw new Exception();
            //        }
            //}

            #endregion

            List<ActivityPartyRecord> apList = new List<ActivityPartyRecord>();

            if (xFromDt.Year != 1)
            {
                apList = (from a in wctx.ActivityParties
                          join r in wctx.EntityViews on a.PartyObjectTypeCode equals r.ObjectTypeCode
                          join b in wctx.Emails on a.ActivityId equals b.ActivityId
                          where a.ActivityTypeCode == 4202 && b.ModifiedOn >= xFromDt &&
                          (b.RegardingObjectTypeCode == 1 || b.RegardingObjectTypeCode == 2 ||
                           b.RegardingObjectTypeCode == 3 || b.RegardingObjectTypeCode == 4 || b.RegardingObjectTypeCode == 10007)
                          select new ActivityPartyRecord
                          {
                              ActivityId = a.ActivityId,
                              PartyId = a.PartyId,
                              PartyObjectTypeCode = a.PartyObjectTypeCode,
                              LogicalName = r.LogicalName,
                              ParticipationTypeMask = a.ParticipationTypeMask,
                              UserEmail = (from u in wctx.SystemUsers
                                           where u.SystemUserId == a.PartyId
                                           select u.InternalEMailAddress).FirstOrDefault()
                          }).ToList();
            }
            else
            {
                apList = (from a in wctx.ActivityParties
                          join r in wctx.EntityViews on a.PartyObjectTypeCode equals r.ObjectTypeCode
                          join b in wctx.Emails on a.ActivityId equals b.ActivityId
                          where a.ActivityTypeCode == 4202 && b.CreatedOn >= DateTime.Parse("2019-01-01") &&
                          (b.RegardingObjectTypeCode == 1 || b.RegardingObjectTypeCode == 2 ||
                           b.RegardingObjectTypeCode == 3 || b.RegardingObjectTypeCode == 4 || b.RegardingObjectTypeCode == 10007)
                          select new ActivityPartyRecord
                          {
                              ActivityId = a.ActivityId,
                              PartyId = a.PartyId,
                              PartyObjectTypeCode = a.PartyObjectTypeCode,
                              LogicalName = r.LogicalName,
                              ParticipationTypeMask = a.ParticipationTypeMask,
                              UserEmail = (from u in wctx.SystemUsers
                                           where u.SystemUserId == a.PartyId
                                           select u.InternalEMailAddress).FirstOrDefault()
                          }).ToList();
            }

            wctx.Connection.Close();
            wctx.Connection.Dispose();

            return apList;
        }

    }
}
