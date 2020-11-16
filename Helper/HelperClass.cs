using MigrateEmailAndPhoneCallWPF.Properties;
using MigrateEmailAndPhoneCallWPF.UserControls;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.Xml;
using System.Text;
using System.Threading.Tasks;

namespace MigrateEmailAndPhoneCallWPF.Helper
{
    public class HelperClass
    {
        public object CreateDBConn(string pEnv, string pSrc)
        {
            switch ($"{pEnv}-{pSrc}")
            {
                case "Dev-Fiber":
                    {
                        return new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Dev);
                    }
                case "Dev-Wireless":
                    {
                        return new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Dev);
                    }
                case "Dev-Migration":
                    {
                        return new MigrationDBDC.MigrationDBDataContext(Settings.Default.MigrationDB_Dev);
                    }
                case "Dev-IHUB2":
                    {
                        return new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_DEV);
                    }

                case "Test-Fiber":
                    {
                        return new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Test);
                    }
                case "Test-Wireless":
                    {
                        return new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Test);
                    }
                case "Test-Migration":
                    {
                        return new MigrationDBDC.MigrationDBDataContext(Settings.Default.MigrationDB_Test);
                    }
                case "Test-IHUB2":
                    {
                        return new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_Test);
                    }

                case "UAT-Fiber":
                    {
                        return new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_UAT);
                    }
                case "UAT-Wireless":
                    {
                        return new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_UAT);
                    }
                case "UAT-Migration":
                    {
                        return new MigrationDBDC.MigrationDBDataContext(Settings.Default.MigrationDB_UAT);
                    }
                case "UAT-IHUB2":
                    {
                        return new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_UAT);
                    }

                case "STG-Fiber":
                    {
                        return new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_STG);
                    }
                case "STG-Wireless":
                    {
                       return new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_STG);
                    }
                case "STG-Migration":
                    {
                        return new MigrationDBDC.MigrationDBDataContext(Settings.Default.MigrationDB_STG);
                    }
                case "STG-IHUB2":
                    {
                        return new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_STG);
                    }

                case "MOCK-Fiber":
                    {
                        return new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_UAT);
                    }
                case "MOCK-Wireless":
                    {
                        return new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_UAT);
                    }
                case "MOCK-Migration":
                    {
                        return new MigrationDBDC.MigrationDBDataContext(Settings.Default.MigrationDB_UAT);
                    }
                case "MOCK-IHUB2":
                    {
                        return new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_UAT);
                    }

                case "PROD-Fiber":
                    {
                        return new FiberCRMDBDC.FiberCRMDBDataContext(Settings.Default.FiberCRMDB_Prod);
                    }
                case "PROD-Wireless":
                    {
                        return new WirelessCRMDBDC.WirelessCRMDBDataContext(Settings.Default.WirelessCRMDB_Prod);
                    }
                case "PROD-Migration":
                    {
                        return new MigrationDBDC.MigrationDBDataContext(Settings.Default.MigrationDB_Prod);
                    }
                case "PROD-IHUB2":
                    {
                        return new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_Prod);
                    }

                default:
                    {
                        throw new Exception();
                    }
            }
        }

        public List<UserRecord> GetConsolidatedUsers(string pEnv, string pSrc)
        {
            MigrationDBDC.MigrationDBDataContext migctx = (MigrationDBDC.MigrationDBDataContext)CreateDBConn(pEnv, "Migration");

            List<UserRecord> userList = new List<UserRecord>();
            if (pSrc == "Fiber")
            {
                userList = (from a in migctx.consolidated_system_users
                            where a.msdyn_systemuser_guid != null && a.fiber_crm_guid != null
                            select new UserRecord
                            {
                                Cci_op_active_directory_upn = a.cci_op_active_directory_upn,
                                Fiber_crm_guid = a.fiber_crm_guid,
                                Cci_aim_prsn_seq_num = a.cci_aim_prsn_seq_num,
                                Msdyn_systemuser_guid = a.msdyn_systemuser_guid
                            }).ToList();

            }
            else
            {
                userList = (from a in migctx.consolidated_system_users
                            where a.msdyn_systemuser_guid != null && a.cci_op_active_directory_upn != null
                            select new UserRecord
                            {
                                Cci_op_active_directory_upn = a.cci_op_active_directory_upn,
                                Fiber_crm_guid = a.fiber_crm_guid,
                                Cci_aim_prsn_seq_num = a.cci_aim_prsn_seq_num,
                                Msdyn_systemuser_guid = a.msdyn_systemuser_guid
                            }).ToList();
            }

            migctx.Connection.Close();
            migctx.Connection.Dispose();

            return userList;
        }

        public List<PublishXRef> GetPublishXRef(string pEnv, string pSrc)
        {
            IHUB2DBDC.IHUB2DBDataContext ihubctx = (IHUB2DBDC.IHUB2DBDataContext)CreateDBConn(pEnv, "IHUB2");
            #region MyRegion
            //switch (xEnv)
            //{
            //    case "Dev":
            //        {
            //            ihubctx = new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_DEV);
            //            break;
            //        }
            //    case "Test":
            //        {
            //            ihubctx = new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_Test);
            //            break;
            //        }
            //    case "UAT":
            //        {
            //            ihubctx = new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_UAT);
            //            break;
            //        }
            //    case "STG":
            //        {
            //            ihubctx = new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_STG);
            //            break;
            //        }
            //    case "MOCK":
            //        {
            //            ihubctx = new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_UAT);
            //            break;
            //        }
            //    case "PROD":
            //        {
            //            ihubctx = new IHUB2DBDC.IHUB2DBDataContext(Settings.Default.IHUBDATA2_Prod);
            //            break;
            //        }
            //    default:
            //        {
            //            throw new Exception();
            //        }
            //}
            #endregion

            List<PublishXRef> prefList = new List<PublishXRef>();
            switch (pSrc)
            {
                case "Fiber":
                    {
                        prefList = (from a in ihubctx.Publish_XREFs
                                    where a.source == "FiberCRM" && a.is_survivor == "No" && a.active_flag == "Yes"
                                    select new PublishXRef
                                    {
                                        Is_Survivor = a.is_survivor,
                                        Source = a.source,
                                        Src_sys_id = a.src_sys_id,
                                        EntityType = a.entity_type,
                                        Is_Active = a.active_flag,
                                        Survivor_id = a.survivor_id
                                    }).ToList();
                        break;
                    }
                case "Wireless":
                    {
                        prefList = (from a in ihubctx.Publish_XREFs
                                    where a.source == "WirelessCRM" && a.is_survivor == "No" && a.active_flag == "Yes"
                                    select new PublishXRef
                                    {
                                        Is_Survivor = a.is_survivor,
                                        Source = a.source,
                                        Src_sys_id = a.src_sys_id,
                                        EntityType = a.entity_type,
                                        Is_Active = a.active_flag,
                                        Survivor_id = a.survivor_id
                                    }).ToList();
                        break;
                    }
                case "InsideSales":
                    {
                        prefList = (from a in ihubctx.Publish_XREFs
                                    where a.source == "InsideSales" && a.is_survivor == "No" && a.active_flag == "Yes"
                                    select new PublishXRef
                                    {
                                        Is_Survivor = a.is_survivor,
                                        Source = a.source,
                                        Src_sys_id = a.src_sys_id,
                                        EntityType = a.entity_type,
                                        Is_Active = a.active_flag,
                                        Survivor_id = a.survivor_id
                                    }).ToList();
                        break;
                    }
                default:
                    break;
            }
            ihubctx.Connection.Close();
            ihubctx.Connection.Dispose();
            return prefList;
        }

    }
}
