using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mono.Data.Sqlite;
using System.Data;
using System.IO;
using clsTabletCommon.ITPExternal;

namespace ITPAndroidApp
{
    public class clsTabletDB
    {
        public class ITPStaticTable
        {
            string[] colHeaderNames = { "AutoID", "TableName", "VersionNumber", "VersionDate" };
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](250) NOT NULL", "[float] NULL", "[nvarchar](50) null" };
            string[] colHeaderBaseTypes = { "autoincrement", "string", "float", "string" };
            LocalDB DB = new LocalDB();
            string sDocITPStaticTableName = "ITPStaticTableDetails";

            public bool CheckStaticTable()
            {
                if (!DB.TableExists(sDocITPStaticTableName))
                {

                    return DB.CreateTable(sDocITPStaticTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }
            }

            public bool IsNewVersionOfTable(string sSessionId, string sUser, string sTableName, ref double dNewVersionNumber, ref DateTime dtLastLocalVersionDate)
            {
                string sSQLLocal = "Select VersionNumber, VersionDate from " + sDocITPStaticTableName + " where TableName = '" + sTableName + "'";
                string sRtnMsg = "";
                double dLocalVersion = -1.0;
                string[] colNameLocal = new string[2];
                int iColNo = -1;
                DateTime dtVersiondate;

                colNameLocal[0] = "VersionNumber";
                colNameLocal[1] = "VersionDate";

                DataSet ds = DB.ReadSQLDataSet(sSQLLocal, colNameLocal, ref sRtnMsg);
                if (ds.Tables[0].Rows.Count > 0)
                {
                    iColNo = ds.Tables[0].Columns["VersionNumber"].Ordinal;
                    dLocalVersion = ds.Tables[0].Rows[j].ItemArray[iColNo].ToString();
                    iColNo = ds.Tables[0].Columns["VersionDate"].Ordinal;
                    dtVersiondate = Convert.ToDateTime(ds.Tables[0].Rows[j].ItemArray[iColNo].ToString());
                    dtLastLocalVersionDate = dtVersiondate;
                }
                else
                {
                    dLocalVersion = 0;
                    dtLastLocalVersionDate = DateTime.Now;
                }
                wbsITP_External ws = new wbsITP_External();
                object[] objRemote = ws.GetITPStaticTableVersionNumber(sSessionId, sUser, sTableName);
                double dRemoteVersion = 0.0;

                if ((bool)objRemote[0])
                {
                    dRemoteVersion = Convert.ToDouble(objRemote[1]);
                }

                if (dLocalVersion < dRemoteVersion)
                {
                    dNewVersionNumber = dRemoteVersion;
                    return true;
                }
                else
                {
                    return false;
                }

            }

            public bool VersionRecordExists(string sTableName)
            {
                string sSQL = "Select * from " + sDocITPStaticTableName + " where TableName = '" + sTableName + "'";
                string sRtnMsg = "";
                int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
                if (iRecords > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }

            }

            public bool UpdateVersionNumber(string sTableName, double dNewVersionUmber)
            {
                try
                {
                    string sSQL = "";
                    string sRtnString = "";
                    DateTime dtNow = DateTime.Now;
                    DateClass clsDt = new DateClass();
                    string sdtNow = clsDt.Get_Date_String(dtNow, "dd/mm/yyyy");
                    if (VersionRecordExists(sTableName))
                    {
                        sSQL = "Update " + sDocITPStaticTableName + " set VersionNumber = " + dNewVersionUmber + ", VersionDate = '" + sdtNow + "' where TableName = '" + sTableName + "'";
                    }
                    else
                    {
                        sSQL = "Insert into " + sDocITPStaticTableName + " (TableName, VersionNumber, VersionDate) Values ('" + sTableName + "'," + dNewVersionUmber + ",'" + sdtNow + "')";
                    }

                    return DB.ExecuteSQL(sSQL, ref sRtnString);
                }
                catch
                {
                    return false;
                }
            }
        }

        public class ITPHeaderTable
        {
            string[] colHeaderNames = { "AutoID", "ID", "ITPType", "PwrID", "SystemVolts", "DocumentID", "VersionNo", "AuditUserId", "AuditDateStamp", "SubContractorID", "Downloaded", "DownloadUserId", "ProjectDesc" };
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NOT NULL", "[nvarchar](100) NULL", "[nvarchar](50) NULL", "[float] NULL", "[nvarchar](50) NULL",
                                       "[int] NULL", "[nvarchar](50) NULL", "[nvarchar] (50) NULL", "[nvarchar](50) NULL", "[int] NULL", "[nvarchar](50) NULL", "[nvarchar](250) NULL" };
            string[] colHeaderBaseTypes = { "autoincrement", "string", "string", "string", "float", "string", "int", "string", "string", "string", "int", "string", "string" };
            LocalDB DB = new LocalDB();
            string sDocHeaderTableName = "ITPDocumentHeader";

            public bool CheckHeaderTable()
            {

                if (!DB.TableExists(sDocHeaderTableName))
                {

                    return DB.CreateTable(sDocHeaderTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }


            }

            public bool TableHeaderDeleteAllRecords(string sId, ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();

                if (DB.TableExists(sDocHeaderTableName))
                {

                    sSQL = "delete from " + sDocHeaderTableName + " where ID = '" + sId + "'";
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }

            }

            public bool TableHeaderAddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sDocHeaderTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
                return true;
            }

            public DataSet GetLocalITPProjects()
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[3];

                colNames[0] = "ID";
                colNames[1] = "ProjectDesc";
                colNames[2] = "Downloaded";

                if (DB.TableExists(sDocHeaderTableName))
                {
                    sSQL = "select DISTINCT ID,ProjectDesc, Downloaded from " + sDocHeaderTableName + " order by Downloaded";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    return ds;
                }
                else
                {
                    DataSet ds = new DataSet();
                    return ds;
                }
            }

            public bool MarkLocalITPDownloaded(string sId, int iStatus, ref string sRtnMsg)
            {
                //Now also mark them the same locally
                string sSQL = "Update " + sDocHeaderTableName + " set Downloaded = " + iStatus + " where ID = '" + sId + "'";
                return DB.ExecuteSQL(sSQL, ref sRtnMsg);
            }
        }

        public class ITPQuestionnaire
        {
            string[] colHeaderNames = { "Row", "SectionID", "Item", "ITPCode" };
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[int] NULL", "[nvarchar](8000) NULL", "[nvarchar](50) NULL" };
            string[] colHeaderBaseTypes = { "autoincrement", "int", "string", "string" };
            LocalDB DB = new LocalDB();
            public string sDocQuestionnaireTableName = "ITPQuestionnaire";

            public bool CheckFullQuestionnaireTable()
            {

                if (!DB.TableExists(sDocQuestionnaireTableName))
                {

                    return DB.CreateTable(sDocQuestionnaireTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }


            }
            public bool TableQuestionnaireDeleteAllRecords(ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();

                if (DB.TableExists(sDocQuestionnaireTableName))
                {

                    sSQL = "delete from " + sDocQuestionnaireTableName;
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }

            }

            public bool TableQuestionnaireAddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sDocQuestionnaireTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
                return true;
            }
        }

        public class ITPTypes
        {
            string[] colHeaderNames = { "AutoId", "ITPCode", "ITPDescription" };
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NULL", "[nvarchar](50) NULL" };
            string[] colHeaderBaseTypes = { "autoincrement", "string", "string" };
            LocalDB DB = new LocalDB();
            public string sITPTypeTableName = "ITPTypes";

            public bool CheckFullITPTypeTable()
            {

                if (!DB.TableExists(sITPTypeTableName))
                {

                    return DB.CreateTable(sITPTypeTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }


            }
            public bool TableITPTypeDeleteAllRecords(ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();

                if (DB.TableExists(sITPTypeTableName))
                {

                    sSQL = "delete from " + sITPTypeTableName;
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }

            }

            public bool TableITPTypeAddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sITPTypeTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
                return true;
            }

            public bool FillITPTypeMainTable(string sSessionId, string sUser, ref string sRtnMsg)
            {
                try
                {
                    ITPStaticTable Static = new ITPStaticTable();
                    double dNewVersionNumber = 0.0;
                    //Only do all of this if the version has changed. So get the local versoin umber and compare to that on the DB. If different do all of this. - WRITE LATER as a general function
                    bool bNewVersion = Static.IsNewVersionOfTable(sSessionId, sUser, sITPTypeTableName, ref dNewVersionNumber);
                    if (!DB.TableExists(sITPTypeTableName) || bNewVersion)
                    {
                        clsLocalUtils util = new clsLocalUtils();
                        string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                        wbsITP_External ws = new wbsITP_External();
                        ws.Url = sURL;
                        object[] objQuestions = ws.GetITPFullITPTypeInfo(sSessionId, sUser);
                        if (objQuestions[0].ToString() == "Success")
                        {
                            if (TableITPTypeDeleteAllRecords(ref sRtnMsg))
                            {
                                string sITPDocumentQuestionnaireInfo = objQuestions[1].ToString();
                                string[] sHeaderInfo = sITPDocumentQuestionnaireInfo.Split('~');
                                if (sHeaderInfo[0] == "ITPTypeInfo")
                                {
                                    string[] delimiters = new string[] { "||" };
                                    string[] sQuestionnaireItems = sHeaderInfo[1].Split(delimiters, StringSplitOptions.RemoveEmptyEntries);
                                    int iQuestionCount = sQuestionnaireItems.Length;
                                    if (iQuestionCount > 0)
                                    {
                                        //First check if the ITPType table exists and if not create it
                                        if (CheckFullITPTypeTable())
                                        {
                                            for (int i = 0; i < iQuestionCount; i++)
                                            {
                                                string[] delimiters2 = new string[] { "^" };
                                                string[] sQuestionItems = sQuestionnaireItems[i].Split(delimiters2, StringSplitOptions.None);
                                                TableITPTypeAddRecord(sQuestionItems);
                                            }
                                        }
                                    }
                                }
                            }
                            //Update the version number locally
                            Static.UpdateVersionNumber(sITPTypeTableName, dNewVersionNumber);
                            return true;
                        }
                        else
                        {
                            sRtnMsg = objQuestions[1].ToString();
                            return false;
                        }
                    }
                    else
                    {
                        //This means you don't have to fill this static table
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    sRtnMsg = "Failure" + ex.Message.ToString();
                    return false;
                }
            }
        }

        public class ITPInventoryTypes
        {
            string[] colHeaderNames = { "AutoId", "Model", "Make", "SPN" };
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](500) NULL", "[nvarchar](500) NULL", "[nvarchar](10) NULL" };
            string[] colHeaderBaseTypes = { "autoincrement", "string", "string", "string" };
            LocalDB DB = new LocalDB();
            public string sITPInventoryTableName = "ITPInventoryTypes";

            public bool CheckFullITPInventoryTypeTable()
            {

                if (!DB.TableExists(sITPInventoryTableName))
                {

                    return DB.CreateTable(sITPInventoryTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }


            }
            public bool TableITPInventoryTypeDeleteAllRecords(ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();

                if (DB.TableExists(sITPInventoryTableName))
                {

                    sSQL = "delete from " + sITPInventoryTableName;
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }

            }

            public bool TableITPInventoryTypeAddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sITPInventoryTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
                return true;
            }

            public bool FillITPInventoryTypeTable(string sSessionId, string sUser, ref string sRtnMsg)
            {
                try
                {
                    ITPStaticTable Static = new ITPStaticTable();
                    double dNewVersionNumber = 0.0;
                    //Only do all of this if the version has changed. So get the local versoin umber and compare to that on the DB. If different do all of this. - WRITE LATER as a general function
                    bool bNewVersion = Static.IsNewVersionOfTable(sSessionId, sUser, sITPInventoryTableName, ref dNewVersionNumber);

                    //Now also check the last date the inventory list was updated. If more than 3 months then we should automatically redo it.
                    DateTime dtNow = DateTime.Now;


                    if (!DB.TableExists(sITPInventoryTableName) || bNewVersion)
                    {
                        clsLocalUtils util = new clsLocalUtils();
                        string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                        wbsITP_External ws = new wbsITP_External();
                        ws.Url = sURL;
                        object[] objInventory = ws.GetITPFullITPTypeInfo(sSessionId, sUser);
                        if (objInventory[0].ToString() == "Success")
                        {
                            if (TableITPInventoryTypeDeleteAllRecords(ref sRtnMsg))
                            {
                                string sITPInventoryInfo = objInventory[1].ToString();
                                string[] sHeaderInfo = sITPInventoryInfo.Split('~');
                                if (sHeaderInfo[0] == "ITPBatteryMakeAndModelInfo")
                                {
                                    string[] delimiters = new string[] { "||" };
                                    string[] sInventoryItems = sHeaderInfo[1].Split(delimiters, StringSplitOptions.RemoveEmptyEntries);
                                    int iInventoryTypeCount = sInventoryItems.Length;
                                    if (iInventoryTypeCount > 0)
                                    {
                                        //First check if the ITPType table exists and if not create it
                                        if (CheckFullITPInventoryTypeTable())
                                        {
                                            for (int i = 0; i < iInventoryTypeCount; i++)
                                            {
                                                string[] delimiters2 = new string[] { "^" };
                                                string[] sInventoryMakeItems = sInventoryItems[i].Split(delimiters2, StringSplitOptions.None);
                                                TableITPInventoryTypeAddRecord(sInventoryMakeItems);
                                            }
                                        }
                                    }
                                }
                            }
                            //Update the version number locally
                            Static.UpdateVersionNumber(sITPInventoryTableName, dNewVersionNumber);
                            return true;
                        }
                        else
                        {
                            sRtnMsg = objInventory[1].ToString();
                            return false;
                        }
                    }
                    else
                    {
                        //This means you don't have to fill this static table
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    sRtnMsg = "Failure" + ex.Message.ToString();
                    return false;
                }
            }
        }

        public class ITPDocumentSection
        {
            LocalDB DB = new LocalDB();

            string[] colHeaderNames = { "AutoId", "DocumentId", "MappedSectionId", "DisplaySectionID", "Name", "QuestionType" };
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NULL", "[int] NULL", "[int] NULL", "[nvarchar] (8000) NULL", "[int] NULL" };
            string[] colHeaderBaseTypes = { "autoincrement", "string", "int", "int", "string", "int" };
            public string sITPDocumentSectionTableName = "ITPDocumentSection";

            //These arrays are with the autoId column for these times we need to define the complete table
            string[] colSectionHeaderNames = { "AutoID", "ID", "SectionID", "Question", "Yes", "No", "NA", "Comments", "Audit_UserId", "Audit_DateStamp" };
            string[] colSectionHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NULL", "[int] NULL", "[nvarchar] (8000) NULL", "[bit] NULL", "[bit] NULL", "[bit] NULL", "[nvarchar] (8000) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL" };
            string[] colSectionHeaderBaseTypes = { "autoincrement", "string", "int", "string", "bit", "bit", "bit", "string", "string", "string" };
            //These arrays are without the autoId column because we do not bring this across and don't want it populated by the code
            string[] colSectionQuestionMstNames = { "ID", "SectionID", "Question", "Yes", "No", "NA", "Comments" };
            string[] colSectionQuestionMstTypes = { "[nvarchar](50) NULL", "[int] NULL", "[nvarchar] (8000) NULL", "[bit] NULL", "[bit] NULL", "[bit] NULL", "[nvarchar] (8000) NULL" };
            string[] colSectionQuestionMstBaseTypes = { "string", "int", "string", "bit", "bit", "bit", "string" };
            public string sITPSectionQuestionsTableName = "ITPQuestionnaireMst";

            //These arrays are with the autoId column for these times we need to define the complete table
            string[] colSection10HeaderNames = { "AutoID", "ID", "PWRID", "BankNo", "Floor", "Suite", "Rack", "SubRack", "Make", "Model", "SerialBatch", "DOM", 
                                                 "FuseOrCB", "RatingAmps", "LinkTest", "BatteryTest", "Audit_UserId", "Audit_DateStamp", 
                                                 "tblMaximoTransfer_Eqnum", "tblMaximoPSA_ID", "Equipment_Condition", "SPN", "Duplicate" };
            string[] colSection10HeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", 
                                                 "[nvarchar](50) NULL","[nvarchar](8000) NULL","[nvarchar](8000) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL",
                                                 "[nvarchar](50) NULL","[nvarchar](10) NULL","[int] NULL","[int] NULL", "[nvarchar] (50) NULL", "[nvarchar] (50) NULL",
                                                 "[nvarchar] (50) NULL", "[int] NULL", "[nvarchar] (50) NULL",  "[nvarchar] (50) NULL", "[int] NULL"};
            string[] colSection10HeaderBaseTypes = { "autoincrement", "string", "string", "string", "string", "string", "string", 
                                                     "string", "string", "string", "string", "string",
                                                     "string", "string", "int", "int", "string", "string",
                                                     "string", "int", "string", "string", "int"};
            //These arrays are without the autoId column because we do not bring this across and don't want it populated by the code
            string[] colSection10ItemsNames = { "ID", "PWRID", "BankNo", "Floor", "Suite", "Rack", "SubRack", "Make", "Model", "SerialBatch", "DOM", 
                                                 "FuseOrCB", "RatingAmps", "LinkTest", "BatteryTest", "tblMaximoTransfer_Eqnum", "tblMaximoPSA_ID", "Equipment_Condition", "SPN", "Duplicate" };
            string[] colSection10ItemsTypes = {  "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", 
                                                 "[nvarchar](50) NULL","[nvarchar](8000) NULL","[nvarchar](8000) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL",
                                                 "[nvarchar](50) NULL","[nvarchar](10) NULL","[int] NULL","[int] NULL",
                                                 "[nvarchar] (50) NULL", "[int] NULL", "[nvarchar] (50) NULL", "[nvarchar] (50) NULL", "[int] NULL"};
            string[] colSection10ItemsBaseTypes = {  "string", "string", "string", "string", "string", "string", 
                                                     "string", "string", "string", "string", "string",
                                                     "string", "string", "int", "int",
                                                     "string", "int", "string", "string", "int",};
            
            public string sITPSection10TableName = "ITPSection10";

            public bool CheckFullDocumentSectionTable()
            {

                if (!DB.TableExists(sITPDocumentSectionTableName))
                {

                    return DB.CreateTable(sITPDocumentSectionTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }


            }
            public bool TableITPDocumentSectionDeleteAllRecords(ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();

                if (DB.TableExists(sITPDocumentSectionTableName))
                {

                    sSQL = "delete from " + sITPDocumentSectionTableName;
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }

            }

            public bool TableITPDocumentSectionAddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sITPDocumentSectionTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
                return true;
            }

            public bool CheckQuestionTableMst()
            {
                if (!DB.TableExists(sITPSectionQuestionsTableName))
                {
                    if (!DB.CreateTable(sITPSectionQuestionsTableName, colSectionHeaderNames, colSectionHeaderTypes))
                    {
                        return false;
                    }
                }

                return true;
            }

            public bool ITPProjectQuestionAddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sITPSectionQuestionsTableName, colSectionQuestionMstNames, colSectionQuestionMstBaseTypes, sItemValues);
                return true;
            }

            public bool FillLocalITPSections(string sId, ref string sRtnMsg)
            {
                string sSQL;
                string[] colNames = new string[1];

                colNames[0] = "SectionID";

                if (!CheckQuestionTableMst())
                {
                    return false;
                }

                if (!DB.TableExists(sITPSectionQuestionsTableName))
                {
                    if (!DB.CreateTable(sITPSectionQuestionsTableName, colSectionHeaderNames, colSectionHeaderTypes))
                    {
                        return false;
                    }
                }

                if (!IsProjectQuestionExist(sId))
                {
                    sSQL = "insert into " + sITPSectionQuestionsTableName + " ( " +
                           "ID, SectionID, Question,Yes,[No],NA) " +
                           "select H.ID, Q.SectionId, Q.Item, 0,0,0 " +
                           "from ITPDocumentHeader H, ITPTypes T, ITPQuestionnaire Q " +
                           "where H.ID = '" + sId + "' " +
                           "and H.ITPType = T.ITPDescription " +
                           "and Q.ITPCode = T.ITPCode";
                    DB.ExecuteSQL(sSQL, ref sRtnMsg);
                    if (sRtnMsg != "")
                    {
                        return false;
                    }
                }

                return true;
            }

            public bool ITPProjectSectionDeleteAllQuestions(string sId, ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();

                if (DB.TableExists(sITPSectionQuestionsTableName))
                {

                    sSQL = "delete from " + sITPSectionQuestionsTableName + " where ID = '" + sId + "'";
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }

            }

            public bool IsProjectQuestionExist(string sId)
            {
                string sSQL = "Select * from " + sITPSectionQuestionsTableName + " where ID = '" + sId + "'";
                string sRtnMsg = "";
                int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
                if (iRecords > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }

            }

            public DataSet GetLocalITPSections(string sId)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[3];

                colNames[0] = "SectionId";
                colNames[1] = "Name";
                colNames[2] = "QuestionType";

                if (DB.TableExists(sITPSectionQuestionsTableName) && DB.TableExists(sITPDocumentSectionTableName))
                {
                    sSQL = "select distinct SectionId, D.Name as Name, D.QuestionType as QuestionType  " +
                           "from " + sITPSectionQuestionsTableName + " M, " + sITPDocumentSectionTableName + " D " +
                           "where M.id = '" + sId + "' " +
                           "and M.SectionId = D.MappedSectionId";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    return ds;
                }
                else
                {
                    DataSet ds = new DataSet();
                    return ds;
                }
            }


            public DataSet GetLocalITPSectionQuestions(string sId, int iSectionId)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[6];

                colNames[0] = "AutoId";
                colNames[1] = "Question";
                colNames[2] = "Yes";
                colNames[3] = "No";
                colNames[4] = "NA";
                colNames[5] = "Comments";

                if (DB.TableExists(sITPSectionQuestionsTableName))
                {
                    sSQL = "select distinct AutoId, Question, Yes, No, NA, Comments  " +
                           "from " + sITPSectionQuestionsTableName + " " +
                           "where id = '" + sId + "' " +
                           "and SectionId = " + iSectionId;
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    return ds;
                }
                else
                {
                    DataSet ds = new DataSet();
                    return ds;
                }
            }

            public bool SetLocalITPSectionQuestion(int iAutoId, string sId, int iSectionId, int iAnswer, string sComments)
            {
                string sSQL;
                int iYes = 0;
                int iNo = 0;
                int iNA = 0;
                LocalDB DB = new LocalDB();
                string sRtnMsg = "";
                string sCurrentDateAndTime = "";

                sCurrentDateAndTime = DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss");

                switch (iAnswer)
                {
                    case 0:
                        iYes = 1;
                        break;
                    case 1:
                        iNo = 1;
                        break;
                    case 2:
                        iNA = 1;
                        break;
                }
                //Now the question should always exist so it should only be an update

                sSQL = "Update ITPQuestionnaireMst Set Yes=" + iYes + ",No=" + iNo + ",NA=" + iNA + ",Comments='" + sComments.Replace("'", "''") + "', Audit_DateStamp = '" + sCurrentDateAndTime + "' " +
                "where  ID='" + sId + "' and SectionID=" + iSectionId + " and AutoId = " + iAutoId;

                return DB.ExecuteSQL(sSQL, ref sRtnMsg);
            }

            public DataSet GetAllLocalITPSectionQuestions(string sId)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[9];

                colNames[0] = "AutoId";
                colNames[1] = "Id";
                colNames[2] = "SectionId";
                colNames[3] = "Question";
                colNames[4] = "Yes";
                colNames[5] = "No";
                colNames[6] = "NA";
                colNames[7] = "Comments";
                colNames[8] = "Audit_DateStamp";

                if (DB.TableExists(sITPSectionQuestionsTableName))
                {
                    sSQL = "select AutoId, Id, SectionId,  Question, Yes, No, NA, Comments, Audit_DateStamp " +
                           "from " + sITPSectionQuestionsTableName + " " +
                           "where id = '" + sId + "'";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    return ds;
                }
                else
                {
                    DataSet ds = new DataSet();
                    return ds;
                }
            }

            public bool ITPProjectSectionDeleteAllSection10Items(string sId, ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();

                if (DB.TableExists(sITPSection10TableName))
                {

                    sSQL = "delete from " + sITPSection10TableName + " where ID = '" + sId + "'";
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }

            }

            public bool IsProjectSection10ItemsExist(string sId)
            {
                string sSQL = "Select * from " + sITPSection10TableName + " where ID = '" + sId + "'";
                string sRtnMsg = "";
                int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
                if (iRecords > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }

            }
            public bool CheckSection10Table()
            {
                if (!DB.TableExists(sITPSection10TableName))
                {
                    if (!DB.CreateTable(sITPSection10TableName, colSection10HeaderNames, colSection10HeaderTypes))
                    {
                        return false;
                    }
                }

                return true;
            }

            public bool ITPSection10AddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sITPSection10TableName, colSection10ItemsNames, colSection10ItemsBaseTypes, sItemValues);
                return true;
            }

            public DataSet GetLocalITPSection10PwrIds(string sId)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];

                colNames[0] = "PwrId";

                if (DB.TableExists(sITPSectionQuestionsTableName))
                {
                    sSQL = "select distinct PwrId " +
                           "from " + sITPSection10TableName + " " +
                           "where id = '" + sId + "' order by PwrId "; 
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    return ds;
                }
                else
                {
                    DataSet ds = new DataSet();
                    return ds;
                }
            }

            public DataSet GetLocalITPSection10PwrIdStringDetails(string sId, string sPwrId)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[23];

                colNames[0] = "AutoId";
                colNames[1] = "ID";
                colNames[2] = "PwrId";
                colNames[3] = "BankNo";
                colNames[4] = "Floor";
                colNames[5] = "Suite";
                colNames[6] = "Rack";
                colNames[7] = "SubRack";
                colNames[8] = "Make";
                colNames[9] = "Model";
                colNames[10] = "SerialBatch";
                colNames[11] = "DOM";
                colNames[12] = "FuseOrCB";
                colNames[13] = "RatingAmps";
                colNames[14] = "LinkTest";
                colNames[15] = "BatteryTest";
                colNames[16] = "Audit_UserId";
                colNames[17] = "Audit_DateStamp";
                colNames[18] = "tblMaximoTransfer_Eqnum";
                colNames[19] = "tblMaximoPSA_ID";
                colNames[20] = "Equipment_Condition";
                colNames[21] = "SPN";
                colNames[22] = "Duplicate";

                if (DB.TableExists(sITPSectionQuestionsTableName))
                {
                    sSQL = "select * " +
                           "from " + sITPSection10TableName + " " +
                           "where id = '" + sId + "' " +
                           "and PwrId = '" + sPwrId + "' order by BankNo ";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    return ds;
                }
                else
                {
                    DataSet ds = new DataSet();
                    return ds;
                }
            }
        }
    }

    public class LocalDB
    {
        public SqliteConnection Connection()
        {
            string dbPath = Path.Combine(
                            System.Environment.GetFolderPath(System.Environment.SpecialFolder.MyDocuments),
                            "ITPDB2.db3");
            bool exists = File.Exists(dbPath);
            if (!exists)
            {
                DirectoryInfo dirInfo = new DirectoryInfo(dbPath);
                //                SqliteConnection.CreateFile(dbPath);
            }
            SqliteConnection connection = new SqliteConnection("Data Source=" + dbPath + ";Version=3;");
            connection.Open();

            return connection;
        }

        public void CloseConnection(SqliteConnection conn)
        {
            conn.Close();
        }

        public bool CreateTable(string sTableName, string[] sColumns, string[] sTypes)
        {
            SqliteConnection conn = Connection();
            string sCommand = "";
            string sCols = "";
            int i = 0;

            try
            {
                if (!TableExists(sTableName))
                {
                    sCommand += "CREATE TABLE [" + sTableName + "] (";

                    for (i = 0; i < sTypes.Length; i++)
                    {
                        sCols += "[" + sColumns[i] + "] " + sTypes[i] + ",";
                    }

                    sCols = sCols.Substring(0, sCols.Length - 1) + ");";
                    sCommand += sCols;

                    using (var c = conn.CreateCommand())
                    {
                        c.CommandText = sCommand;
                        c.ExecuteNonQuery();

                    }
                }
                CloseConnection(conn);
                return true;
            }
            catch (Exception ex)
            {
                CloseConnection(conn);
                return false;
            }
        }

        //Note that each of the arrays should be the same length
        public bool AddRecord(string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues)
        {
            try
            {
                string sSQL = "INSERT INTO " + sTableName + " (";
                string sValues = " VALUES (";
                DateClass Dte = new DateClass();
                string sDate = "";
                bool bSQL;
                string sRtnMsg = "";

                for (int i = 0; i < sColNames.Length; i++)
                {
                    if (sColTypes[i].ToLower() != "autoincrement")
                    {
                        sSQL += "[" + sColNames[i] + "],";
                    }
                    //                    string sColType = sColTypes[i].Substring(sColTypes[i].IndexOf("[") + 1, sColTypes[i].IndexOf("]") - sColTypes[i].IndexOf("[") -1);
                    switch (sColTypes[i].ToLower())
                    {
                        case "autoincrement":
                            break;
                        case "string":
                        case "varchar":
                        case "nvarchar":
                            sValues += "'" + objColValues[i].ToString().Replace("'", "''") + "'";
                            break;
                        case "date":
                            sDate = Dte.Get_Date_String(Convert.ToDateTime(objColValues[i]), "yyyymmdd");
                            sValues += "'" + sDate + "'";
                            break;
                        case "datetime":
                            sDate = Dte.Get_Date_And_Time_String(Convert.ToDateTime(objColValues[i]), "yyyymmdd hh:mm:ss");
                            sValues += "'" + sDate + "'";
                            break;
                        case "int":
                        case "bit":
                        case "float":
                        case "decimal":
                            sValues += objColValues[i].ToString();
                            break;
                        default: //String
                            sValues += "'" + objColValues[i].ToString().Replace("'", "''") + "'";
                            break;
                    }
                    if (sColTypes[i].ToLower() != "autoincrement")
                    {
                        sValues += ",";
                    }
                }

                sSQL = sSQL.Substring(0, sSQL.Length - 1) + ") ";
                sValues = sValues.Substring(0, sValues.Length - 1) + ")";
                sSQL = sSQL + sValues;
                bSQL = ExecuteSQL(sSQL, ref sRtnMsg);
                return bSQL;
            }
            catch (Exception ex)
            {
                string sRtnMsg = ex.Message.ToString();
                return false;
            }
        }

        public bool ExecuteSQL(string sSQL, ref string sRtnMsg)
        {
            SqliteConnection conn = Connection();
            SqliteCommand c = new SqliteCommand();
            int iRecords = -1;
            try
            {
                c = conn.CreateCommand();
                c.CommandText = sSQL;
                iRecords = c.ExecuteNonQuery();
                CloseConnection(conn);
                return true;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
                CloseConnection(conn);
                return false;
            }

        }

        public int GetSQLRecordCount(string sSQL, ref string sRtnMsg)
        {
            SqliteConnection conn = Connection();
            SqliteCommand c = new SqliteCommand();
            int iRecords = 0;
            try
            {
                c = conn.CreateCommand();
                c.CommandText = sSQL;
                SqliteDataReader r = c.ExecuteReader();
                if (r.HasRows)
                {
                    while (r.Read())
                    {
                        iRecords++;
                    }
                }
                r.Dispose();
                CloseConnection(conn);
                return iRecords;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
                CloseConnection(conn);
                return -1;
            }

        }

        public double GetSQLValue(string sSQL, int iRow, ref string sRtnMsg)
        {
            SqliteConnection conn = Connection();
            SqliteCommand c = new SqliteCommand();
            double dRtnValue = 0.0;
            int i = 0;
            try
            {
                c = conn.CreateCommand();
                c.CommandText = sSQL;
                SqliteDataReader r = c.ExecuteReader();
                while (r.Read())
                {

                    if (i == iRow)
                    {
                        dRtnValue = r.GetDouble(0);
                        break;
                    }

                    i++;
                }

                r.Dispose();
                CloseConnection(conn);
                sRtnMsg = dRtnValue.ToString();
                return dRtnValue;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
                CloseConnection(conn);
                return 0.0;
            }

        }

        public bool TableExists(string sTableName)
        {
            string sSQL = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + sTableName + "'";
            DataSet ds = new DataSet();
            string[] sColNames = new string[1];
            sColNames[0] = "name";
            string sRtnMsg = "";


            ds = ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);

            if (ds.Tables[0].Rows.Count > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public DataSet ReadSQLDataSet(string sSQL, string[] sColNames, ref string sRtnMsg)
        {
            SqliteConnection conn = Connection();
            SqliteCommand c = new SqliteCommand();
            try
            {
                DataSet ds = new DataSet();
                c = conn.CreateCommand();
                c.CommandText = sSQL;
                SqliteDataReader r = c.ExecuteReader();
                ds = ConvertDataReaderToDataSet(r, sColNames);
                CloseConnection(conn);
                return ds;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
                CloseConnection(conn);
                return null;
            }

        }

        public DataSet ConvertDataReaderToDataSet(SqliteDataReader reader, string[] sColNames)
        {
            DataSet dataSet = new DataSet();
            try
            {
                //            DataTable schemaTable = reader.GetSchemaTable();

                DataTable dataTable = new DataTable();

                for (int i = 0; i < sColNames.Length; i++)
                {


                    string columnName = sColNames[i];
                    DataColumn column = new DataColumn(columnName);
                    dataTable.Columns.Add(column);

                }

                //for (int i = 0; i <= schemaTable.Rows.Count - 1; i++)
                //{

                //    DataRow dataRow = schemaTable.Rows[i];

                //    string columnName = dataRow["ColumnName"].ToString();DataColumn column = new DataColumn(columnName, dataRow["DataType"].GetType());
                //    dataTable.Columns.Add(column);

                //}

                dataSet.Tables.Add(dataTable);
                int j = 0;
                while (reader.Read())
                {

                    DataRow dataRow = dataTable.NewRow();

                    for (int i = 0; i < sColNames.Length; i++)
                    {
                        dataRow[i] = reader.GetValue(i);
                    }

                    j++;
                    dataTable.Rows.Add(dataRow);

                }

                reader.Dispose();
            }
            catch (Exception ex)
            {
                string sMsg = ex.Message.ToString();
            }
            return dataSet;
        }
    }


    public class clsLocalUtils
    {
        public int iEnvironment = 1; //0 = Development, 1 = Test, 2 = Production

        public string GetEnvironment_wbsURL(string sWBSType)
        {
            string sURL;
            switch (sWBSType)
            {
                case "wbsITP_External":
                    switch (iEnvironment)
                    {
                        case 0:
                            sURL = "http://silcar-dt11.silcar.com.au:8088/wbsITP_External.asmx";
                            break;
                        case 1:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        case 2:
                            sURL = "https://scms.silcar.com.au/wbsITP_External.asmx";
                            break;
                        default:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                    }
                    break;
                default:
                    switch (iEnvironment)
                    {
                        case 0:
                            sURL = "http://silcar-dt11.silcar.com.au:8088/wbsITP_External.asmx";
                            break;
                        case 1:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        case 2:
                            sURL = "http://scms.silcar.com.au/wbsITP_External.asmx";
                            break;
                        default:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                    }
                    break;
            }

            return sURL;
        }

        public string Get_Tag_Name_Prefix(string sTag)
        {
            string sPrefix;
            int iUnder = 0;

            iUnder = sTag.IndexOf("_");

            if(iUnder <= 0)
            {
                sPrefix = sTag;
            }
            else
            {
                sPrefix = sTag.Substring(0, iUnder);
            }

            return sPrefix;
        }

        public int Get_Row_From_Tag_Name(string sTag)
        {
            string sPrefix;
            int iUnder = 0;
            int iReturn = -1;

            iUnder = sTag.LastIndexOf("_");

            if (iUnder > 0)
            {
                sPrefix = sTag.Substring(iUnder + 1);
                if (int.TryParse(sPrefix,out iReturn))
                {
                    return iReturn;
                }
                else
                {
                    return -1;
                }
            }

            return iReturn;
        }

    }

    public class clsITPFramework
    {
        public object[] GetITPsForDownload(string sSessionId, string sUser)
        {
            try
            {
                clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITPs = ws.GetSubcontractorITPsForDownload(sSessionId, sUser);
                return objListITPs;
            }
            catch (Exception ex)
            {
                object[] objListITPs = new object[2];
                objListITPs[0] = "Failure";
                objListITPs[1] = ex.Message.ToString();
                return objListITPs;
            }
        }

        public object[] DownloadITPInfo(string sSessionId, string sUser, string sId)
        {
            try
            {
                clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITPs = ws.GetITPDownloadInfo(sSessionId, sUser, sId);
                return objListITPs;
            }
            catch (Exception ex)
            {
                object[] objListITPs = new object[2];
                objListITPs[0] = "Failure";
                objListITPs[1] = ex.Message.ToString();
                return objListITPs;
            }
        }

        public object[] DownloadProjectITPQuestions(string sSessionId, string sUser, string sId)
        {
            try
            {
                clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITPs = ws.GetITPProjectQuestionnaireInfo(sSessionId, sUser, sId);
                return objListITPs;
            }
            catch (Exception ex)
            {
                object[] objListITPs = new object[2];
                objListITPs[0] = "Failure";
                objListITPs[1] = ex.Message.ToString();
                return objListITPs;
            }
        }


        public bool MarkITPDownloaded(string sSessionId, string sUser, string sId, ref string sRtnMsg)
        {
            try
            {
                clsTabletDB.ITPHeaderTable ITP = new clsTabletDB.ITPHeaderTable();
                clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITP = ws.SetITPStatus(sSessionId, sUser, sId, 1);
                if (objListITP[0].ToString() == "Success")
                {
                    //Now also mark them the same locally
                    if (!ITP.MarkLocalITPDownloaded(sId, 0, ref sRtnMsg))
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    sRtnMsg = objListITP[1].ToString();
                    return false;
                }
            }
            catch (Exception ex)
            {
                sRtnMsg = ex.Message.ToString();
                return false;
            }

        }

        public DataSet GetITPsDownloaded(ref string sRtnMsg)
        {
            clsTabletDB.ITPHeaderTable ITP = new clsTabletDB.ITPHeaderTable();

            try
            {

                DataSet objListITPs = ITP.GetLocalITPProjects();
                return objListITPs;
            }
            catch (Exception ex)
            {
                DataSet ds = new DataSet();
                sRtnMsg = ex.Message.ToString();
                return ds;
            }
        }

        public DataSet GetITPSections(string sId, ref string sRtnMsg)
        {
            clsTabletDB.ITPDocumentSection ITP = new clsTabletDB.ITPDocumentSection();

            try
            {

                DataSet objListITPs = ITP.GetLocalITPSections(sId);
                return objListITPs;
            }
            catch (Exception ex)
            {
                DataSet ds = new DataSet();
                sRtnMsg = ex.Message.ToString();
                return ds;
            }
        }

        public object[] DownloadProjectITPSection10(string sSessionId, string sUser, string sId)
        {
            try
            {
                clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITPs = ws.GetITPProjectSection10Info(sSessionId, sUser, sId);
                return objListITPs;
            }
            catch (Exception ex)
            {
                object[] objListITPs = new object[2];
                objListITPs[0] = "Failure";
                objListITPs[1] = ex.Message.ToString();
                return objListITPs;
            }
        }

        public bool UploadITPInfo(string sSessionId, string sUser, string sId, ref string sRtnMsg)
        {
            try
            {
                clsTabletDB.ITPDocumentSection ITP = new clsTabletDB.ITPDocumentSection();
                clsTabletDB.ITPHeaderTable ITPHdr = new clsTabletDB.ITPHeaderTable();
                DataSet ds = ITP.GetAllLocalITPSectionQuestions(sId);
                string sSendString = "ITPUploadQuestionnaireInfo~";
                int i;
                int iRows = ds.Tables[0].Rows.Count;
                int iCol;
                int iAutoId;
                string sProjId;
                int iSectionId;
                string sQuestion;
                int iYes;
                int iNo;
                int iNA;
                bool bYes;
                bool bNo;
                bool bNA;
                string sComments;
                string sAudit_DateStamp;

                for (i = 0; i < iRows; i++)
                {
                    iCol = ds.Tables[0].Columns["AutoId"].Ordinal;
                    iAutoId = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[iCol]);

                    iCol = ds.Tables[0].Columns["Id"].Ordinal;
                    sProjId = ds.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds.Tables[0].Columns["SectionId"].Ordinal;
                    iSectionId = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[iCol]);

                    iCol = ds.Tables[0].Columns["Question"].Ordinal;
                    sQuestion = ds.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds.Tables[0].Columns["Yes"].Ordinal;
                    bYes = Convert.ToBoolean(ds.Tables[0].Rows[i].ItemArray[iCol]);
                    if (bYes)
                    {
                        iYes = 1;
                    }
                    else
                    {
                        iYes = 0;
                    }

                    iCol = ds.Tables[0].Columns["No"].Ordinal;
                    bNo = Convert.ToBoolean(ds.Tables[0].Rows[i].ItemArray[iCol]);
                    if (bNo)
                    {
                        iNo = 1;
                    }
                    else
                    {
                        iNo = 0;
                    }

                    iCol = ds.Tables[0].Columns["NA"].Ordinal;
                    bNA = Convert.ToBoolean(ds.Tables[0].Rows[i].ItemArray[iCol]);
                    if (bNA)
                    {
                        iNA = 1;
                    }
                    else
                    {
                        iNA = 0;
                    }

                    iCol = ds.Tables[0].Columns["Comments"].Ordinal;
                    sComments = ds.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds.Tables[0].Columns["Audit_DateStamp"].Ordinal;
                    sAudit_DateStamp = ds.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    sSendString += iAutoId + "^" + sProjId + "^" + iSectionId + "^" + sQuestion + "^" + iYes + "^" + iNo + "^" + iNA + "^" + sComments + "^" + sAudit_DateStamp + "||";
                }

                if (sSendString.Length > 2)
                {
                    sSendString = sSendString.Substring(0, sSendString.Length - 2);
                }

                clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITP = ws.SetITPUploadInfo(sSessionId, sUser, sSendString);
                if (objListITP[0].ToString() == "Success")
                {
                    object[] objListITPStatus = ws.SetITPStatus(sSessionId, sUser, sId, 0);
                    if (objListITPStatus[0].ToString() == "Success")
                    {
                        //Now also mark them the same locally
                        if (!ITPHdr.MarkLocalITPDownloaded(sId, 1, ref sRtnMsg))
                        {
                            return false;
                        }
                        else
                        {
                            return true;
                        }
                    }
                    else
                    {
                        sRtnMsg = objListITPStatus[1].ToString();
                        return false;
                    }
                }
                else
                {
                    sRtnMsg = objListITP[1].ToString();
                    return false;
                }
            }
            catch (Exception ex)
            {
                sRtnMsg = ex.Message.ToString();
                return false;
            }
        }
    }

    public class DateClass
    {
        public string ConvertDate2Save(DateTime dDate)
        {
            string sDateTime = dDate.Year.ToString() + "-" + dDate.Month.ToString() + "-" + dDate.Day.ToString() + " " + dDate.ToLongTimeString();
            return sDateTime;
        }

        public string Get_Date_String(DateTime DTDate, string sFormat = "")
        {
            int iDay;
            int iMonth;
            int iYear;
            string sDay;
            string sMonth;
            string sYear;
            string sReturn;

            iDay = DTDate.Day;
            iMonth = DTDate.Month;
            iYear = DTDate.Year;

            sReturn = "";
            if (sFormat == "")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sDay + sMonth + sYear;
            }

            if (sFormat.ToUpper() == "DD-MM-YYYY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sDay + "-" + sMonth + "-" + sYear;
            }

            if (sFormat.ToUpper() == "DD/MM/YYYY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sDay + "/" + sMonth + "/" + sYear;
            }

            if (sFormat.ToUpper() == "DD-MM-YY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString().Substring(3);
                sReturn = sDay + "-" + sMonth + "-" + sYear;
            }

            if (sFormat.ToUpper() == "DD/MM/YY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString().Substring(3);
                sReturn = sDay + "/" + sMonth + "/" + sYear;
            }

            if (sFormat.ToUpper() == "MM-DD-YYYY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sMonth + "-" + sDay + "-" + sYear;
            }

            if (sFormat.ToUpper() == "MM/DD/YYYY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sMonth + "/" + sDay + "/" + sYear;
            }

            if (sFormat.ToUpper() == "MM-DD-YY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString().Substring(3);
                sReturn = sMonth + "-" + sDay + "-" + sYear;
            }

            if (sFormat.ToUpper() == "MM/DD/YY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString().Substring(3);
                sReturn = sMonth + "/" + sDay + "/" + sYear;
            }

            if (sFormat.ToUpper() == "YYYY-MM-DD")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sYear + "-" + sMonth + "-" + sDay;
            }

            if (sFormat.ToUpper() == "YYYYMMDD")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sYear + sMonth + sDay;
            }

            if (sFormat.ToUpper() == "YYMMDD")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString().Substring(3);
                sReturn = sYear + sMonth + sDay;
            }

            if (sFormat.ToUpper() == "DDMMYYYY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString("D4");
                sReturn = sDay + sMonth + sYear;
            }

            return sReturn;
        }

        public string Get_Time_String(DateTime DTDate, string sFormat = "")
        {
            int iHour;
            int iMinute;
            int iSecond;
            string sHour;
            string sMinute;
            string sSecond;
            string sReturn;

            iHour = DTDate.Hour;
            iMinute = DTDate.Minute;
            iSecond = DTDate.Second;

            sReturn = "";
            if (sFormat == "")
            {
                sHour = iHour.ToString("D2");
                sMinute = iMinute.ToString("D2");
                sSecond = iSecond.ToString("D2");
                sReturn = sHour + sMinute + sSecond;
            }

            if (sFormat.ToUpper() == "HH:MM:SS")
            {
                sHour = iHour.ToString("D2");
                sMinute = iMinute.ToString("D2");
                sSecond = iSecond.ToString("D2");
                sReturn = sHour + ":" + sMinute + ":" + sSecond;
            }

            if (sFormat.ToUpper() == "HHMMSS")
            {
                sHour = iHour.ToString("D2");
                sMinute = iMinute.ToString("D2");
                sSecond = iSecond.ToString("D2");
                sReturn = sHour + sMinute + sSecond;
            }

            return sReturn;

        }

        public string Get_Date_And_Time_String(DateTime dtDate, string sFormat = "")
        {
            string sDateFormat;
            string sTimeFormat;
            string[] sFormats;
            string[] delimiters = new string[] { " " };

            sFormats = sFormat.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);
            sDateFormat = sFormats[0];
            sTimeFormat = sFormats[1];

            return Get_Date_String(dtDate, sDateFormat) + " " + Get_Time_String(dtDate, sTimeFormat);
        }

    }

}

