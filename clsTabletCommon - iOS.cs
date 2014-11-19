using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using Android.Database.Sqlite;
using Mono.Data.Sqlite;
using System.Data;
using System.IO;
using clsTabletCommon.ITPExternal;

namespace nspTabletCommon
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
					dLocalVersion = Convert.ToDouble(ds.Tables[0].Rows[0].ItemArray[iColNo]);
					iColNo = ds.Tables[0].Columns["VersionDate"].Ordinal;
					dtVersiondate = Convert.ToDateTime(ds.Tables[0].Rows[0].ItemArray[iColNo].ToString());
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
				return bRtn;
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

			public double GetSystemVolts(string sId, string sPwrId)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];

				colNames[0] = "SystemVolts";

				if (DB.TableExists(sDocHeaderTableName))
				{
					sSQL = "select SystemVolts from " + sDocHeaderTableName + 
						" where ID = '" + sId + "' and PwrId = '" + sPwrId + "' ";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					return Convert.ToDouble(ds.Tables[0].Rows[0].ItemArray[0]);
				}
				else
				{
					return -1.0;
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
				return bRtn;
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
				return bRtn;
			}

            public bool FillITPTypeMainTable(string sSessionId, string sUser, ref string sRtnMsg)
            {
                try
                {
                    ITPStaticTable Static = new ITPStaticTable();
                    double dNewVersionNumber = 0.0;
					DateTime dtLastVersionDate;

                    //Only do all of this if the version has changed. So get the local versoin umber and compare to that on the DB. If different do all of this. - WRITE LATER as a general function
                    bool bNewVersion = Static.IsNewVersionOfTable(sSessionId, sUser, sITPTypeTableName, ref dNewVersionNumber, ref dtLastVersionDate);
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

		public class ITPInventory
		{
            string[] colHeaderNames = { "Itemnum", "Type", "Description", "Supplier", "SPN", "ClassificationId", "ClassDescription" };
            string[] colHeaderTypes = {"[nvarchar](30) NULL", "[nvarchar](202) NULL", "[nvarchar](180) NULL", "[nvarchar](250) NULL", "[nvarchar](50) NULL", "[nvarchar](192) NULL", "[nvarchar](100) NULL" };
			string[] colHeaderBaseTypes = { "string", "string", "string", "string", "string", "string" ,"string"};
			LocalDB DB = new LocalDB();
			public string sITPInventoryTableName = "ITPInventory";
			
			public bool CheckFullITPInventoryTable()
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
			public bool TableITPInventoryDeleteAllRecords(ref string sRtnMsg)
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
			
			public bool TableITPInventoryAddRecord(string[] sItemValues)
			{
				bool bRtn;
				
				bRtn = DB.AddRecord(sITPInventoryTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
				return bRtn;
			}
			
			public string[] GetBatteryMakes()
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Supplier";

				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Supplier " +
							"from " + sITPInventoryTableName + " " +
							"where Type Like 'Battery String%' order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];

					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No battery makes in the database";
					return sReturn;
				}
			}

            public string[] GetBatteryModels(string sSupplier)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];
                
                colNames[0] = "Description";
                
                if (DB.TableExists(sITPInventoryTableName))
                {
                    sSQL = "select DISTINCT Description " +
                        	"from " + sITPInventoryTableName + " " +
                            "where Type Like 'Battery String%' " +
                            "and Supplier = '" + sSupplier + "' order by 1";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    int iRows = ds.Tables[0].Rows.Count;
                    string[] sReturn = new string[iRows];
                    
                    for(int i =0; i< iRows;i++)
                    {
                        sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
                    }
                    return sReturn;
                }
                else
                {
                    string[] sReturn = new string[1];
                    sReturn[0] = "No battery models in the database for supplier " + sSupplier;
                    return sReturn;
                }
            }

			public string[] GetRackMakes()
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Supplier";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Supplier " +
							"from " + sITPInventoryTableName + " " +
							"where Type Like '%Rack%' " +
							" and Type not like '%SubRack%' " +
							" and Type not like '%Sub Rack%' " +
//							" and Type not like '%Battery%' " +
							"order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No rack makes in the database";
					return sReturn;
				}
			}

			public string[] GetRackModels(string sSupplier)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Description";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Description " +
							"from " + sITPInventoryTableName + " " +
							"where Type Like '%Rack%' " +
							" and Type not like '%SubRack%' " +
							" and Type not like '%Sub Rack%' " +
//							" and Type not like '%Battery%' " +
							"and Supplier = '" + sSupplier + 
							"' order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No rack models in the database for supplier " + sSupplier;
					return sReturn;
				}
			}

			public string[] GetSubRackMakes()
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Supplier";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Supplier " +
						"from " + sITPInventoryTableName + " " +
							"where Type like '%SubRack%' " +
							" or Type like '%Sub Rack%' " +
							"order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No sub rack makes in the database";
					return sReturn;
				}
			}
			
			public string[] GetSubRackModels(string sSupplier)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Description";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Description " +
						"from " + sITPInventoryTableName + " " +
							"where (Type like '%SubRack%' " +
							" or Type like '%Sub Rack%') " +
							"and Supplier = '" + sSupplier + 
							"' order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No sub rack models in the database for supplier " + sSupplier;
					return sReturn;
				}
			}

			public string[] GetPositionMakes()
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Supplier";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Supplier " +
						"from " + sITPInventoryTableName + " " +
							"where Type not like '%Rack%' " +
							" and Type not like '%Battery%' " +
							" and Type not like '%Solar Panel%' " +
							" and Type not like '%Tank%' " +
							" and Type not like '%Air Conditioner%' " +
							" and Type not like '%Engine%' " +
							" and Type not like '%Power Generating Plant%' " +
							" and Type not like '%Shelter%' " +
							" and Type not like '%Cabinet%' " +
							"order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No position makes in the database";
					return sReturn;
				}
			}
			
			public string[] GetPositionModels(string sSupplier)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Description";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Description " +
						"from " + sITPInventoryTableName + " " +
							"where Type not like '%Rack%' " +
							" and Type not like '%Battery%' " +
							" and Type not like '%Solar Panel%' " +
							" and Type not like '%Tank%' " +
							" and Type not like '%Air Conditioner%' " +
							" and Type not like '%Engine%' " +
							" and Type not like '%Power Generating Plant%' " +
							" and Type not like '%Shelter%' " +
							" and Type not like '%Cabinet%' " +
							"and Supplier = '" + sSupplier + 
							"' order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No position models in the database for supplier " + sSupplier;
					return sReturn;
				}
			}

			public string[] GetSolarStringMakes()
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Supplier";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Supplier " +
						"from " + sITPInventoryTableName + " " +
							"where Type Like 'Solar Panel%' order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No solar panel makes in the database";
					return sReturn;
				}
			}
			
			public string[] GetSolarStringModels(string sSupplier)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "Description";
				
				if (DB.TableExists(sITPInventoryTableName))
				{
					sSQL = "select DISTINCT Description " +
						"from " + sITPInventoryTableName + " " +
							"where Type Like '%Solar Panel%' " +
							"and Supplier = '" + sSupplier + "' order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];
					
					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No solar panel models in the database for supplier " + sSupplier;
					return sReturn;
				}
			}
			
			public string GetSPNFromModelAndMake(string sMake, string sModel)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];
                
                colNames[0] = "SPN";
                
                if (DB.TableExists(sITPInventoryTableName))
                {
                    sSQL = "select DISTINCT SPN " +
                           "from " + sITPInventoryTableName + " " +
                           "where Supplier = '" + sMake + "' " +
                           "and Description = '" + sModel + "' ";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    string sReturn = ds.Tables[0].Rows[0].ItemArray[0].ToString();
                    return sReturn;
                }
                else
                {
                    string sReturn = "";
                    sReturn = "No models in the database fro supplier " + sMake;
                    return sReturn;
                }
            }

		}



        public class ITPBatteryFuseTypes
        {
            string[] colHeaderNames = { "AutoId", "FuseType"};
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](250) NULL"};
            string[] colHeaderBaseTypes = { "autoincrement", "string"};
            LocalDB DB = new LocalDB();
            public string sITPBatteryFuseTypeTableName = "ITPBatteryFuseTypes";

            public bool CheckFullITPBatteryFuseTypeTable()
            {
                
                if (!DB.TableExists(sITPBatteryFuseTypeTableName))
                {
                    
                    return DB.CreateTable(sITPBatteryFuseTypeTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }
                
                
            }
            public bool TableITPBatteryFuseTypeDeleteAllRecords(ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();
                
                if (DB.TableExists(sITPBatteryFuseTypeTableName))
                {
                    
                    sSQL = "delete from " + sITPBatteryFuseTypeTableName;
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }
                
            }
            
            public bool TableITPBatteryFuseTypeAddRecord(string[] sItemValues)
            {
                bool bRtn;
                
                bRtn = DB.AddRecord(sITPBatteryFuseTypeTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
                return bRtn;
            }
            
            public string[] GetBatteryFuseTypes()
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];
                
                colNames[0] = "FuseType";
                
                if (DB.TableExists(sITPBatteryFuseTypeTableName))
                {
                    sSQL = "select DISTINCT FuseType " +
                        "from " + sITPBatteryFuseTypeTableName ;
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    int iRows = ds.Tables[0].Rows.Count;
                    string[] sReturn = new string[iRows];
                    
                    for(int i =0; i< iRows;i++)
                    {
                        sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
                    }
                    return sReturn;
                }
                else
                {
                    string[] sReturn = new string[1];
                    sReturn[0] = "No battery fuse types in the database";
                    return sReturn;
                }
            }
            
        }

		public class ITPBatteryCellInfo
		{
			string[] colHeaderNames = { "AutoId", "SPN", "BatteryType", "AmpereHours", "CellsPerBlock", "VoltsPerCell"};
			string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](250) NULL", "[nvarchar](250) NULL", "[int] NULL", "[int] NULL", "[float] NULL"};
			string[] colHeaderBaseTypes = { "autoincrement", "string","string", "int", "int","float"};
			LocalDB DB = new LocalDB();
			public string sITPBatteryCellInfoTableName = "ITPBatteryCellInfo";

			public bool CheckFullITPBatteryCellInfoTable()
			{

				if (!DB.TableExists(sITPBatteryCellInfoTableName))
				{

					return DB.CreateTable(sITPBatteryCellInfoTableName, colHeaderNames, colHeaderTypes);
				}
				else
				{
					return true;
				}


			}
			public bool TableITPBatteryCellInfoDeleteAllRecords(ref string sRtnMsg)
			{
				string sSQL;
				LocalDB DB = new LocalDB();

				if (DB.TableExists(sITPBatteryCellInfoTableName))
				{

					sSQL = "delete from " + sITPBatteryCellInfoTableName;
					return DB.ExecuteSQL(sSQL, ref sRtnMsg);
				}
				else
				{
					return true;
				}

			}

			public bool TableITPBatteryCellInfoAddRecord(string[] sItemValues)
			{
				bool bRtn;

				bRtn = DB.AddRecord(sITPBatteryCellInfoTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
				return bRtn;
			}

			public int GetBatteryBlockNoOfCells(string sSPN)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				int iReturn = -1;

				colNames[0] = "CellsPerBlock";

				if (DB.TableExists(sITPBatteryCellInfoTableName))
				{
					sSQL = "select CellsPerBlock " +
						"from " + sITPBatteryCellInfoTableName + " where SPN = '" + sSPN + "'";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					iReturn = Convert.ToInt32(ds.Tables[0].Rows[0].ItemArray[0]);
					return iReturn;
				}
				else
				{
					return iReturn;
				}
			}

			public double GetBatteryBlockVoltage(string sSPN)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				double dReturn = 0.0;

				colNames[0] = "BlockVoltage";

				if (DB.TableExists(sITPBatteryCellInfoTableName))
				{
					sSQL = "select (VoltsPerCell * CellsPerBlock) as BlockVoltage " +
						"from " + sITPBatteryCellInfoTableName + " where SPN = '" + sSPN + "'";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					dReturn = Convert.ToDouble(ds.Tables[0].Rows [0].ItemArray[0]);
					return dReturn;
				}
				else
				{
					return dReturn;
				}
			}

			public string[] GetBatteryBlockVoltages()
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];

				colNames[0] = "BlockVoltage";

				if (DB.TableExists(sITPBatteryCellInfoTableName))
				{
					sSQL = "select DISTINCT (VoltsPerCell * CellsPerBlock) as BlockVoltage " +
							"from " + sITPBatteryCellInfoTableName + " " +
							"order by 1";
					DataSet ds = new DataSet();
					ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
					int iRows = ds.Tables[0].Rows.Count;
					string[] sReturn = new string[iRows];

					for(int i =0; i< iRows;i++)
					{
						sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
					}
					return sReturn;
				}
				else
				{
					string[] sReturn = new string[1];
					sReturn[0] = "No block voltages in the database";
					return sReturn;
				}
			}

}

		public class ITPValidHierarchy
        {
            string[] colHeaderNames = { "AutoId", "FieldValue", "FieldType" };
            string[] colHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](10) NULL", "[int] NULL"};
            string[] colHeaderBaseTypes = { "autoincrement", "string","int"};
            LocalDB DB = new LocalDB();
            public string sITPValidHierarchyTableName = "ITPValidHierarchy";
            
            public bool CheckFullITPValidHierarchyTable()
            {
                
                if (!DB.TableExists(sITPValidHierarchyTableName))
                {
                    
                    return DB.CreateTable(sITPValidHierarchyTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }
                
                
            }
            public bool TableITPValidHierarchyDeleteAllRecords(ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();
                
                if (DB.TableExists(sITPValidHierarchyTableName))
                {
                    
                    sSQL = "delete from " + sITPValidHierarchyTableName;
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }
                
            }
            
            public bool TableITPValidHierarchyAddRecord(string[] sItemValues, SqliteConnection conn)
            {
                bool bRtn;
                
                bRtn = DB.AddRecordOpenConnection(sITPValidHierarchyTableName, colHeaderNames, colHeaderBaseTypes, sItemValues, conn);
                return bRtn;
            }
            
            public string[] GetValidHierarchy(int iFieldType)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];
                
                colNames[0] = "FieldValue";
                
                if (DB.TableExists(sITPValidHierarchyTableName))
                {
                    sSQL = "select DISTINCT FieldValue " +
                           "from " + sITPValidHierarchyTableName + " " +
                           "where FieldType = " + iFieldType;
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    int iRows = ds.Tables[0].Rows.Count;
                    string[] sReturn = new string[iRows];
                    
                    for(int i =0; i< iRows;i++)
                    {
                        sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
                    }
                    return sReturn;
                }
                else
                {
                    string[] sReturn = new string[1];
                    string sFieldName = "";
                    switch(iFieldType)
                    {
                        case 1:
                            sFieldName = "Floor";
                            break;
                        case 2:
                            sFieldName = "Suite";
                            break;
                        case 3:
                            sFieldName = "Rack";
                            break;
                        case 4:
                            sFieldName = "Subrack";
                            break;
                        case 5:
                            sFieldName = "Position";
                            break;
                        case 6:
                            sFieldName = "String";
                            break;
                        case 7:
                            sFieldName = "Array String";
                            break;
                    }
                    sReturn[0] = "No valid values for fields of type " + sFieldName + " in the database";
                    return sReturn;
                }
            }
            
            public string[] GetValidHierarchySearch(int iFieldType, string sSearchText)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];
                
                colNames[0] = "FieldValue";
                
                if (DB.TableExists(sITPValidHierarchyTableName))
                {
                    sSQL = "select DISTINCT FieldValue " +
                        "from " + sITPValidHierarchyTableName + " " +
                            "where FieldType = " + iFieldType + " " +
                            " and FieldValue LIKE '%" + sSearchText + "%'";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    int iRows = ds.Tables[0].Rows.Count;
                    string[] sReturn = new string[iRows];
                    
                    for(int i =0; i< iRows;i++)
                    {
                        sReturn[i] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
                    }
                    return sReturn;
                }
                else
                {
                    string[] sReturn = new string[1];
                    string sFieldName = "";
                    switch(iFieldType)
                    {
                    case 1:
                        sFieldName = "Floor";
                        break;
                    case 2:
                        sFieldName = "Suite";
                        break;
                    case 3:
                        sFieldName = "Rack";
                        break;
                    case 4:
                        sFieldName = "Subrack";
                        break;
                    case 5:
                        sFieldName = "Position";
                        break;
                    case 6:
                        sFieldName = "String";
                        break;
                    case 7:
                        sFieldName = "Array String";
                        break;
                    }
                    sReturn[0] = "No valid values for fields of type " + sFieldName + " in the database";
                    return sReturn;
                }
            }

            public bool IsValidItem(string sValue, int iFieldType)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];
                
                colNames[0] = "FieldValue";
                
                if (DB.TableExists(sITPValidHierarchyTableName))
                {
                    sSQL = "select FieldValue " +
                        "from " + sITPValidHierarchyTableName + " " +
                            "where FieldType = " + iFieldType + " and FieldValue = '" + sValue + "'";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    int iRows = ds.Tables[0].Rows.Count;
                    if(iRows > 0)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
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
            string[] colSectionQuestionMstNames = { "ID", "SectionID", "Question", "Yes", "No", "NA", "Comments", "Audit_DateStamp" };
            string[] colSectionQuestionMstTypes = { "[nvarchar](50) NULL", "[int] NULL", "[nvarchar] (8000) NULL", "[bit] NULL", "[bit] NULL", "[bit] NULL", "[nvarchar] (8000) NULL","[nvarchar](50) NULL"  };
            string[] colSectionQuestionMstBaseTypes = { "string", "int", "string", "bit", "bit", "bit", "string", "string" };
            public string sITPSectionQuestionsTableName = "ITPQuestionnaireMst";

            //These arrays are with the autoId column for these times we need to define the complete table
			string[] colSection10HeaderNames = { "AutoID", "SQLAutoId", "ID", "PWRID", "BankNo", "Floor", "Suite", "Rack", "SubRack", "Position", "Make", "Model", "SerialBatch", "DOM", 
                                                 "FuseOrCB", "RatingAmps", "LinkTest", "BatteryTest", "Audit_UserId", "Audit_DateStamp", 
                                                 "tblMaximoTransfer_Eqnum", "tblMaximoPSA_ID", "Equipment_Condition", "SPN", "Duplicate", "Equipment_Type", "Status",  };
			string[] colSection10HeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[int] NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", 
                                                 "[nvarchar](50) NULL","[nvarchar](8000) NULL","[nvarchar](8000) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL",
                                                 "[nvarchar](50) NULL","[nvarchar](10) NULL","[int] NULL","[int] NULL", "[nvarchar] (50) NULL", "[nvarchar] (50) NULL",
                                                 "[nvarchar] (50) NULL", "[int] NULL", "[nvarchar] (50) NULL",  "[nvarchar] (50) NULL", "[int] NULL",  "[int] NULL", "[int] NULL"};
            string[] colSection10HeaderBaseTypes = { "autoincrement", "int", "string", "string", "string", "string", "string", "string","string", 
                                                     "string", "string", "string", "string", "string",
                                                     "string", "string", "int", "int", "string", "string",
                                                     "string", "int", "string", "string", "int", "int", "int"};
            //These arrays are without the autoId column because we do not bring this across and don't want it populated by the code
            string[] colSection10ItemsNames = { "SQLAutoId", "ID", "PWRID", "BankNo", "Floor", "Suite", "Rack", 
                                                "SubRack", "Position", "Make", "Model", "SerialBatch", "DOM", 
                                                "FuseOrCB", "RatingAmps", "LinkTest", "BatteryTest", "Audit_DateStamp", 
                                                "tblMaximoTransfer_Eqnum", "tblMaximoPSA_ID", "Equipment_Condition", "SPN", "Duplicate", "Equipment_Type", "Status" };
			string[] colSection10ItemsTypes = {  "[int] NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", 
                                                 "[nvarchar](50) NULL","[nvarchar](50) NULL","[nvarchar](8000) NULL","[nvarchar](8000) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL",
                                                 "[nvarchar](50) NULL","[nvarchar](10) NULL","[int] NULL","[int] NULL", "[nvarchar] (50) NULL",
                                                 "[nvarchar] (50) NULL", "[int] NULL", "[nvarchar] (50) NULL", "[nvarchar] (50) NULL", "[int] NULL", "[int] NULL", "[int] NULL"};
            string[] colSection10ItemsBaseTypes = {  "int", "string", "string", "string", "string", "string", "string", 
                                                     "string", "string", "string", "string", "string", "string",
                                                     "string", "string", "int", "int", "string",
                                                     "string", "int", "string", "string", "int", "int", "int"};
            
			//These arrays are without the autoId column and the SQLAutoId column because we do not populate this when updating items from the tablet. (We simply hold ity in the local DB to send back to the SQL DB)
			string[] colSection10ItemsNames_NoSQLId = { "ID", "PWRID", "BankNo", "Floor", "Suite", "Rack", 
														"SubRack", "Position", "Make", "Model", "SerialBatch", "DOM", 
														"FuseOrCB", "RatingAmps", "LinkTest", "BatteryTest", "Audit_DateStamp", 
														"tblMaximoTransfer_Eqnum", "tblMaximoPSA_ID", "Equipment_Condition", "SPN", "Duplicate", "Equipment_Type", "Status" };
			string[] colSection10ItemsTypes_NoSQLId = {  "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[nvarchar](50) NULL", 
														"[nvarchar](50) NULL","[nvarchar](50) NULL","[nvarchar](8000) NULL","[nvarchar](8000) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL",
														"[nvarchar](50) NULL","[nvarchar](10) NULL","[int] NULL","[int] NULL", "[nvarchar] (50) NULL",
														"[nvarchar] (50) NULL", "[int] NULL", "[nvarchar] (50) NULL", "[nvarchar] (50) NULL", "[int] NULL", "[int] NULL", "[int] NULL"};
			string[] colSection10ItemsBaseTypes_NoSQLId = {  "string", "string", "string", "string", "string", "string", 
															"string", "string", "string", "string", "string", "string",
															"string", "string", "int", "int", "string",
															"string", "int", "string", "string", "int", "int", "int"};

			public string sITPSection10TableName = "ITPSection10";

            //These arrays are with the autoId column for these times we need to define the complete table
            string[] colRFUHeaderNames = { "AutoID", "ID", "PWRID", "DesignLoad", "CutoverLoad", "CutoverDate", "Decommission", 
                                           "Commission", "Audit_UserId", "Audit_DateStamp", "BatteryCapacity", "Comments"};
            string[] colRFUHeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[float] NULL", "[float] NULL", "[nvarchar](50) NULL", "[int] NULL", 
				"[int] NULL","[nvarchar](50) NULL","[nvarchar](50) NULL","[float] NULL", "[nvarchar](3000) NULL"};
            string[] colRFUHeaderBaseTypes = { "autoincrement", "string", "string", "float", "float", "string", "int", 
                                               "int", "string", "string", "float", "string"};
            //These arrays are without the autoId column because we do not bring this across and don't want it populated by the code
            string[] colRFUItemsNames = { "ID", "PWRID", "DesignLoad", "CutoverLoad", "CutoverDate", "Decommission", 
                                          "Commission", "Audit_DateStamp", "BatteryCapacity", "Comments"};
            string[] colRFUItemsTypes = { "[nvarchar](50) NULL", "[nvarchar](50) NULL", "[float] NULL", "[float] NULL", "[nvarchar](50) NULL", "[int] NULL", 
										  "[int] NULL","[nvarchar](50) NULL","[float] NULL", "[nvarchar](3000) NULL",};
            string[] colRFUItemsBaseTypes = { "string", "string", "float", "float", "string", "int", 
                                              "int", "string", "float", "string"};

            public string sITPRFUTableName = "ITPRFU";

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
				return bRtn;
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
				return bRtn;
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
                           "select Distinct H.ID, Q.SectionId, Q.Item, 0,0,0 " +
                           "from ITPDocumentHeader H, ITPTypes T, ITPQuestionnaire Q " +
                           "where H.ID = '" + sId + "' " +
                           "and trim(H.ITPType) = trim(T.ITPDescription) " +
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

            public bool ProjectQuestionsSectionFullyAnswered(string sId, int iSectionId)
            {
                string sSQL = "Select * from " + sITPSectionQuestionsTableName + " where ID = '" + sId + "' and " +
							  "ifnull(Yes,0) + ifnull(No,0) + ifnull(NA,0) = 0 and SectionId = " + iSectionId;

                //"cast(isnull(Yes,0) as int) + cast(isnull(No,0) as int) + cast(isnull(NA,0) as int) = 0";

                string sRtnMsg = "";
                int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
                if (iRecords > 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }
                
            }

			public bool ProjectQuestionsFullyAnswered(string sId)
			{
				string sSQL = "Select * from " + sITPSectionQuestionsTableName + " where ID = '" + sId + "' and " +
					"ifnull(Yes,0) + ifnull(No,0) + ifnull(NA,0) = 0";
				
				//"cast(isnull(Yes,0) as int) + cast(isnull(No,0) as int) + cast(isnull(NA,0) as int) = 0";
				
				string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
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
                           "and M.SectionId = D.MappedSectionId " + 
						   " order by MappedSectionId";
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

                sCurrentDateAndTime = DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss");

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
           
            public bool ITPProjectSectionDeleteSection10MarkedItems(string sId, ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();
                
                if (DB.TableExists(sITPSection10TableName))
                {
                    
                    sSQL = "delete from " + sITPSection10TableName + " where ID = '" + sId + "' and Status = 3";
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }
                
            }

            public bool ITPProjectSectionDeleteSection10Item(string sId, int iAutoId, bool bFullDelete, ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();
                
                if (DB.TableExists(sITPDocumentSectionTableName))
                {
                    if(bFullDelete)
                    {
                        sSQL = "delete from " + sITPSection10TableName + " where ID = '" + sId + "' and AutoId = " + iAutoId;
                        return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                    }
                    else
                    {
                        sSQL = "update " + sITPSection10TableName + " set status = 3 where ID = '" + sId + "' and AutoId = " + iAutoId;
                        return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                    }
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

            public bool IsProjectSection10ItemExist(string sId, int iAutoId)
            {
                string sSQL = "Select * from " + sITPSection10TableName + " where ID = '" + sId + "' and AutoId = " + iAutoId;
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

			public bool ProjectSection10BatteryComplete(string sId)
            {
                string sSQL = "Select * from " + sITPSection10TableName + " where ID = '" + sId + "' and Equipment_Type = 6 " +
                              "and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
                              "SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
                              "Make='' or Make is null or Model = '' or Model is null or " +
							  "FuseOrCb='' or FuseOrCB is null or RatingAmps = '' or RatingAmps is null) and Status in (0,1,2)";
                string sRtnMsg = "";
                int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
                if (iRecords > 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }
                
            }

			public bool ProjectSection10BatteryFullyCommitted(string sId)
			{
				string sSQL = "Select * from " + sITPSection10TableName + " P," + sITPRFUTableName + " R " + 
						" where P.ID = '" + sId + "' and P.ID = R.ID and P.PwrId = R.PwrId " +
						" and Equipment_Type = 6 " +
						" and ((R.CutoverDate = '' or R.CutoverDate is null or (ifnull(R.Decommission,0) = 0 and ifnull(R.Commission,0) = 0)) " +
						" or (Equipment_Type = 6 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null or " +
						"FuseOrCb='' or FuseOrCB is null or RatingAmps = '' or RatingAmps is null))) and Status in (0,1,2)";
				string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
				}
				
			}

			public bool ProjectSection10BatteryPwrIdComplete(string sId, string sPwrId)
			{
				string sSQL = "Select * from " + sITPSection10TableName + " where ID = '" + sId + "' and " +
						"PwrId ='" + sPwrId + "' and Equipment_Type = 6 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null or " +
						"FuseOrCb='' or FuseOrCB is null or RatingAmps = '' or RatingAmps is null) and Status in (0,1,2)";
				string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
				}
				
			}

			public bool ProjectSection10PowerConversionComplete(string sId)
			{
				string sSQL = "Select * from " + sITPSection10TableName + " where ID = '" + sId + "' and (" +
						"(Equipment_Type = 3 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 4 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 5 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or Position = '0' or Position is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 7 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or Position = '0' or Position is null or " +
						"BankNo='0' or BankNo is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null))) and Status in (0,1,2)";
						string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
				}
				
			}
			
			public bool ProjectSection10PowerConversionFullyCommitted(string sId)
			{
				string sSQL = "Select * from " + sITPSection10TableName + " P," + sITPRFUTableName + " R " + 
						" where P.ID = '" + sId + "' and P.ID = R.ID and P.PwrId = R.PwrId " +
						" and Equipment_Type in (3,4,5,7) " +
						" and ((R.CutoverDate = '' or R.CutoverDate is null or (ifnull(R.Decommission,0) = 0 and ifnull(R.Commission,0) = 0)) " +
						"or ((Equipment_Type = 3 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 4 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 5 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or Position = '0' or Position is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 7 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or Position = '0' or Position is null or " +
						"BankNo='0' or BankNo is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)))) and Status in (0,1,2)";
				string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
				}
				
			}

			public bool ProjectSection10PwrIdPowerConversionExists(string sId, string sPwrId)
			{
				string sSQL = "Select * from " + sITPSection10TableName + " where ID = '" + sId + "' " +
					"and PwrId ='" + sPwrId + "' and Status in (0,1,2)";
				string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
				}
				
			}

			public bool ProjectSection10PwrIdPowerConversionComplete(string sId, string sPwrId)
			{
				string sSQL = "Select * from " + sITPSection10TableName + " where ID = '" + sId + "' " +
					"and PwrId ='" + sPwrId + "' and Equipment_Type in (3,4,5,7) and (" +
						"(Equipment_Type = 3 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 4 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 5 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or Position = '0' or Position is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null)) or " +
						"(Equipment_Type = 7 " +
						"and ([Floor]='0' or [Floor] is null or Suite = '0' or Suite is null or " +
						"Rack='0' or Rack is null or SubRack is null or Position = '0' or Position is null or " +
						"BankNo='0' or BankNo is null or " +
						"SerialBatch='' or SerialBatch is null or DOM = '' or DOM is null or " +
						"Make='' or Make is null or Model = '' or Model is null))) and Status in (0,1,2)";
				string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
				}

			}

			public bool ProjectSectionRFUAnyPwrIdCommitted(string sId)
			{
				string sSQL = "Select * from " + sITPRFUTableName + " where ID = '" + sId + "' and " +
					"CutoverDate <> '' and CutoverDate is not null and (Decommission in (1,-1) or Commission in (1,-1))";
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

			public bool ProjectSectionRFUPwrIdCommitted(string sId, string sPwrId)
			{
				string sSQL = "Select * from " + sITPRFUTableName + " where ID = '" + sId + "' and " +
					"PwrId ='" + sPwrId + "' and CutoverDate <> '' and CutoverDate is not null and (ifnull(Decommission,0) in (1,-1) or ifnull(Commission,0) in (1,-1))";
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

			public bool ProjectSectionRFUFullyCommitted(string sId)
			{
				string sSQL = "Select * from " + sITPRFUTableName + " where ID = '" + sId + "' and " +
					" (CutoverDate = '' or CutoverDate is null or (ifnull(Decommission,0) = 0 and ifnull(Commission,0) = 0))";
				string sRtnMsg = "";
				int iRecords = DB.GetSQLRecordCount(sSQL, ref sRtnMsg);
				if (iRecords > 0)
				{
					return false;
				}
				else
				{
					return true;
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
				return bRtn;
			}

            public bool ITPSection10SetRecord (string sId, ref int iAutoId, string[] sItemValues)
            {
                bool bRtn;
                string sSQL = "";
                string[] colNames = new string[1];
                string sRtnMsg = "";

                if (IsProjectSection10ItemExist (sId, iAutoId)) 
                {
                    string sWhereClause = " AutoId = " + iAutoId;
					bRtn = DB.UpdateRecord(sITPSection10TableName, colSection10ItemsNames_NoSQLId, colSection10ItemsBaseTypes_NoSQLId, sItemValues, sWhereClause);
                } 
                else 
                {
                    SqliteConnection conn =   DB.OpenConnection();
					bRtn = DB.AddRecordOpenConnection(sITPSection10TableName, colSection10ItemsNames_NoSQLId, colSection10ItemsBaseTypes_NoSQLId, sItemValues, conn);
                    sSQL = "Select last_insert_rowid() as NewRow";
                    DataSet ds = new DataSet();
                    colNames[0] = "NewRow";
                    ds = DB.ReadSQLDataSetOpenConnection(sSQL, colNames, ref sRtnMsg, conn);
                    string sReturn = ds.Tables[0].Rows[0].ItemArray[0].ToString();
                    iAutoId = Convert.ToInt32(sReturn);
                    DB.CloseConnection(conn);
                }
                return bRtn;
            }

            public DataSet GetLocalITPSection10PwrIds(string sId, int iEquipment_Type)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];

                colNames[0] = "PwrId";

                if (DB.TableExists(sITPSection10TableName))
                {
                    sSQL = "select distinct PwrId " +
                           "from " + sITPSection10TableName + " " +
                           "where id = '" + sId + "' " +
                           "and Equipment_Type = " + iEquipment_Type + " " +
                           "order by PwrId "; 
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

			public DataSet GetLocalITPSectionEquipmentPwrIds(string sId)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[1];
				
				colNames[0] = "PwrId";
				
				if (DB.TableExists(sITPSection10TableName))
				{
					sSQL = "select distinct PwrId " +
						"from " + sITPSection10TableName + " " +
							"where id = '" + sId + "' " +
							"and Equipment_Type in (3,4,5,7) " +  //This includes racks, subracks, position and solar strings but NOT batteries
							"order by PwrId "; 
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
                string[] colNames = new string[27];

                colNames[0] = "AutoId";
				colNames[1] = "SQLAutoId";
				colNames[2] = "ID";
                colNames[3] = "PwrId";
                colNames[4] = "BankNo";
                colNames[5] = "Floor";
                colNames[6] = "Suite";
                colNames[7] = "Rack";
                colNames[8] = "SubRack";
                colNames[9] = "Position";
                colNames[10] = "Make";
                colNames[11] = "Model";
                colNames[12] = "SerialBatch";
                colNames[13] = "DOM";
                colNames[14] = "FuseOrCB";
                colNames[15] = "RatingAmps";
                colNames[16] = "LinkTest";
                colNames[17] = "BatteryTest";
                colNames[18] = "Audit_UserId";
                colNames[19] = "Audit_DateStamp";
                colNames[20] = "tblMaximoTransfer_Eqnum";
                colNames[21] = "tblMaximoPSA_ID";
                colNames[22] = "Equipment_Condition";
                colNames[23] = "SPN";
                colNames[24] = "Duplicate";
                colNames[25] = "Equipment_Type";
                colNames[26] = "Status";

                if (DB.TableExists(sITPSection10TableName))
                {
                    sSQL = "select * " +
                           "from " + sITPSection10TableName + " " +
                           "where id = '" + sId + "' " +
                           "and PwrId = '" + sPwrId + "' " +
                           "and Status in (0,1,2) " +
                           "and Equipment_Type = 6 " +
                           "order by BankNo ";
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

			public DataSet GetLocalITPSection10PwrIdEquipmentDetails(string sId, string sPwrId)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[22];
				
				colNames[0] = "AutoId";
				colNames[1] = "ID";
				colNames[2] = "PwrId";
				colNames[3] = "BankNo";
				colNames[4] = "Floor";
				colNames[5] = "Suite";
				colNames[6] = "Rack";
				colNames[7] = "SubRack";
				colNames[8] = "Position";
				colNames[9] = "Make";
				colNames[10] = "Model";
				colNames[11] = "SerialBatch";
				colNames[12] = "DOM";
				colNames[13] = "Audit_UserId";
				colNames[14] = "Audit_DateStamp";
				colNames[15] = "tblMaximoTransfer_Eqnum";
				colNames[16] = "tblMaximoPSA_ID";
				colNames[17] = "Equipment_Condition";
				colNames[18] = "SPN";
				colNames[19] = "Duplicate";
				colNames[20] = "Equipment_Type";
				colNames[21] = "Status";
				
				if (DB.TableExists(sITPSection10TableName))
				{
					sSQL = "select AutoId,ID,PwrId,BankNo,Floor,Suite,Rack,SubRack,Position,Make,Model,SerialBatch,DOM, " +
							"Audit_UserId,Audit_DateStamp,tblMaximoTransfer_Eqnum,tblMaximoPSA_ID,Equipment_Condition,SPN, " +
							"Duplicate,Equipment_Type, Status " +
							"from " + sITPSection10TableName + " " +
							"where id = '" + sId + "' " +
							"and PwrId = '" + sPwrId + "' " +
							"and Status in (0,1,2) " +
							"and Equipment_Type IN (3,4,5,7) " + //Everything BUT batteries
							"order by ifnull(Floor,0) * 1, Floor, " +
							"ifnull(Suite,0) * 1, Suite, ifnull(Rack,0) * 1, Rack, " +
							"ifnull(SubRack,0) * 1, SubRack, ifnull(Position,0) * 1, " +
							"Position, ifnull(BankNo,0) * 1, BankNo "; //The bank no column holds the solar string where applicable
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
			
			public DataSet GetLocalITPSection10Details(string sId)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[27];
                
                colNames[0] = "AutoId";
				colNames[1] = "SQLAutoId";
				colNames[2] = "ID";
                colNames[3] = "PwrId";
                colNames[4] = "BankNo";
                colNames[5] = "Floor";
                colNames[6] = "Suite";
                colNames[7] = "Rack";
                colNames[8] = "SubRack";
                colNames[9] = "Position";
                colNames[10] = "Make";
                colNames[11] = "Model";
                colNames[12] = "SerialBatch";
                colNames[13] = "DOM";
                colNames[14] = "FuseOrCB";
                colNames[15] = "RatingAmps";
                colNames[16] = "LinkTest";
                colNames[17] = "BatteryTest";
                colNames[18] = "Audit_UserId";
                colNames[19] = "Audit_DateStamp";
                colNames[20] = "tblMaximoTransfer_Eqnum";
                colNames[21] = "tblMaximoPSA_ID";
                colNames[22] = "Equipment_Condition";
                colNames[23] = "SPN";
                colNames[24] = "Duplicate";
                colNames[25] = "Equipment_Type";
                colNames[26] = "Status";

                if (DB.TableExists(sITPSection10TableName))
                {
                        sSQL = "select * " +
	                            "from " + sITPSection10TableName + " " +
                                "where id = '" + sId + "' " + 
                                "order by PwrId, Floor, Suite, Rack, SubRack, Position ";
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

            public bool ITPProjectSectionDeleteAllRFUItems(string sId, ref string sRtnMsg)
            {
                string sSQL;
                LocalDB DB = new LocalDB();
                
                if (DB.TableExists(sITPRFUTableName))
                {
                    
                    sSQL = "delete from " + sITPRFUTableName + " where ID = '" + sId + "'";
                    return DB.ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    return true;
                }
                
            }

            public bool CheckRFUTable()
            {
                if (!DB.TableExists(sITPRFUTableName))
                {
                    if (!DB.CreateTable(sITPRFUTableName, colRFUHeaderNames, colRFUHeaderTypes))
                    {
                        return false;
                    }
                }
                
                return true;
            }
            
            public bool ITPRFUAddRecord(string[] sItemValues)
            {
                bool bRtn;
                
                bRtn = DB.AddRecord(sITPRFUTableName, colRFUItemsNames, colRFUItemsBaseTypes, sItemValues);
                return bRtn;
            }
            
            public bool ITPRFUSetRecord (string sId, string sPwrId, string[] sItemValues)
            {
                bool bRtn;

                string sWhereClause = " ID = '" + sId + "' and PwrId = '" + sPwrId + "'";
                bRtn = DB.UpdateRecord(sITPRFUTableName, colRFUItemsNames, colRFUItemsBaseTypes, sItemValues, sWhereClause);
                return bRtn;
            }

            public DataSet GetLocalITPRFUPwrIds(string sId)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[8];
                
                colNames[0] = "PwrId";
                colNames[1] = "DesignLoad";
                colNames[2] = "CutoverLoad";
                colNames[3] = "CutoverDate";
                colNames[4] = "Decommission";
                colNames[5] = "Commission";
                colNames[6] = "BatteryCapacity";
				colNames[7] = "Comments";

                if (DB.TableExists(sITPRFUTableName))
                {
                    sSQL =  "select PwrId, DesignLoad, CutoverLoad, CutoverDate, Decommission, Commission, BatteryCapacity, Comments " +
                            "from " + sITPRFUTableName + " " +
                            "where id = '" + sId + "' " +
                            "order by PwrId "; 
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

			public DataSet GetLocalITPRFUInfo(string sId)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[8];
				
				colNames[0] = "Id";
				colNames[1] = "PwrId";
				colNames[2] = "CutoverLoad";
				colNames[3] = "CutoverDate";
				colNames[4] = "Decommission";
				colNames[5] = "Commission";
				colNames[6] = "Audit_DateStamp";
				colNames[7] = "Comments";

				if (DB.TableExists(sITPRFUTableName))
				{
					sSQL =  "select Id, PwrId, CutoverLoad, CutoverDate, Decommission, Commission, Audit_DateStamp, Comments " +
							"from " + sITPRFUTableName + " " +
							"where id = '" + sId + "' " +
							"order by PwrId "; 
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

		public class ITPBatteryTest
		{
			LocalDB DB = new LocalDB();

			string[] colAcceptTestType1HeaderNames = { "AutoId", "ID", "PwrId", "BankNo", 
													  "Sec0", "Sec15", "Sec30", "Sec45", 
													  "Min1", "Min1_15", "Min1_30", "Min1_45",
													  "Min2", "Min2_15", "Min2_30", "Min2_45",
													  "Min3", "Min4", "Min5", "Min6",
													  "Min7", "Min8", "Min10",
													  "Min12", "Min14", "Min16", "Min18","Min20", 
													  "Audit_UserId", "Audit_DateStamp"};
			string[] colAcceptTestType1HeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
													  "[float] NULL","[float] NULL","[float] NULL","[float] NULL",
													  "[float] NULL","[float] NULL","[float] NULL","[float] NULL",
													  "[float] NULL","[float] NULL","[float] NULL","[float] NULL",
													  "[float] NULL","[float] NULL","[float] NULL","[float] NULL",
													  "[float] NULL","[float] NULL","[float] NULL",
													  "[float] NULL","[float] NULL","[float] NULL","[float] NULL","[float] NULL", 
													  "[nvarchar](50) NULL", "[nvarchar](50) NULL" };
			string[] colAcceptTestType1HeaderBaseTypes = { "autoincrement", "string", "string", "int",
														  "float","float","float","float",
														  "float","float","float","float",
														  "float","float","float","float",
														  "float","float","float","float",
														  "float","float","float",
														  "float","float","float","float","float",
														  "string", "string",};

			string[] colAcceptTestType1HeaderNamesNoAutoIdAndUserOnly = {"ID", "PwrId", "BankNo", 
				"Sec0", "Sec15", "Sec30", "Sec45", 
				"Min1", "Min1_15", "Min1_30", "Min1_45",
				"Min2", "Min2_15", "Min2_30", "Min2_45",
				"Min3", "Min4", "Min5", "Min6",
				"Min7", "Min8", "Min10",
				"Min12", "Min14", "Min16", "Min18","Min20", 
				"Audit_UserId", "Audit_DateStamp"};
			string[] colAcceptTestType1HeaderTypesNoAutoIdAndUserOnly = { "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL","[float] NULL", 
				"[nvarchar](50) NULL", "[nvarchar](50) NULL" };
			string[] colAcceptTestType1HeaderBaseTypesNoAutoIdAndUserOnly = { "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float",
				"float","float","float","float","float",
				"string", "string",};

			string[] colAcceptTestType1HeaderNamesNoAutoId = { "ID", "PwrId", "BankNo", 
				"Sec0", "Sec15", "Sec30", "Sec45", 
				"Min1", "Min1_15", "Min1_30", "Min1_45",
				"Min2", "Min2_15", "Min2_30", "Min2_45",
				"Min3", "Min4", "Min5", "Min6",
				"Min7", "Min8", "Min10",
				"Min12", "Min14", "Min16", "Min18","Min20"};
			string[] colAcceptTestType1HeaderTypesNoAutoId = { "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL","[float] NULL",};
			string[] colAcceptTestType1HeaderBaseTypesNoAutoId = { "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float",
				"float","float","float","float","float"};

			public string sITPBatteryAcceptTestDischargeCurrentTableName = "ITPBattAcceptTest_DischrgCurrent";
			public string sITPBatteryAcceptTestDischargeVoltTableName = "ITPBattAcceptTest_DischrgVolt";

			string[] colAcceptTestType2HeaderNames = { "AutoId", "ID", "PwrId", "BankNo", 
				"Cell1", "Cell2", "Cell3", "Cell4", 
				"Cell5", "Cell6", "Cell7", "Cell8", 
				"Cell9", "Cell10", "Cell11", "Cell12", 
				"Cell13", "Cell14", "Cell15", "Cell16", 
				"Cell17", "Cell18", "Cell19", "Cell20", 
				"Cell21", "Cell22", "Cell23", "Cell24",
				"Audit_UserId", "Audit_DateStamp"};

			string[] colAcceptTestType2HeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[nvarchar](50) NULL", "[nvarchar](50) NULL" };
			string[] colAcceptTestType2HeaderBaseTypes = { "autoincrement", "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"string", "string",};

			string[] colAcceptTestType2HeaderNamesNoAutoIdAndUserOnly = { "ID", "PwrId", "BankNo", 
				"Cell1", "Cell2", "Cell3", "Cell4", 
				"Cell5", "Cell6", "Cell7", "Cell8", 
				"Cell9", "Cell10", "Cell11", "Cell12", 
				"Cell13", "Cell14", "Cell15", "Cell16", 
				"Cell17", "Cell18", "Cell19", "Cell20", 
				"Cell21", "Cell22", "Cell23", "Cell24",
				"Audit_UserId", "Audit_DateStamp"};

			string[] colAcceptTestType2HeaderTypesNoAutoIdAndUserOnly = { "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[nvarchar](50) NULL", "[nvarchar](50) NULL" };
			string[] colAcceptTestType2HeaderBaseTypesNoAutoIdAndUserOnly = { "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"string", "string",};

			string[] colAcceptTestType2HeaderNamesNoAutoId = {"ID", "PwrId", "BankNo", 
				"Cell1", "Cell2", "Cell3", "Cell4", 
				"Cell5", "Cell6", "Cell7", "Cell8", 
				"Cell9", "Cell10", "Cell11", "Cell12", 
				"Cell13", "Cell14", "Cell15", "Cell16", 
				"Cell17", "Cell18", "Cell19", "Cell20", 
				"Cell21", "Cell22", "Cell23", "Cell24"};

			string[] colAcceptTestType2HeaderTypesNoAutoId = { "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL"};
			string[] colAcceptTestType2HeaderBaseTypesNoAutoId = { "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float"};

			public string sITPBatteryAcceptTestFloatRecordTableName = "ITPBattAcceptTest_FloatRecord";
			public string sITPBatteryAcceptTestOCVolts05HrTableName = "ITPBattAcceptTest_OCVolts05Hr";
			public string sITPBatteryAcceptTestUnpackedTableName = "ITPBattAcceptTest_Unpacked";
			public string sITPBatteryAcceptTestVolts5MinTableName = "ITPBattAcceptTest_Volts5Min";
			public string sITPBatteryAcceptTestVolts10MinTableName = "ITPBattAcceptTest_Volts10Min";
			public string sITPBatteryAcceptTestVolts15MinTableName = "ITPBattAcceptTest_Volts15Min";
			public string sITPBatteryAcceptTestVolts20MinTableName = "ITPBattAcceptTest_Volts20Min";

			string[] colAcceptTestType3HeaderNames = { "AutoId", "ID", "PwrId", "BankNo", 
				"Cell1to2", "Cell2to3", "Cell3to4", "Cell4to5", 
				"Cell5to6", "Cell6to7", "Cell7to8", "Cell8to9", 
				"Cell9to10", "Cell10to11", "Cell11to12", "Cell12to13", 
				"Cell13to14", "Cell14to15", "Cell15to16", "Cell16to17", 
				"Cell17to18", "Cell18to19", "Cell19to20", "Cell20to21", 
				"Cell21to22", "Cell22to23", "Cell23to24", "Pos","Neg",
				"Audit_UserId", "Audit_DateStamp"};

			string[] colAcceptTestType3HeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[nvarchar](50) NULL", "[nvarchar](50) NULL" };

			string[] colAcceptTestType3HeaderBaseTypes = { "autoincrement", "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float","float",
				"string", "string",};

			string[] colAcceptTestType3HeaderNamesNoAutoIdAndUserOnly  = { "ID", "PwrId", "BankNo", 
				"Cell1to2", "Cell2to3", "Cell3to4", "Cell4to5", 
				"Cell5to6", "Cell6to7", "Cell7to8", "Cell8to9", 
				"Cell9to10", "Cell10to11", "Cell11to12", "Cell12to13", 
				"Cell13to14", "Cell14to15", "Cell15to16", "Cell16to17", 
				"Cell17to18", "Cell18to19", "Cell19to20", "Cell20to21", 
				"Cell21to22", "Cell22to23", "Cell23to24", "Pos","Neg",
				"Audit_UserId", "Audit_DateStamp"};

			string[] colAcceptTestType3HeaderTypesNoAutoIdAndUserOnly = {"[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[nvarchar](50) NULL", "[nvarchar](50) NULL" };

			string[] colAcceptTestType3HeaderBaseTypesNoAutoIdAndUserOnly = { "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float","float",
				"string", "string",};


			string[] colAcceptTestType3HeaderNamesNoAutoId = { "ID", "PwrId", "BankNo", 
				"Cell1to2", "Cell2to3", "Cell3to4", "Cell4to5", 
				"Cell5to6", "Cell6to7", "Cell7to8", "Cell8to9", 
				"Cell9to10", "Cell10to11", "Cell11to12", "Cell12to13", 
				"Cell13to14", "Cell14to15", "Cell15to16", "Cell16to17", 
				"Cell17to18", "Cell18to19", "Cell19to20", "Cell20to21", 
				"Cell21to22", "Cell22to23", "Cell23to24", "Pos","Neg"};

			string[] colAcceptTestType3HeaderTypesNoAutoId = { "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL","[float] NULL","[float] NULL"};

			string[] colAcceptTestType3HeaderBaseTypesNoAutoId = { "string", "string", "int",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float",
				"float","float","float","float","float"};

			public string sITPBatteryAcceptTestLink1To3TableName = "ITPBattAcceptTest_Link1To3";
			public string sITPBatteryAcceptTestLink2To3TableName = "ITPBattAcceptTest_Link2To3";
			public string sITPBatteryAcceptTestLink3To3TableName = "ITPBattAcceptTest_Link3To3";

			string[] colAcceptTestType4HeaderNames = { "AutoId", "ID", "PwrId", "BankNo", 
				"InspectionDate", "InspectedBy", "TestDate", "FloatVoltsPriorTest", 
				"PeriodOnChargeTest", "BatteryStringCapacity", "DischargeLoadRate", 
				"TechnicianName", "TechnicianNumber", "BMP_BPU_CB_Alarm", "SiteLoad", 
				"ResistiveLoad", "Comments", "FloatVoltage", "FloatLoadCurrent", 
				"TwentyMinuteEndDischargeVolts", "CellVoltage", "CellPost", "MinMonoBlockVolts",
				"Audit_UserId", "Audit_DateStamp"};

			string[] colAcceptTestType4HeaderTypes = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[nvarchar](50) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL",
				"[nvarchar](50) NULL","[nvarchar](50) NULL","[int] NULL","[int] NULL",
				"[int] NULL","[nvarchar](5000) NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[int] NULL","[float] NULL",
				"[nvarchar](50) NULL", "[nvarchar](50) NULL" };

			string[] colAcceptTestType4HeaderBaseTypes = { "autoincrement", "string", "string", "int",
				"string","string","string","float",
				"float","float","float",
				"string","string","int","int",
				"int","string","float","float",
				"float","float","int","float",
				"string", "string",};

			string[] colAcceptTestType4HeaderNamesNoAutoIdAndUserOnly = { "ID", "PwrId", "BankNo", 
				"InspectionDate", "InspectedBy", "TestDate", "FloatVoltsPriorTest", 
				"PeriodOnChargeTest", "BatteryStringCapacity", "DischargeLoadRate", 
				"TechnicianName", "TechnicianNumber", "BMP_BPU_CB_Alarm", "SiteLoad", 
				"ResistiveLoad", "Comments", "FloatVoltage", "FloatLoadCurrent", 
				"TwentyMinuteEndDischargeVolts", "CellVoltage", "CellPost", "MinMonoBlockVolts",
				"Audit_UserId", "Audit_DateStamp"};

			string[] colAcceptTestType4HeaderTypesNoAutoIdAndUserOnly = {"[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[nvarchar](50) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL",
				"[nvarchar](50) NULL","[nvarchar](50) NULL","[int] NULL","[int] NULL",
				"[int] NULL","[nvarchar](5000) NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[int] NULL","[float] NULL",
				"[nvarchar](50) NULL", "[nvarchar](50) NULL" };

			string[] colAcceptTestType4HeaderBaseTypesNoAutoIdAndUserOnly = { "string", "string", "int",
				"string","string","string","float",
				"float","float","float",
				"string","string","int","int",
				"int","string","float","float",
				"float","float","int","float",
				"string", "string",};

			string[] colAcceptTestType4HeaderNamesNoAutoId = { "ID", "PwrId", "BankNo", 
				"InspectionDate", "InspectedBy", "TestDate", "FloatVoltsPriorTest", 
				"PeriodOnChargeTest", "BatteryStringCapacity", "DischargeLoadRate", 
				"TechnicianName", "TechnicianNumber", "BMP_BPU_CB_Alarm", "SiteLoad", 
				"ResistiveLoad", "Comments", "FloatVoltage", "FloatLoadCurrent", 
				"TwentyMinuteEndDischargeVolts", "CellVoltage", "CellPost", "MinMonoBlockVolts"};

			string[] colAcceptTestType4HeaderTypesNoAutoId = { "[nvarchar](50) NOT NULL","[nvarchar](50) NOT NULL", "[int] NOT NULL",
				"[nvarchar](50) NULL","[nvarchar](50) NULL","[nvarchar](50) NULL","[float] NULL",
				"[float] NULL","[float] NULL","[float] NULL",
				"[nvarchar](50) NULL","[nvarchar](50) NULL","[int] NULL","[int] NULL",
				"[int] NULL","[nvarchar](5000) NULL","[float] NULL","[float] NULL",
				"[float] NULL","[float] NULL","[int] NULL","[float] NULL"};

			string[] colAcceptTestType4HeaderBaseTypesNoAutoId = { "string", "string", "int",
				"string","string","string","float",
				"float","float","float",
				"string","string","int","int",
				"int","string","float","float",
				"float","float","int","float"};

			public string sITPBatteryAcceptTestHeaderTableName = "ITPBattAcceptTest_Header";

			public bool CheckITPBatteryAcceptTest_DischargeCurrentTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestDischargeCurrentTableName, 1);
			}

			public bool CheckITPBatteryAcceptTest_DischargeVoltTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestDischargeVoltTableName, 1);
			}

			public bool CheckITPBatteryAcceptTest_FloatRecordTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestFloatRecordTableName, 2);
			}

			public bool CheckITPBatteryAcceptTest_OCVolts05HrTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestOCVolts05HrTableName, 2);
			}

			public bool CheckITPBatteryAcceptTest_UnpackedTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestUnpackedTableName, 2);
			}

			public bool CheckITPBatteryAcceptTest_Volts5MinTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestVolts5MinTableName, 2);
			}

			public bool CheckITPBatteryAcceptTest_Volts10MinTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestVolts10MinTableName, 2);
			}

			public bool CheckITPBatteryAcceptTest_Volts15MinTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestVolts15MinTableName, 2);
			}

			public bool CheckITPBatteryAcceptTest_Volts20MinTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestVolts20MinTableName, 2);
			}

			public bool CheckITPBatteryAcceptTest_Link1To3Table()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestLink1To3TableName, 3);
			}

			public bool CheckITPBatteryAcceptTest_Link2To3Table()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestLink2To3TableName, 3);
			}

			public bool CheckITPBatteryAcceptTest_Link3To3Table()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestLink3To3TableName, 3);
			}

			public bool CheckITPBatteryAcceptTest_HeaderTable()
			{
				return CheckITPBatteryAcceptTestTable (sITPBatteryAcceptTestHeaderTableName, 4);
			}

			public bool CheckITPBatteryAcceptTestTable(string sTableName, int iTableType)
			{

				if (!DB.TableExists(sTableName))
				{
					switch(iTableType)
					{
						case 1:
							return DB.CreateTable(sTableName, colAcceptTestType1HeaderNames, colAcceptTestType1HeaderTypes);
						case 2:
							return DB.CreateTable(sTableName, colAcceptTestType2HeaderNames, colAcceptTestType2HeaderTypes);
						case 3:
							return DB.CreateTable(sTableName, colAcceptTestType3HeaderNames, colAcceptTestType3HeaderTypes);
						case 4:
							return DB.CreateTable(sTableName, colAcceptTestType4HeaderNames, colAcceptTestType4HeaderTypes);
						default:
							return DB.CreateTable(sTableName, colAcceptTestType1HeaderNames, colAcceptTestType1HeaderTypes);
					}
				}
				else
				{
					return true;
				}


			}

			public bool TableITPBatteryAcceptTestDeleteAllRecords(string sTableName, string sId, ref string sRtnMsg)
			{
				string sSQL;
				LocalDB DB = new LocalDB();

				if (DB.TableExists(sTableName))
				{

					sSQL = "delete from " + sTableName + " where ID = '" + sId + "'";
					return DB.ExecuteSQL(sSQL, ref sRtnMsg);
				}
				else
				{
					return true;
				}

			}

			public bool ITPBattTestAddRecord(string[] sItemValues, string sTableName, int iTableType)
			{
				bool bRtn;

				switch (iTableType) 
				{
				case 1:
					bRtn = DB.AddRecord(sTableName, colAcceptTestType1HeaderNamesNoAutoId, colAcceptTestType1HeaderTypesNoAutoId, sItemValues);
					break;
				case 2:
					bRtn = DB.AddRecord(sTableName, colAcceptTestType2HeaderNamesNoAutoId, colAcceptTestType2HeaderTypesNoAutoId, sItemValues);
					break;
				case 3:
					bRtn = DB.AddRecord(sTableName, colAcceptTestType3HeaderNamesNoAutoId, colAcceptTestType3HeaderTypesNoAutoId, sItemValues);
					break;
				case 4:
					bRtn = DB.AddRecord(sTableName, colAcceptTestType4HeaderNamesNoAutoId, colAcceptTestType4HeaderTypesNoAutoId, sItemValues);
					break;
				default:
					bRtn = DB.AddRecord(sTableName, colAcceptTestType1HeaderNamesNoAutoId, colAcceptTestType1HeaderTypesNoAutoId, sItemValues);
					break;
				}
				return bRtn;
			}


			public bool TableITPBatteryAcceptTestDischargeCurrentSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestDischargeCurrentTableName, 1); 
			}

			public bool TableITPBatteryAcceptTestDischargeVoltSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestDischargeVoltTableName, 1); 
			}

			public bool TableITPBatteryAcceptTestFloatRecordSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestFloatRecordTableName, 2); 
			}

			public bool TableITPBatteryAcceptTestOCVolts05HrSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestOCVolts05HrTableName, 2); 
			}

			public bool TableITPBatteryAcceptTestUnpackedSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestUnpackedTableName, 2); 
			}

			public bool TableITPBatteryAcceptTestVolts5MinSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestVolts5MinTableName, 2); 
			}

			public bool TableITPBatteryAcceptTestVolts10MinSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestVolts10MinTableName, 2); 
			}

			public bool TableITPBatteryAcceptTestVolts15MinSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestVolts15MinTableName, 2); 
			}

			public bool TableITPBatteryAcceptTestVolts20MinSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestVolts20MinTableName, 2); 
			}

			public bool TableITPBatteryAcceptTestLink1to3SetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestLink1To3TableName, 3); 
			}

			public bool TableITPBatteryAcceptTestLink2to3SetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestLink2To3TableName, 3); 
			}

			public bool TableITPBatteryAcceptTestLink3to3SetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestLink3To3TableName, 3); 
			}

			public bool TableITPBatteryAcceptTestHeaderSetRecord (string sId, ref int iAutoId, string[] sItemValues)
			{
				return TableITPBatteryAcceptTestSetRecord (sId, ref iAutoId, sItemValues, sITPBatteryAcceptTestHeaderTableName, 4); 
			}

			public bool TableITPBatteryAcceptTestSetRecord (string sId, ref int iAutoId, string[] sItemValues, string sTableName, int iTableType)
			{
				bool bRtn = false;
				string sSQL = "";
				string[] colNames = new string[1];
				string sRtnMsg = "";

				if (ITPBatteryAcceptTestItemExist (iAutoId, sTableName)) 
				{
					string sWhereClause = " AutoId = " + iAutoId;
					switch (iTableType) 
					{
						case 1:
							bRtn = DB.UpdateRecord (sTableName, colAcceptTestType1HeaderNamesNoAutoIdAndUserOnly, colAcceptTestType1HeaderTypesNoAutoIdAndUserOnly, sItemValues, sWhereClause);
							break;
						case 2:
							bRtn = DB.UpdateRecord (sTableName, colAcceptTestType2HeaderNamesNoAutoIdAndUserOnly, colAcceptTestType2HeaderTypesNoAutoIdAndUserOnly, sItemValues, sWhereClause);
							break;
						case 3:
							bRtn = DB.UpdateRecord (sTableName, colAcceptTestType3HeaderNamesNoAutoIdAndUserOnly, colAcceptTestType3HeaderTypesNoAutoIdAndUserOnly, sItemValues, sWhereClause);
							break;
						case 4:
							bRtn = DB.UpdateRecord (sTableName, colAcceptTestType4HeaderNamesNoAutoIdAndUserOnly, colAcceptTestType4HeaderTypesNoAutoIdAndUserOnly, sItemValues, sWhereClause);
							break;
					}
				} 
				else 
				{
					SqliteConnection conn =   DB.OpenConnection();
					switch (iTableType) 
					{
						case 1:
							bRtn = DB.AddRecordOpenConnection (sTableName, colAcceptTestType1HeaderNamesNoAutoId, colAcceptTestType1HeaderTypesNoAutoId, sItemValues, conn);
							break;
						case 2:
							bRtn = DB.AddRecordOpenConnection (sTableName, colAcceptTestType2HeaderNamesNoAutoId, colAcceptTestType2HeaderTypesNoAutoId, sItemValues, conn);
							break;
						case 3:
							bRtn = DB.AddRecordOpenConnection (sTableName, colAcceptTestType3HeaderNamesNoAutoId, colAcceptTestType3HeaderTypesNoAutoId, sItemValues, conn);
							break;
						case 4:
						bRtn = DB.AddRecordOpenConnection (sTableName, colAcceptTestType4HeaderNamesNoAutoIdAndUserOnly, colAcceptTestType4HeaderTypesNoAutoIdAndUserOnly, sItemValues, conn);
							break;
					}

					sSQL = "Select last_insert_rowid() as NewRow";
					DataSet ds = new DataSet();
					colNames[0] = "NewRow";
					ds = DB.ReadSQLDataSetOpenConnection(sSQL, colNames, ref sRtnMsg, conn);
					string sReturn = ds.Tables[0].Rows[0].ItemArray[0].ToString();
					iAutoId = Convert.ToInt32(sReturn);
					DB.CloseConnection(conn);
				}
				return bRtn;
			}


			public bool ITPBatteryAcceptTestItemExist(int iAutoId, string sTableName)
			{
				string sSQL = "Select * from " + sTableName + " where AutoId = " + iAutoId;
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

			public DataSet GetBatteryAcceptTestType1Records(string sId, string sPwrId, int iBankNo, string sTableName)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[28];

				colNames[0] = "AutoId";
				colNames[1] = "ID";
				colNames[2] = "PwrId";
				colNames[3] = "BankNo";
				colNames[4] = "Sec0";
				colNames[5] = "Sec15";
				colNames[6] = "Sec30";
				colNames[7] = "Sec45";
				colNames[8] = "Min1";
				colNames[9] = "Min1_15";
				colNames[10] = "Min1_30";
				colNames[11] = "Min1_45";
				colNames[12] = "Min2";
				colNames[13] = "Min2_15";
				colNames[14] = "Min2_30";
				colNames[15] = "Min2_45";
				colNames[16] = "Min3";
				colNames[17] = "Min4";
				colNames[18] = "Min5";
				colNames[19] = "Min6";
				colNames[20] = "Min7";
				colNames[21] = "Min8";
				colNames[22] = "Min10";
				colNames[23] = "Min12";
				colNames[24] = "Min14";
				colNames[25] = "Min16";
				colNames[26] = "Min18";
				colNames[27] = "Min20";

				if (DB.TableExists(sTableName))
				{
					sSQL = "select AutoId, ID, PwrId, BankNo," + 
						"Sec0, Sec15, Sec30, Sec45, " +
						"Min1, Min1_15, Min1_30, Min1_45," +
						"Min2, Min2_15, Min2_30, Min2_45," +
						"Min3, Min4, Min5, Min6," +
						"Min7, Min8, Min10," +
						"Min12, Min14, Min16, Min18, Min20 " +
						"from " + sTableName + " " +
						"where ID = '" + sId + "' " +
						"and PwrId = '" + sPwrId + "'" +
						"and BankNo = " + iBankNo ;
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

			public DataSet GetBatteryAcceptTestType2Records(string sId, string sPwrId, int iBankNo, string sTableName)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[28];

				colNames[0] = "AutoId";
				colNames[1] = "ID";
				colNames[2] = "PwrId";
				colNames[3] = "BankNo";
				colNames[4] = "Cell1";
				colNames[5] = "Cell2";
				colNames[6] = "Cell3";
				colNames[7] = "Cell4";
				colNames[8] = "Cell5";
				colNames[9] = "Cell6";
				colNames[10] = "Cell7";
				colNames[11] = "Cell8";
				colNames[12] = "Cell9";
				colNames[13] = "Cell10";
				colNames[14] = "Cell11";
				colNames[15] = "Cell12";
				colNames[16] = "Cell13";
				colNames[17] = "Cell14";
				colNames[18] = "Cell15";
				colNames[19] = "Cell16";
				colNames[20] = "Cell17";
				colNames[21] = "Cell18";
				colNames[22] = "Cell19";
				colNames[23] = "Cell20";
				colNames[24] = "Cell21";
				colNames[25] = "Cell22";
				colNames[26] = "Cell23";
				colNames[27] = "Cell24";

				if (DB.TableExists(sTableName))
				{
					sSQL = "select AutoId, ID, PwrId, BankNo," + 
							"Cell1, Cell2, Cell3, Cell4, " +
							"Cell5, Cell6, Cell7, Cell8," +
							"Cell9, Cell10, Cell11, Cell12," +
							"Cell13, Cell14, Cell15, Cell16," +
							"Cell17, Cell18, Cell19, Cell20," +
							"Cell21, Cell22, Cell23, Cell24 " +
							"from " + sTableName + " " +
							"where ID = '" + sId + "' " +
							"and PwrId = '" + sPwrId + "'" +
							"and BankNo = " + iBankNo ;
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

			public DataSet GetBatteryAcceptTestType3Records(string sId, string sPwrId, int iBankNo, string sTableName)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[29];

				colNames[0] = "AutoId";
				colNames[1] = "ID";
				colNames[2] = "PwrId";
				colNames[3] = "BankNo";
				colNames[4] = "Cell1to2";
				colNames[5] = "Cell2to3";
				colNames[6] = "Cell3to4";
				colNames[7] = "Cell4to5";
				colNames[8] = "Cell5to6";
				colNames[9] = "Cell6to7";
				colNames[10] = "Cell7to8";
				colNames[11] = "Cell8to9";
				colNames[12] = "Cell9to10";
				colNames[13] = "Cell10to11";
				colNames[14] = "Cell11to12";
				colNames[15] = "Cell12to13";
				colNames[16] = "Cell13to14";
				colNames[17] = "Cell14to15";
				colNames[18] = "Cell15to16";
				colNames[19] = "Cell16to17";
				colNames[20] = "Cell17to18";
				colNames[21] = "Cell18to19";
				colNames[22] = "Cell19to20";
				colNames[23] = "Cell20to21";
				colNames[24] = "Cell21to22";
				colNames[25] = "Cell22to23";
				colNames[26] = "Cell23to24";
				colNames[27] = "Pos";
				colNames[28] = "Neg";

				if (DB.TableExists(sTableName))
				{
					sSQL = "select AutoId, ID, PwrId, BankNo," + 
							"Cell1to2, Cell2to3, Cell3to4, Cell4to5, " +
							"Cell5to6, Cell6to7, Cell7to8, Cell8to9," +
							"Cell9to10, Cell10to11, Cell11to12, Cell12to13," +
							"Cell13to14, Cell14to15, Cell15to16, Cell16to17," +
							"Cell17to18, Cell18to19, Cell19to20, Cell20to21," +
							"Cell21to22, Cell22to23, Cell23to24, Pos, Neg " +
							"from " + sTableName + " " +
							"where ID = '" + sId + "' " +
							"and PwrId = '" + sPwrId + "' " +
							"and BankNo = " + iBankNo ;
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
		
			public DataSet GetBatteryAcceptTestType4Records(string sId, string sPwrId, int iBankNo, string sTableName)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[23];

				colNames[0] = "AutoId";
				colNames[1] = "ID";
				colNames[2] = "PwrId";
				colNames[3] = "BankNo";
				colNames[4] = "InspectionDate";
				colNames[5] = "InspectedBy";
				colNames[6] = "TestDate";
				colNames[7] = "FloatVoltsPriorTest";
				colNames[8] = "PeriodOnChargeTest";
				colNames[9] = "BatteryStringCapacity";
				colNames[10] = "DischargeLoadRate";
				colNames[11] = "TechnicianName";
				colNames[12] = "TechnicianNumber";
				colNames[13] = "BMP_BPU_CB_Alarm";
				colNames[14] = "SiteLoad";
				colNames[15] = "ResistiveLoad";
				colNames[16] = "Comments";
				colNames[17] = "FloatVoltage";
				colNames[18] = "FloatLoadCurrent";
				colNames[19] = "TwentyMinuteEndDischargeVolts";
				colNames[20] = "CellVoltage";
				colNames[21] = "CellPost";
				colNames[22] = "MinMonoBlockVolts";

				if (DB.TableExists(sTableName))
				{
					sSQL = "select AutoId, ID, PwrId, BankNo," + 
						"InspectionDate, InspectedBy, TestDate, FloatVoltsPriorTest, " +
						"PeriodOnChargeTest, BatteryStringCapacity, DischargeLoadRate, TechnicianName," +
						"TechnicianNumber, BMP_BPU_CB_Alarm, SiteLoad, ResistiveLoad," +
						"Comments, FloatVoltage, FloatLoadCurrent, TwentyMinuteEndDischargeVolts," +
						"CellVoltage, CellPost, MinMonoBlockVolts " +
						"from " + sTableName + " " +
						"where ID = '" + sId + "' " +
						"and PwrId = '" + sPwrId + "'" +
							"and BankNo = " + iBankNo ;
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

			public DataSet GetBatteryAcceptTestType4KeyRecord(string sId, string sTableName, string sPwrId, int iBankNo)
			{
				string sSQL;
				string sRtnMsg = "";
				string[] colNames = new string[23];

				colNames[0] = "AutoId";
				colNames[1] = "ID";
				colNames[2] = "PwrId";
				colNames[3] = "BankNo";
				colNames[4] = "InspectionDate";
				colNames[5] = "InspectedBy";
				colNames[6] = "TestDate";
				colNames[7] = "FloatVoltsPriorTest";
				colNames[8] = "PeriodOnChargeTest";
				colNames[9] = "BatteryStringCapacity";
				colNames[10] = "DischargeLoadRate";
				colNames[11] = "TechnicianName";
				colNames[12] = "TechnicianNumber";
				colNames[13] = "BMP_BPU_CB_Alarm";
				colNames[14] = "SiteLoad";
				colNames[15] = "ResistiveLoad";
				colNames[16] = "Comments";
				colNames[17] = "FloatVoltage";
				colNames[18] = "FloatLoadCurrent";
				colNames[19] = "TwentyMinuteEndDischargeVolts";
				colNames[20] = "CellVoltage";
				colNames[21] = "CellPost";
				colNames[22] = "MinMonoBlockVolts";

				if (DB.TableExists(sTableName))
				{
					sSQL = "select AutoId, ID, PwrId, BankNo," + 
							"InspectionDate, InspectedBy, TestDate, FloatVoltsPriorTest, " +
							"PeriodOnChargeTest, BatteryStringCapacity, DischargeLoadRate, TechnicianName," +
							"TechnicianNumber, BMP_BPU_CB_Alarm, SiteLoad, ResistiveLoad," +
							"Comments, FloatVoltage, FloatLoadCurrent, TwentyMinuteEndDischargeVolts," +
							"CellVoltage, CellPost, MinMonoBlockVolts " +
							"from " + sTableName + " " +
							"where ID = '" + sId + "' " + 
							"and PwrId = '" + sPwrId + "' " +
							"and BankNo = " + iBankNo ;
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



		public class ITPCollection
		{
			public bool ITPLocalRemove (string sId, ref string sRtnMsg)
			{
				ITPHeaderTable hdr = new ITPHeaderTable ();
				ITPDocumentSection doc = new ITPDocumentSection ();

				if (!doc.ITPProjectSectionDeleteAllQuestions (sId, ref sRtnMsg)) 
				{
					return false;
				} 
				else 
				{
                    if (!doc.ITPProjectSectionDeleteAllSection10Items(sId, ref sRtnMsg)) 
					{
						return false;
					} 
					else 
					{
                        if (!hdr.TableHeaderDeleteAllRecords (sId, ref sRtnMsg)) 
						{
							return false;
						} 
						else 
						{
							return true;
						}
					}
				}
			}
		}
    }

    public class LocalDB
    {
        SqliteConnection m_conn;

        public SqliteConnection OpenConnection()
        {
            return Connection();;
        }

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
            catch
            {
                CloseConnection(conn);
                return false;
            }
        }

        public bool AddRecord (string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues)
        {
            return AddRecordConnection(sTableName, sColNames, sColTypes, objColValues, true);
        }

        public bool AddRecordOpenConnection (string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, SqliteConnection conn)
        {
            m_conn = conn;
            return AddRecordConnection(sTableName, sColNames, sColTypes, objColValues, false);
        }

        //Note that each of the arrays should be the same length
        public bool AddRecordConnection(string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, bool bConnectionType)
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
							if(objColValues[i].ToString() == "")
							{
								sValues += "NULL";
							}
							else
							{
	                            sDate = Dte.Get_Date_String(Convert.ToDateTime(objColValues[i]), "yyyymmdd");
	                            sValues += "'" + sDate + "'";
							}
                            break;
                        case "datetime":
							if(objColValues[i].ToString() == "")
							{
								sValues += "NULL";
							}
							else
							{
	                            sDate = Dte.Get_Date_And_Time_String(Convert.ToDateTime(objColValues[i]), "yyyymmdd hh:mm:ss");
	                            sValues += "'" + sDate + "'";
							}
                            break;
                        case "int":
                        case "bit":
                        case "float":
                        case "decimal":
							if(objColValues[i].ToString() == "")
							{
								sValues += "NULL";
							}
							else
							{
	                            sValues += objColValues[i].ToString();
							}
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
                if(bConnectionType)
                {
                    bSQL = ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    bSQL = ExecuteSQLOpenConnection(sSQL, ref sRtnMsg);
                }

                return bSQL;
            }
            catch 
            {
                return false;
            }
        }


        public bool UpdateRecord (string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, string sWhereClause)
        {
            return UpdateRecordConnection(sTableName, sColNames, sColTypes, objColValues, sWhereClause, true);
        }
        
        public bool UpdateRecordOpenConnection (string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, string sWhereClause, SqliteConnection conn)
        {
            m_conn = conn;
            return UpdateRecordConnection(sTableName, sColNames, sColTypes, objColValues, sWhereClause, false);
        }
        

        //Note that each of the arrays should be the same length
        public bool UpdateRecordConnection(string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, string sWhereClause, bool bConnectionType)
        {
            try
            {
                string sSQL = "UPDATE " + sTableName + " set ";
                DateClass Dte = new DateClass();
                string sDate = "";
                bool bSQL;
                string sRtnMsg = "";
                
                for (int i = 0; i < sColNames.Length; i++)
                {
                    if (sColTypes[i].ToLower() != "autoincrement")
                    {
                        sSQL += "[" + sColNames[i] + "] = ";
                    }
                    //                    string sColType = sColTypes[i].Substring(sColTypes[i].IndexOf("[") + 1, sColTypes[i].IndexOf("]") - sColTypes[i].IndexOf("[") -1);
                    switch (sColTypes[i].ToLower())
                    {
                    case "autoincrement":
                        break;
                    case "string":
                    case "varchar":
                    case "nvarchar":
                        sSQL += "'" + objColValues[i].ToString().Replace("'", "''") + "'";
                        break;
                    case "date":
						if(objColValues[i].ToString() == "")
						{
							sSQL +="NULL";
						}
						else
						{
							sDate = Dte.Get_Date_String(Convert.ToDateTime(objColValues[i]), "yyyymmdd");
                        	sSQL += "'" + sDate + "'";
						}
                        break;
                    case "datetime":
						if(objColValues[i].ToString() == "")
						{
							sSQL +="NULL";
						}
						else
						{
							sDate = Dte.Get_Date_And_Time_String(Convert.ToDateTime(objColValues[i]), "yyyymmdd hh:mm:ss");
                        	sSQL += "'" + sDate + "'";
						}
                        break;
                    case "int":
                    case "bit":
                    case "float":
                    case "decimal":
						if(objColValues[i].ToString() == "")
						{
							sSQL +="NULL";
						}
						else
						{
                        	sSQL += objColValues[i].ToString();
						}
                        break;
                    default: //String
                        sSQL += "'" + objColValues[i].ToString().Replace("'", "''") + "'";
                        break;
                    }
                    if (sColTypes[i].ToLower() != "autoincrement")
                    {
                        sSQL += ",";
                    }
                }
                
                sSQL = sSQL.Substring(0, sSQL.Length - 1)  + " where " + sWhereClause;

                if(bConnectionType)
                {
                    bSQL = ExecuteSQL(sSQL, ref sRtnMsg);
                }
                else
                {
                    bSQL = ExecuteSQLOpenConnection(sSQL, ref sRtnMsg);
                }
                return bSQL;
            }
            catch
            {
                return false;
            }
        }

        public bool ExecuteSQL(string sSQL, ref string sRtnMsg)
        {
            try
            {
                SqliteConnection conn = Connection();
                SqliteCommand c = new SqliteCommand();
                //int iRecords = -1;
                c = conn.CreateCommand();
                c.CommandText = sSQL;
                c.ExecuteNonQuery();
                c.Dispose();
                CloseConnection(conn);
                return true;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
                return false;
            }

        }

        public bool ExecuteSQLOpenConnection(string sSQL, ref string sRtnMsg)
        {
            try
            {
                SqliteCommand c = new SqliteCommand();
                //int iRecords = -1;
                c = m_conn.CreateCommand();
                c.CommandText = sSQL;
                c.ExecuteNonQuery();
                c.Dispose();
                return true;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
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

        public DataSet ReadSQLDataSet (string sSQL, string[] sColNames, ref string sRtnMsg)
        {
            return ReadSQLDataSetConnection(sSQL, sColNames, ref sRtnMsg, false);
        }

        public DataSet ReadSQLDataSetOpenConnection (string sSQL, string[] sColNames, ref string sRtnMsg,SqliteConnection conn )
        {
            m_conn = conn;
            return ReadSQLDataSetConnection(sSQL, sColNames, ref sRtnMsg, true);
        }

        public DataSet ReadSQLDataSetConnection(string sSQL, string[] sColNames, ref string sRtnMsg, bool bConnection)
        {
            if(!bConnection)
            {
                SqliteConnection conn = Connection();
                m_conn = conn;
            }

            SqliteCommand c = new SqliteCommand();
            try
            {
                DataSet ds = new DataSet();
                c = m_conn.CreateCommand();
                c.CommandText = sSQL;
                SqliteDataReader r = c.ExecuteReader();
                ds = ConvertDataReaderToDataSet(r, sColNames);
                if(!bConnection)
                {
                    CloseConnection(m_conn);
                }
                return ds;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
                if(!bConnection)
                {
                    CloseConnection(m_conn);
                }
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
            catch 
            {
                //string sMsg = ex.Message.ToString();
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
                            sURL = "http://silcar-ws01.silcar.com.au:8003/wbsITP_External.asmx";
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
							sURL = "http://silcar-ws01.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        default:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                    }
                    break;
            }

            return sURL;
        }

		public bool IsNumeric (System.Object Expression)
		{
			if(Expression == null || Expression is DateTime)
				return false;

			if(Expression is Int16 || Expression is Int32 || Expression is Int64 || Expression is Decimal || Expression is Single || Expression is Double || Expression is Boolean)
				return true;

			try 
			{
				if(Expression is string)
					Double.Parse(Expression as string);
				else
					Double.Parse(Expression.ToString());
				return true;
			} 
			catch 
			{

			} // just dismiss errors but return false
			return false;
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

        public object[] DownloadProjectITPRFU(string sSessionId, string sUser, string sId)
        {
            try
            {
                clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITPs = ws.GetITPProjectRFUInfo(sSessionId, sUser, sId);
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

		public object[] DownloadProjectITPBatteryDischargeTest(string sSessionId, string sUser, string sId)
		{
			try
			{
				clsLocalUtils util = new clsLocalUtils();
				string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
				wbsITP_External ws = new wbsITP_External();
				ws.Url = sURL;
				object[] objListITPs = ws.GetITPProjectBatteryAcceptTestInfo(sSessionId, sUser, sId);
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

		public object[] IsUserLoggedIn(string sSessionId, string sUser)
		{
			try
			{
				clsLocalUtils util = new clsLocalUtils();
				string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
				wbsITP_External ws = new wbsITP_External();
				ws.Url = sURL;
				object[] objListITPs = ws.IsUserLoggedIn(sSessionId, sUser);
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

		public object[] IsITPUploadable(string sSessionId, string sUser, string sId)
		{
			try
			{
				clsLocalUtils util = new clsLocalUtils();
				string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
				wbsITP_External ws = new wbsITP_External();
				ws.Url = sURL;
				object[] objListITPs = ws.IsITPUploadable(sSessionId, sUser, sId);
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

		public bool UploadITPInfo(string sSessionId, string sUser, string sId, int iUploadOrBackup, ref string sRtnMsg)
        {
            try
            {
                clsTabletDB.ITPHeaderTable ITPHdr = new clsTabletDB.ITPHeaderTable();

                //Get all the question info
                clsTabletDB.ITPDocumentSection ITP = new clsTabletDB.ITPDocumentSection();
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
                string sPwrId;
                string sBankNo;
				string sFloor;
                string sSuite;
                string sRack;
                string sSubRack;
                string sPosition;
                string sMake;
                string sModel;
                string sSerialBatch;
                string sDOM;
                string sFuseOrCB;
				string sRating;
				double v;
				string sLinkTest;
				string sBatteryTest;
                string stblMaximoTransfer_Eqnum;
				string stblMaximoPSA_ID;
                string sEquipment_Condition;
                string sSPN;
                int iDuplicate;
                int iEquipmentType;
                int iStatus;
				double dCutoverLoad;
				string sCutoverDate;
				string sDecommission;
				string sCommission;
				string sRtnStatus;
				int iSQLAutoId;
				string sBatteryTest_DischargeCurrent;
				string sBatteryTest_DischargeVolt;
				string sBatteryTest_FloatRecord;
				string sBatteryTest_OCVolts05Hr;
				string sBatteryTest_Unpacked;
				string sBatteryTest_Volts5Min;
				string sBatteryTest_Volts10Min;
				string sBatteryTest_Volts15Min;
				string sBatteryTest_Volts20Min;
				string sBatteryTest_Link1To3;
				string sBatteryTest_Link2To3;
				string sBatteryTest_Link3To3;
				string sBatteryTest_Header;

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


                //Now get all the equipment info into another big string
                DataSet ds2 = ITP.GetLocalITPSection10Details(sId);
                string sSendStringSection10 = "ITPUploadEquipmentInfo~";
                iRows = ds2.Tables[0].Rows.Count;

                if(iRows > 0)
                {
                    if (sSendString.Length > 2)
                    {
                        //Replace the last || with ~|~
                        sSendString = sSendString.Substring(0, sSendString.Length - 2) + "~|~" + sSendStringSection10;
                    }
                }

                for (i = 0; i < iRows; i++)
                {
                    iCol = ds2.Tables[0].Columns["AutoId"].Ordinal;
                    iAutoId = Convert.ToInt32(ds2.Tables[0].Rows[i].ItemArray[iCol]);
                    
					iCol = ds2.Tables[0].Columns["SQLAutoId"].Ordinal;
					if(ds2.Tables[0].Rows[i].ItemArray[iCol].ToString() == "")
					{
						iSQLAutoId = -1;
					}
					else
					{
						iSQLAutoId = Convert.ToInt32(ds2.Tables[0].Rows[i].ItemArray[iCol].ToString());
					}

					iCol = ds2.Tables[0].Columns["Id"].Ordinal;
                    sProjId = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();
                    
                    iCol = ds2.Tables[0].Columns["PwrId"].Ordinal;
                    sPwrId = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;
                    
					iCol = ds2.Tables[0].Columns["Equipment_Type"].Ordinal;
					if(ds2.Tables[0].Rows[i].ItemArray[iCol].ToString() == "")
					{
						iEquipmentType = -1;
					}
					else
					{
						iEquipmentType = Convert.ToInt32(ds2.Tables[0].Rows[i].ItemArray[iCol].ToString());
					}

                    iCol = ds2.Tables[0].Columns["BankNo"].Ordinal;
                    sBankNo = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();
                    
                    iCol = ds2.Tables[0].Columns["Floor"].Ordinal;
                    sFloor = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

                    iCol = ds2.Tables[0].Columns["Suite"].Ordinal;
                    sSuite = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

                    iCol = ds2.Tables[0].Columns["Rack"].Ordinal;
                    sRack = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

					iCol = ds2.Tables[0].Columns["SubRack"].Ordinal;
                    sSubRack = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

                    iCol = ds2.Tables[0].Columns["Position"].Ordinal;
                    sPosition = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

                    iCol = ds2.Tables[0].Columns["Make"].Ordinal;
                    sMake = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

                    iCol = ds2.Tables[0].Columns["Model"].Ordinal;
                    sModel = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

                    iCol = ds2.Tables[0].Columns["SerialBatch"].Ordinal;
                    sSerialBatch = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

                    iCol = ds2.Tables[0].Columns["DOM"].Ordinal;
                    sDOM = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

					if(sDOM == "")
					{
						sDOM = "01/01/1900";

					}
					else
					{
	                    DateTime dtDOM = DateTime.Parse(sDOM);
	                    sDOM = dtDOM.ToString("dd/MM/yyyy");
					}

                    iCol = ds2.Tables[0].Columns["FuseOrCB"].Ordinal;
                    sFuseOrCB = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();;

					iCol = ds2.Tables[0].Columns["RatingAmps"].Ordinal;
					sRating = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();
					if(!double.TryParse(sRating, out v))
					{
						sRating = "";
					}


                    iCol = ds2.Tables[0].Columns["LinkTest"].Ordinal;
                    sLinkTest = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds2.Tables[0].Columns["BatteryTest"].Ordinal;
                    sBatteryTest = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds2.Tables[0].Columns["Audit_DateStamp"].Ordinal;
                    sAudit_DateStamp = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds2.Tables[0].Columns["tblMaximoTransfer_Eqnum"].Ordinal;
                    stblMaximoTransfer_Eqnum = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds2.Tables[0].Columns["tblMaximoPSA_ID"].Ordinal;
                    stblMaximoPSA_ID = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();
                    
                    iCol = ds2.Tables[0].Columns["Equipment_Condition"].Ordinal;
                    sEquipment_Condition = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();

                    iCol = ds2.Tables[0].Columns["SPN"].Ordinal;
                    sSPN = ds2.Tables[0].Rows[i].ItemArray[iCol].ToString();
                    
                    iCol = ds2.Tables[0].Columns["Duplicate"].Ordinal;
                    iDuplicate = Convert.ToInt32(ds2.Tables[0].Rows[i].ItemArray[iCol].ToString());

                    iCol = ds2.Tables[0].Columns["Status"].Ordinal;
                    iStatus = Convert.ToInt32(ds2.Tables[0].Rows[i].ItemArray[iCol].ToString());

                    sSendString += iAutoId + "^" + sProjId + "^" + sPwrId + "^" + sBankNo + "^" + sFloor + "^" + sSuite + "^" + 
                        		   sRack + "^" + sSubRack + "^" + sPosition + "^" + sMake + "^" + sModel + "^" + sSerialBatch + "^" + sDOM + "^" + 
                                   sFuseOrCB + "^" + sRating + "^" + sLinkTest + "^" + sBatteryTest + "^" + sAudit_DateStamp + "^" + 
                                   stblMaximoTransfer_Eqnum + "^" + stblMaximoPSA_ID + "^"+ sEquipment_Condition + "^" + 
								   sSPN + "^" + iDuplicate + "^" + iEquipmentType + "^" + iStatus + "^" + iSQLAutoId + "||";

                }

				if(iRows > 0)
				{
					if (sSendString.Length > 2)
					{
						//Replace the last || with ~|~
						sSendString = sSendString.Substring(0, sSendString.Length - 2) + "~|~";
					}
				//And now grab all the battery acceptance test info for upload
				sBatteryTest_DischargeCurrent = "ITPBatteryAcceptTest_DischrgCurrent~";
				sBatteryTest_DischargeVolt = "ITPBatteryAcceptTest_DischrgVolt~";
				sBatteryTest_FloatRecord = "ITPBatteryAcceptTest_FloatRecord~";
				sBatteryTest_OCVolts05Hr = "ITPBatteryAcceptTest_OCVolts05Hr~";
				sBatteryTest_Unpacked = "ITPBatteryAcceptTest_Unpacked~";
				sBatteryTest_Volts5Min = "ITPBatteryAcceptTest_Volts5Min~";
				sBatteryTest_Volts10Min = "ITPBatteryAcceptTest_Volts10Min~";
				sBatteryTest_Volts15Min = "ITPBatteryAcceptTest_Volts15Min~";
				sBatteryTest_Volts20Min = "ITPBatteryAcceptTest_Volts20Min~";
				sBatteryTest_Link1To3 = "ITPBatteryAcceptTest_Link1To3~";
				sBatteryTest_Link2To3 = "ITPBatteryAcceptTest_Link2To3~";
				sBatteryTest_Link3To3 = "ITPBatteryAcceptTest_Link3To3~";
				sBatteryTest_Header = "ITPBatteryAcceptTest_Header~";

					sSendString += sBatteryTest_DischargeCurrent + "~|^|~" +
						sBatteryTest_DischargeVolt + "~|^|~" +
							sBatteryTest_FloatRecord + "~|^|~" +
							sBatteryTest_OCVolts05Hr + "~|^|~" +
							sBatteryTest_Unpacked + "~|^|~" +
							sBatteryTest_Volts5Min + "~|^|~" +
							sBatteryTest_Volts10Min + "~|^|~" +
							sBatteryTest_Volts15Min + "~|^|~" +
							sBatteryTest_Volts20Min + "~|^|~" +
							sBatteryTest_Link1To3 + "~|^|~" +
							sBatteryTest_Link2To3 + "~|^|~" +
							sBatteryTest_Link3To3 + "~|^|~" +
							sBatteryTest_Header + "||";
			}
				//Only add the RFU stuff if we are doing a full upload
				if(iUploadOrBackup == 0)
				{

					//Now get all the RFU info into another big string
					DataSet ds3 = ITP.GetLocalITPRFUInfo(sId);
					string sSendStringRFU = "ITPUploadRFUInfo~";
					iRows = ds3.Tables[0].Rows.Count;
					
					if(iRows > 0)
					{
						if (sSendString.Length > 2)
						{
							//Replace the last || with ~|~
							sSendString = sSendString.Substring(0, sSendString.Length - 2) + "~|~" + sSendStringRFU;
						}
					}
					
					for (i = 0; i < iRows; i++)
					{
						iCol = ds3.Tables[0].Columns["Id"].Ordinal;
						sProjId = ds3.Tables[0].Rows[i].ItemArray[iCol].ToString();
						
						iCol = ds3.Tables[0].Columns["PwrId"].Ordinal;
						sPwrId = ds3.Tables[0].Rows[i].ItemArray[iCol].ToString();;
						
						iCol = ds3.Tables[0].Columns["CutoverLoad"].Ordinal;
						dCutoverLoad = Convert.ToDouble(ds3.Tables[0].Rows[i].ItemArray[iCol].ToString());
						
						iCol = ds3.Tables[0].Columns["CutoverDate"].Ordinal;
						sCutoverDate = ds3.Tables[0].Rows[i].ItemArray[iCol].ToString();;
						
						iCol = ds3.Tables[0].Columns["Decommission"].Ordinal;
						sDecommission = ds3.Tables[0].Rows[i].ItemArray[iCol].ToString();;
						
						iCol = ds3.Tables[0].Columns["Commission"].Ordinal;
						sCommission = ds3.Tables[0].Rows[i].ItemArray[iCol].ToString();;
						
						iCol = ds3.Tables[0].Columns["Audit_DateStamp"].Ordinal;
						sAudit_DateStamp = ds3.Tables[0].Rows[i].ItemArray[iCol].ToString();
						
						iCol = ds3.Tables[0].Columns["Comments"].Ordinal;
						sComments = ds3.Tables[0].Rows[i].ItemArray[iCol].ToString();

						sSendString += sProjId + "^" + sPwrId + "^" + dCutoverLoad.ToString() + "^" + sCutoverDate + "^" + 
									   sDecommission + "^" + sCommission + "^" + sAudit_DateStamp + "^" + sComments +  "||";
					}
					
					if (sSendString.Length > 2)
					{
						//Replace the last || with nothing
						sSendString = sSendString.Substring(0, sSendString.Length - 2);
					}
				}
				else
				{
					if (sSendString.Length > 2)
					{
						//Replace the last || with nothing
						sSendString = sSendString.Substring(0, sSendString.Length - 2);
					}
					
					
				}

				clsLocalUtils util = new clsLocalUtils();
                string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
                wbsITP_External ws = new wbsITP_External();
                ws.Url = sURL;
                object[] objListITP = ws.SetITPUploadInfo(sSessionId, sUser, sSendString);
                if (objListITP[0].ToString() == "Success")
                {
					if(objListITP.Length > 1)
					{
						//Now update any new items on the tablet to the SQL autoId back in the SQL DB
						object[] arrTabletIdList = (object[])objListITP[1];
						object[] arrSQLIdList = (object[])objListITP[2];
						LocalDB DB = new LocalDB();
						string sRtnString = "";

						for(i=0;i<arrTabletIdList.Length;i++)
						{
							string sSQL = "Update " + ITP.sITPSection10TableName + " Set SQLAutoId = " + arrSQLIdList[i] + " where AutoId = " + arrTabletIdList[i];
							DB.ExecuteSQL(sSQL, ref sRtnString);
						}
					}

					if(iUploadOrBackup == 0)
					{
	                    object[] objListITPStatus = ws.SetITPStatus(sSessionId, sUser, sId, 0);
						sRtnStatus = objListITPStatus[0].ToString();
						if(sRtnStatus != "Success")
						{
							sRtnMsg = objListITPStatus[1].ToString();
						}
						{
							sRtnMsg = "";
						}
					}
					else
					{
						sRtnStatus = "Success";
					}

					if (sRtnStatus == "Success")
                    {
                        //Get rid of all the locally marked deleted items for this project
                        if(!ITP.ITPProjectSectionDeleteSection10MarkedItems(sId, ref sRtnMsg))
                        {
                            sRtnMsg = "Could not delete battery string records on project " + sId;
                            return false;
                        }

                        //Now also mark them the same locally
						if(iUploadOrBackup == 0)
						{
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
							return true;
						}
					}
                    else
                    {
                        return false;
                    }
                }
                else
                {
					if(objListITP.Length > 2)
					{
						//Now update any new items on the tablet o the SQL autoId back in the SQL DB
						object[] arrTabletIdList = (object[])objListITP[2];
						object[] arrSQLIdList = (object[])objListITP[3];
						LocalDB DB = new LocalDB();
						string sRtnString = "";

						for(i=0;i<arrTabletIdList.Length;i++)
						{
							string sSQL = "Update " + ITP.sITPSection10TableName + " Set SQLAutoId = " + arrSQLIdList[i] + " where AutoId = " + arrTabletIdList[i];
							DB.ExecuteSQL(sSQL, ref sRtnString);
						}
					}
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
                sYear = iYear.ToString().Substring(2);
                sReturn = sDay + "-" + sMonth + "-" + sYear;
            }

            if (sFormat.ToUpper() == "DD/MM/YY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString().Substring(2);
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
                sYear = iYear.ToString().Substring(2);
                sReturn = sMonth + "-" + sDay + "-" + sYear;
            }

            if (sFormat.ToUpper() == "MM/DD/YY")
            {
                sDay = iDay.ToString("D2");
                sMonth = iMonth.ToString("D2");
                sYear = iYear.ToString().Substring(2);
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
                sYear = iYear.ToString().Substring(2);
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

        public bool ValidateDate(string sDate, ref DateTime dtReturnDate)
        {
            string[] formats = {"dd/MM/yyyy", "d/MM/yyyy", "d/M/yyyy", "dd/M/yyyy",
                                "dd/MM/yy", "d/MM/yy", "d/M/yy", "dd/M/yy"};
            DateTime dateValue;
            bool bReturn;

            bReturn = DateTime.TryParseExact(sDate,formats, new System.Globalization.CultureInfo("en-AU"), System.Globalization.DateTimeStyles.None, out dateValue);
            dtReturnDate = dateValue;
            return bReturn;
        }
    }

}

