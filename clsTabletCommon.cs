using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Net;
using System.Reflection;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Security.Permissions;
using System.Web.Services.Description;
using System.Web.Services.Discovery;
//using Android.Database.Sqlite;
using Mono.Data.Sqlite;
using System.Data;
using System.IO;
using clsTabletCommon.ITPExternal;
using Mono.Interop;


namespace appBusinessFormBuilder
{
    public class clsTabletDB
    {
       
        public class TableNextId
        {
            string[] colHeaderNames = {"ID", "TypeName", "Counter" };
            string[] colHeaderTypes = {"[nvarchar](50) NULL", "[nvarchar](50) NULL", "[int] NULL" };
            string[] colHeaderBaseTypes = { "string", "string", "int" };
            LocalDB DB = new LocalDB();
            public string sTableName = "tblNextId";

            public bool CheckFullNextIdTable()
            {

                if (!DB.TableExists(sTableName))
                {

                    return DB.CreateTable(sTableName, colHeaderNames, colHeaderTypes);
                }
                else
                {
                    return true;
                }


            }

            public bool TableNextIdRecordExists(string sId, string sTypeName)
            {
                string sSQL = "Select * from " + sTableName + " where TypeName = '" + sTypeName + "' And ID = '" + sId + "'";
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

            public int GetNextIdRecord(string sId, string sTypeName)
            {
                string sSQL;
                string sRtnMsg = "";
                string[] colNames = new string[1];
                int iCounter;

                colNames[0] = "Counter";

                if (DB.TableExists(sTableName))
                {
                    sSQL = "Select Counter from " + sTableName + " where TypeName = '" + sTypeName + "' And ID = '" + sId + "'";
                    DataSet ds = new DataSet();
                    ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        iCounter = Convert.ToInt32(ds.Tables[0].Rows[0].ItemArray[0]);
                    }
                    else
                    {
                        iCounter = 0;
                    }

                    return iCounter;
                }
                else
                {
                    return 0;
                }
            }


            public bool SetNextIdRecord(string sId, string sTypeName, int iCounter)
            {
                string[] sItemValues = new string[3];
                sItemValues[0] = sId;
                sItemValues[1] = sTypeName;
                sItemValues[2] = iCounter.ToString();

                    if(TableNextIdRecordExists(sId, sTypeName))
                    {
                        return TableNextIdUpdateRecord(sId, sTypeName, iCounter);
                    }
                    else
                    {
                        return TableNextIdAddRecord(sItemValues);
                    }
            }

            public bool TableNextIdAddRecord(string[] sItemValues)
            {
                bool bRtn;

                bRtn = DB.AddRecord(sTableName, colHeaderNames, colHeaderBaseTypes, sItemValues);
                return bRtn;
            }

            public bool TableNextIdUpdateRecord(string sId, string sTypeName, int iCounter)
            {
                string sSQL;
                LocalDB DB = new LocalDB();
                string sRtnMsg = "";

                sSQL = "Update " + sTableName + " Set Counter=" + iCounter + " " +
                "where  TypeName='" + sTypeName + "' And ID = '" + sId + "'";

                return DB.ExecuteSQL(sSQL, ref sRtnMsg);
            }

        }

        public class GridUtils
        {
            enum SectionType { Form = 1, Header, Detail, Footer, HeaderRow, HeaderColumn, DetailRow, DetailColumn, FooterRow, FooterColumn, GridItem };
            enum ItemType { Label = 1, TextBox, TextArea, DropDown, Checkbox, RadioButton, Button, DatePicker, TimePicker, Image, ColumnHeader, RowHeader, ColumnDetail, RowDetail, ColumnFooter, RowFooter };
            enum VersionType { Free = 0, Base, Pro, Premium };

            LocalDB DB = new LocalDB();

            string[] sTableNames = {"tblForms","tblSections", "tblSectionType", "tblGridItems","tblItemType", "tblItemAttributes", "tblParameters", "tblVersionType", "tblColors", "tblReservedTables", "tblUserTables"};
            static int iTableNamesLength = 11;
            string[][] sColumns = new string[iTableNamesLength][];
            string[] sColumnsTable1 = {"ID","Name", "Description"}; //tblForms
            string[] sColumnsTable2 = { "ID", "FormId", "SectionType" }; //tblSections
            string[] sColumnsTable3 = { "ID", "Description" }; //tblSectionType
            string[] sColumnsTable4 = { "ID", "FormId", "SectionId", "ItemType", "RowId", "ColumnId", "ItemId" }; //tblGridItems
            string[] sColumnsTable5 = { "ID", "Description" }; //tblItemType
            string[] sColumnsTable6 = { "ID", "FormId", "ItemId", "SectionType", "ParameterId", "ParameterValue" }; //tblItemAttributes
            string[] sColumnsTable7 = { "ID", "SectionType", "ItemType", "ParameterName", "ParameterDescription", "ParameterType", "VersionType", "SortOrder", "DropdownSQL", "DefaultValue", "OnBlur", "ExtraButton" }; //tblParameters
            string[] sColumnsTable8 = { "ID", "Description", "MonthlyFee", "YearlyFee" }; //tblVersionType
            string[] sColumnsTable9 = { "ID", "Color"}; //tblColors
            string[] sColumnsTable10 = { "ID", "TableName" }; //tblReservedTables
            string[] sColumnsTable11 = { "ID", "TableName" }; //tblUserTables

            string[][] sColumnsNoAutoId = new string[iTableNamesLength][];
            string[] sColumnsTableNoAutoId1 = { "Name", "Description" }; //tblForms
            string[] sColumnsTableNoAutoId2 = { "FormId", "SectionType" }; //tblSections
            string[] sColumnsTableNoAutoId3 = { "ID", "Description" }; //tblSectionType
            string[] sColumnsTableNoAutoId4 = { "FormId", "SectionId", "ItemType", "RowId", "ColumnId", "ItemId" }; //tblGridItems
            string[] sColumnsTableNoAutoId5 = { "ID", "Description" }; //tblItemType
            string[] sColumnsTableNoAutoId6 = { "FormId", "ItemId", "SectionType", "ParameterId", "ParameterValue" }; //tblItemAttributes
            string[] sColumnsTableNoAutoId7 = { "ID", "SectionType", "ItemType", "ParameterName", "ParameterDescription", "ParameterType", "VersionType", "SortOrder", "DropdownSQL", "DefaultValue", "OnBlur", "ExtraButton" }; //tblParameters
            string[] sColumnsTableNoAutoId8 = { "ID", "Description", "MonthlyFee", "YearlyFee" }; //tblVersionType
            string[] sColumnsTableNoAutoId9 = { "Color" }; //tblColors
            string[] sColumnsTableNoAutoId10 = { "TableName" }; //tblReservedTables
            string[] sColumnsTableNoAutoId11 = { "TableName" }; //tblUserTables

            string[][] sTableColumnTypes = new string[iTableNamesLength][];
            string[] sTypesTable1 = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NULL", "[nvarchar](500) NULL" };
            string[] sTypesTable2 = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[int] NULL", "[int] NULL" };
            string[] sTypesTable3 = { "[int] NULL", "[nvarchar](50) NULL" };
            string[] sTypesTable4 = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[int] NULL", "[int] NULL", "[int] NULL", "[int] NULL", "[int] NULL", "[int] NULL" };
            string[] sTypesTable5 = { "[int] NULL", "[nvarchar](50) NULL" };
            string[] sTypesTable6 = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[int] NULL", "[int] NULL", "[int] NULL", "[int] NULL", "[nvarchar](1000) NULL" };
            string[] sTypesTable7 = { "[int] NULL", "[int] NULL", "[int] NULL", "[nvarchar](100) NULL", "[nvarchar](100) NULL", "[int] NULL", "[int] NULL", "[int] NULL", "[nvarchar](500) NULL", "[nvarchar](1000) NULL", "[nvarchar](100) NULL", "[nvarchar](100) NULL" };
            string[] sTypesTable8 = { "[int] NULL", "[nvarchar](50) NULL", "[float] NULL", "[float] NULL" };
            string[] sTypesTable9 = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](50) NULL" };
            string[] sTypesTable10 = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](250) NULL" };
            string[] sTypesTable11 = { "INTEGER PRIMARY KEY AUTOINCREMENT", "[nvarchar](250) NULL" };

            string[][] sTableColumnBaseTypes = new string[iTableNamesLength][];
            string[] sBaseTypesTable1 = { "autoincrement", "string", "string" };
            string[] sBaseTypesTable2 = { "autoincrement", "int", "int" };
            string[] sBaseTypesTable3 = { "int", "string" };
            string[] sBaseTypesTable4 = { "autoincrement", "int", "int", "int", "int", "int", "int" };
            string[] sBaseTypesTable5 = { "int", "string" };
            string[] sBaseTypesTable6 = { "autoincrement", "int", "int", "int", "int", "string" };
            string[] sBaseTypesTable7 = { "int", "int", "int", "string", "string", "int", "int", "int", "string", "string", "string", "string" };
            string[] sBaseTypesTable8 = { "int", "string", "float", "float" };
            string[] sBaseTypesTable9 = { "autoincrement", "string"};
            string[] sBaseTypesTable10 = { "autoincrement", "string" };
            string[] sBaseTypesTable11 = { "autoincrement", "string" };

            string[][] sTableColumnBaseTypesNoAutoId = new string[iTableNamesLength][];
            string[] sBaseTypesTableNoAutoId1 = { "string", "string" };
            string[] sBaseTypesTableNoAutoId2 = { "int", "int" };
            string[] sBaseTypesTableNoAutoId3 = { "int", "string" };
            string[] sBaseTypesTableNoAutoId4 = { "int", "int", "int", "int", "int", "int" };
            string[] sBaseTypesTableNoAutoId5 = { "int", "string" };
            string[] sBaseTypesTableNoAutoId6 = { "int", "int", "int", "int", "string" };
            string[] sBaseTypesTableNoAutoId7 = { "int", "int", "int", "string", "string", "int", "int", "int", "string", "string", "string", "string" };
            string[] sBaseTypesTableNoAutoId8 = { "int", "string", "float", "float" };
            string[] sBaseTypesTableNoAutoId9 = { "string" };
            string[] sBaseTypesTableNoAutoId10 = { "string" };
            string[] sBaseTypesTableNoAutoId11 = { "string" };

            int[] iSectionTypes = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            string[] sSectionTypeDescriptions = { "Form", "Header", "Detail", "Footer", "Header Row", "Header Column", "Detail Row", "Detail Column", "Footer Row", "Footer Column", "Grid Item" };

            int[] iItemTypes = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            public string[] sItemTypeDescriptions = { "Label", "TextBox", "TextArea", "DropDown", "Checkbox", "RadioButton", "Button", "DatePicker", "TimePicker", "Image" };

            public string[] sColors = {"Antique White","Aqua", "Beige", "Black", "Blue", "Brown", "Coral", "Crimson", "Cyan", "Dark Blue", "Dark Gray", "Dark Green", "Dark Orange", "Dark Red", "Dark Violet",
                                       "Deep Pink", "Ghost White", "Gold" ,"Gray", "Green", "Hot Pink", "Indigo", "Lavender", "Light Blue", "Light Cyan", "Light Gray", "Light Green", "Light Pink", "Light Yellow",
                                       "Lime", "Magenta", "Maroon", "Navy", "Olive", "Orange", "Pale Green", "Pink", "Plum", "Purple", "Red", "Royal Blue", "Salmon", "Silver", "Sky Blue", "Slate Blue", "Slate Gray",
                                       "Snow", "Steel Blue", "Tan", "Teal", "Turquoise", "Violet", "Wheat", "White", "White Smoke", "Yellow", "Yellow Green"};

            /***********************************************************************************************************************/
            /*                          PARAMETER TABLE                                                                            */
            /* It is very important that you have the same number of items per array per section otherwise things get out of order */
            /***********************************************************************************************************************/

            //Line 1 - for Forms
            //Line 2 - for Headers section
            //Line 3 - for Details section
            //Line 4 - for Footers section
            //Line 5 - for Header Rows
            //Line 6 - for Header Columns
            //Line 7 - for Detail Rows
            //Line 8 - for Detail Columns
            //Line 9 - for Footer Rows
            //Line 10 - for Footer Columns
            //Line 11 - for Grid Items Label
            //Line 12 - for Grid Items Textbox
            //Line 13 - for Grid Items Textarea
            //Line 14 - for Grid Items Dropdown
            //Line 15 - for Grid Items Checkbox
            //Line 16 - for Grid Items RadioGroup/Radiobutton
            //Line 17 - for Grid Items Button
            //Line 18 - for Grid Items Date Picker
            //Line 19 - for Grid Items Time Picker
            //Line 20 - for Grid Items Image

            //Allow for up to 100 parameters per section or type
            int[] iParameterIds = { 1, 2, 
                                    101, 102, 103, 104, 105,
                                    201, 202, 203, 204, 205, 206, 207,
                                    301, 302, 303, 304, 305,
                                    401, 402,
                                    501, 502,
                                    601, 602,
                                    701, 702,
                                    801, 802,
                                    901, 902,
                                    1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 
                                    1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1115, 1116, 1117, 1118, 1119, 1120, 1121, 1122, 1123,
                                    1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211, 1212, 1213, 1214, 1215, 1216, 1217, 1218, 1219, 1220, 1221, 1222, 1223,
                                    1301, 1302, 1303, 1304, 1305, 1306, 1307, 1308, 1309, 1310, 1311, 1312, 1313, 1314, 1315, 1316, 1317, 1318, 1319, 1320, 1321, 1322, 1323, 1324,
                                    1401, 1402, 1403, 1404, 1405, 1406, 1407, 1408, 1409, 1410, 1411, 1412, 1413, 1414, 1415, 1416, 1417, 1418, 1419, 1420, 1421, 1422, 1423, 1424,
                                    1501, 1502, 1503, 1504, 1505, 1506, 1507, 1508, 1509, 1510, 1511, 1512, 1513, 1514, 1515, 1516, 1517, 1518, 1519, 1520, 1521, 1522, 1523, 1524, 1525, 1526, 1527,
                                    1601, 1602, 1603, 1604, 1605, 1606, 1607, 1608, 1609, 1610, 1611, 1612, 1613, 1614, 1615, 1616, 1617, 1618, 1619, 1620,
                                    1701, 1702, 1703, 1704, 1705, 1706, 1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1717, 1718, 1719, 1720, 1721, 1722,
                                    1801, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810, 1811, 1812, 1813, 1814, 1815, 1816, 1817, 1818, 1819, 1820, 1821, 1822,
                                    1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911, 1912, 1913
                                  };

            int[] iParameterSectionTypes = { 1,1,
                                             2,2,2,2,2, 
                                             3,3,3,3,3,3,3,
                                             4,4,4,4,4,
                                             5,5,
                                             6,6,
                                             7,7,
                                             8,8,
                                             9,9,
                                             10,10,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
                                             11,11,11,11,11,11,11,11,11,11,11,11,11
                                           };
            //The parameter type is whether it is a textbox, drop down etc and is really only applicable to grid items, rows and columns. 
            //For all other sections we only have one set of parameters so make this zero (0) for anything other than grid items, rows and columns
            int[] iParameterItemTypes = { 0,0,
                                          0,0,0,0,0,
                                          0,0,0,0,0,0,0,
                                          0,0,0,0,0,
                                          0,0,
                                          0,0,
                                          0,0,
                                          0,0,
                                          0,0,
                                          0,0,
                                          1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, //This is a label
                                          2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, //This is a textbox
                                          3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3, //This is a textarea
                                          4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4, //This is a dropdown
                                          5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5, //This is a checkbox
                                          6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6, //This is a radio group
                                          7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7, //This is a button
                                          8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,  //This is a date picker
                                          9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,  //This is a time picker
                                          10,10,10,10,10,10,10,10,10,10,10,10,10 //This is an image
                                        };
            string[] sParameterNames = { "FormSQL", "FormAutoSave", 
                                         "Rows", "Columns", "Gridlines", "GridlineColor", "GridlineWeight",
                                         "Rows", "Columns", "Gridlines", "GridlineColor", "GridlineWeight", "Repeatable", "RowsPerPage",
                                         "Rows", "Columns", "Gridlines", "GridlineColor", "GridlineWeight",
                                         "Height", "Visible",
                                         "Width", "Visible",
                                         "Height", "Visible",
                                         "Width", "Visible",
                                         "Height", "Visible",
                                         "Width", "Visible",
                                         "BoundColumn", "Value", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor",
                                         "BoundColumn", "Value", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnGotFocus", "OnLostFocus", "OnChange",
                                         "BoundColumn", "Value", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnGotFocus", "OnLostFocus", "OnChange",
                                         "BoundColumn", "DropdownSQL", "Value", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnGotFocus", "OnLostFocus", "OnChange",
                                         "BoundColumn", "CheckBoxLabel", "Value", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnGotFocus", "OnLostFocus", "OnClick",
                                         "BoundColumn", "Value", "ItemLabels", "ItemValues", "Orientation", "HighlightColor", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnGotFocus", "OnLostFocus", "OnChange",
                                         "ButtonLabel", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom","BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnClick",
                                         "BoundColumn", "Value", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom",  "EnableDateField", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnChange",
                                         "BoundColumn", "Value", "BackgroundColor", "ColumnSpan", "FontSize", "Font", "Italic", "Bold", "TextColor", "TextAlign", "TextVertAlignment", "TextPaddingLeft", "TextPaddingRight", "TextPaddingTop", "TextPaddingBottom",  "EnableTimeField", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnChange",
                                         "BoundColumn", "ImageFile", "ColumnSpan", "ImageAlign", "ImageVertAlignment", "BorderLeft", "BorderRight", "BorderTop", "BorderBottom", "BorderColor", "OnGotFocus", "OnLostFocus", "OnClick"
                                       };
            string[] sParameterDescriptions = { "SQL or Query", "AutoSave", 
                                                "Rows", "Columns", "Gridlines", "Gridline Color", "Gridline Weight", 
                                                "Rows", "Columns", "Gridlines", "Gridline Color", "Gridline Weight", "Repeatable", "Total Rows Per Page",
                                                "Rows", "Columns", "Gridlines", "Gridline Color", "Gridline Weight",
                                                "Row Height (px)", "Visible",
                                                "Column Width (px)", "Visible",
                                                "Row Height (px)", "Visible",
                                                "Column Width (px)", "Visible",
                                                "Row Height (px)", "Visible",
                                                "Column Width (px)", "Visible",
                                                "Bound Column", "Value", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color",
                                                "Bound Column", "Value", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnGotFocus", "OnLostFocus", "OnChange",
                                                "Bound Column", "Value", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnGotFocus", "OnLostFocus", "OnChange",
                                                "Bound Column", "Dropdown SQL", "Value", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnGotFocus", "OnLostFocus", "OnChange",
                                                "Bound Column", "Label", "Value", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnGotFocus", "OnLostFocus", "OnClick",
                                                "Bound Column", "Value", "Item Labels", "Item Values", "Orientation", "Highlight Color", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Radio Group Align", "Radio Group Vertical Alignment", "Radio Group Padding Left", "Radio Group Padding Right", "Radio Group Padding Top", "Radio Group Padding Bottom", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnGotFocus", "OnLostFocus", "OnChange",
                                                "Label", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnClick",
                                                "Bound Column", "Value", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Enable Date Field", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnChange",
                                                "Bound Column", "Value", "Background Color", "Column Span", "Font Size", "Font", "Italic", "Bold", "Text Color", "Text Align", "Text Vertical Alignment", "Text Padding Left", "Text Padding Right", "Text Padding Top", "Text Padding Bottom", "Enable Time Field", "Border Left", "Border Right", "Border Top", "Border Bottom",  "Border Color", "OnChange",
                                                "Bound Column", "Image File", "Column Span", "Image Align", "Image Vertical Alignment", "Border Left", "Border Right", "Border Top", "Border Bottom", "Border Color", "OnGotFocus", "OnLostFocus", "OnClick"
                                              };
            //This parameter type is to how this line will show in the detail popup. Most cases will be a textbox or a drop down
            int[] iParameterTypes = { 2, 4,
                                      2, 2, 4, 4, 4,
                                      2, 2, 4, 4, 4, 4, 4,
                                      2, 2, 4, 4, 4,
                                      2, 4,
                                      2, 4,
                                      2, 4,
                                      2, 4,
                                      2, 4,
                                      2, 4,
                                      400, 2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                      400, 2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 2, 2,
                                      400, 2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 2, 2,
                                      400, 2, 2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 2, 2,
                                      400, 2, 2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 2, 2,
                                      400, 2, 2, 2, 4, 4, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 2, 2,
                                      2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2,
                                      400, 2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2,
                                      400, 2, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2,
                                      400, 2, 2, 4, 4, 4, 4, 4, 4, 4, 2, 2, 2
                                    };
            int[] iVersionAvailable = { 0, 0, 
                                        0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0, 2, 2,
                                        0, 0, 0, 0, 0,
                                        0, 0,
                                        0, 0,
                                        0, 0,
                                        0, 0,
                                        0, 0,
                                        0, 0,
                                        0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                                      };
            int[] iParameterOrder = { 1, 2,  
                                      1, 2, 3, 4, 5,
                                      1, 2, 3, 4, 5, 6, 7,
                                      1, 2, 3, 4, 5,
                                      1, 2,
                                      1, 2,
                                      1, 2,
                                      1, 2,
                                      1, 2,
                                      1, 2,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                                      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
                                    };
            string[] sDropdownSQL = { "", "Yes;No", 
                                      "", "", "Yes;No", "select Color from tblColors", "2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px",
                                      "", "", "Yes;No", "select Color from tblColors", "2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "Yes;No", "1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20",
                                      "", "", "Yes;No", "select Color from tblColors", "2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px",
                                      "", "Yes;No",
                                      "", "Yes;No",
                                      "", "Yes;No",
                                      "", "Yes;No",
                                      "", "Yes;No",
                                      "", "Yes;No",
                                      "", "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px", "select Color from tblColors", 
                                      "", "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","","","",   
                                      "", "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","","","",   
                                      "", "", "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","","","",   
                                      "", "", "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","","","",   
                                      "", "", "", "", "Horizontal;Vertical", "select Color from tblColors", "select Color from tblColors","", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","","","",   
                                      "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px", "select Color from tblColors", "",
                                      "", "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "Yes;No","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","",
                                      "", "", "select Color from tblColors", "", "8pt;9pt;10pt;11pt;12pt;14pt;16pt;18pt;20pt;22pt;24pt;26pt;28pt;30pt;32pt","Default;Sans Serif;Serif;Monospace", "Yes;No","Yes;No","select Color from tblColors", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px","0px;1px;2px;3px;4px;5px;6px;7px;8px;9px;10px;11px;12px", "Yes;No","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","",
                                      "", "", "", "Left;Center;Right", "Top;Center;Bottom", "0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","0px;1px;2px;3px;4px;5px;6px;7px;8px","select Color from tblColors","","",""  
                                    };

            string[] sDefaultValue = { "", "Yes", 
                                       "", "", "Yes", "Light Gray", "3px",
                                       "", "", "Yes", "Light Gray", "3px", "No", "10",
                                       "", "", "Yes", "Light Gray", "3px",
                                       "", "Yes",
                                       "", "Yes",
                                       "", "Yes",
                                       "", "Yes",
                                       "", "Yes",
                                       "", "Yes",
                                       "", "", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "0px", "0px", "0px", "0px", "Black",
                                       "", "", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "0px", "0px", "0px", "0px", "Black", "", "", "", 
                                       "", "", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "0px", "0px", "0px", "0px", "Black", "", "", "", 
                                       "", "", "", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "0px", "0px", "0px", "0px", "Black", "", "", "",
                                       "", "", "", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "0px", "0px", "0px", "0px", "Black", "", "", "", 
                                       "", "", "", "", "Horizontal", "Yellow", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "0px", "0px", "0px", "0px", "Black", "", "", "",
                                       "", "Light Grey","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "0px", "0px", "0px", "0px", "Black", "",
                                       "", "", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "No", "0px", "0px", "0px", "0px", "Black", "",
                                       "", "", "White","1", "10pt", "", "No", "No", "Black", "Left", "Center", "2px", "2px", "2px", "2px", "No", "0px", "0px", "0px", "0px", "Black", "",
                                       "", "","1", "Center", "Center", "0px", "0px", "0px", "0px", "Black", "", "", ""
                                     };

            string[] sOnBlur = { "ValidateSQL(this);", "", 
                                 "", "", "", "", "",
                                 "", "", "", "", "", "", "",
                                 "", "", "", "", "",
                                 "", "",
                                 "", "",
                                 "", "",
                                 "", "",
                                 "", "",
                                 "", "",
                                 "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                                 "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "ValidateMacro(this);","ValidateMacro(this);","ValidateMacro(this);",
                                 "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "ValidateMacro(this);","ValidateMacro(this);","ValidateMacro(this);",
                                 "", "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "ValidateMacro(this);","ValidateMacro(this);","ValidateMacro(this);",
                                 "", "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "ValidateMacro(this);","ValidateMacro(this);","ValidateMacro(this);",
                                 "", "", "CheckRadioItems(this)", "CheckRadioItems(this)", "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "ValidateMacro(this);","ValidateMacro(this);","ValidateMacro(this);",
                                 "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","ValidateMacro(this);",
                                 "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "ValidateMacro(this);",
                                 "", "", "", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "ValidateMacro(this);",
                                 "", "CheckFileExits(this.value)", "CheckColumnSpan(this);", "", "", "", "", "", "", "", "ValidateMacro(this);", "ValidateMacro(this);", "ValidateMacro(this);"
                                };

            string[] sExtraButton = {"OpenSQLManager(this);", "", 
                                     "", "", "", "", "",
                                     "", "", "", "", "", "", "",
                                     "", "", "", "", "",
                                     "", "",
                                     "", "",
                                     "", "",
                                     "", "",
                                     "", "",
                                     "", "",
                                     "", "", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                                     "", "", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "OpenMacroManager(this);","OpenMacroManager(this);","OpenMacroManager(this);",
                                     "", "", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "OpenMacroManager(this);","OpenMacroManager(this);","OpenMacroManager(this);",
                                     "", "OpenSQLManager(this);", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "OpenMacroManager(this);","OpenMacroManager(this);","OpenMacroManager(this);",
                                     "", "", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "OpenMacroManager(this);","OpenMacroManager(this);","OpenMacroManager(this);",
                                     "", "OpenMacroManager(this);", "", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "OpenMacroManager(this);","OpenMacroManager(this);","OpenMacroManager(this);",
                                     "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","OpenMacroManager(this);",
                                     "", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "OpenMacroManager(this);",
                                     "", "OpenMacroManager(this);", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "OpenMacroManager(this);",
                                     "", "OpenFileManager(this);", "", "", "", "", "", "", "", "", "OpenMacroManager(this);", "OpenMacroManager(this);", "OpenMacroManager(this);"
                                    };
            /***********************************************************************************************************************/

            int[] iVersionTypes = { 1, 2, 3, 4 };
            string[] sVersionDescriptions = { "Free", "Base", "Pro", "Premium" };
            double[] dVersionMonthly = { 0, 10, 20, 30 };
            double[] dVersionYearly = { 0, 100, 200, 270 };

            //Table for holding which device has which version. A device ID can change on a factory reset etc but 
            //typically this table would hold one record
            string sVersionTableName = "tblVersion";
            string[] sVersionTableColumns = { "DeviceId", "VersionType" };
            string[] sVersionTableColumnTypes = { "[nvarchar](250) NULL", "[int] NULL" };
            string[] sVersionTableColumnBaseTypes = { "string", "int" };


            public void FillBaseTableArrays()
            {
                sColumns[0] = sColumnsTable1;
                sColumns[1] = sColumnsTable2;
                sColumns[2] = sColumnsTable3;
                sColumns[3] = sColumnsTable4;
                sColumns[4] = sColumnsTable5;
                sColumns[5] = sColumnsTable6;
                sColumns[6] = sColumnsTable7;
                sColumns[7] = sColumnsTable8;
                sColumns[8] = sColumnsTable9;
                sColumns[9] = sColumnsTable10;
                sColumns[10] = sColumnsTable11;

                sTableColumnTypes[0] = sTypesTable1;
                sTableColumnTypes[1] = sTypesTable2;
                sTableColumnTypes[2] = sTypesTable3;
                sTableColumnTypes[3] = sTypesTable4;
                sTableColumnTypes[4] = sTypesTable5;
                sTableColumnTypes[5] = sTypesTable6;
                sTableColumnTypes[6] = sTypesTable7;
                sTableColumnTypes[7] = sTypesTable8;
                sTableColumnTypes[8] = sTypesTable9;
                sTableColumnTypes[9] = sTypesTable10;
                sTableColumnTypes[10] = sTypesTable11;

                sTableColumnBaseTypes[0] = sBaseTypesTable1;
                sTableColumnBaseTypes[1] = sBaseTypesTable2;
                sTableColumnBaseTypes[2] = sBaseTypesTable3;
                sTableColumnBaseTypes[3] = sBaseTypesTable4;
                sTableColumnBaseTypes[4] = sBaseTypesTable5;
                sTableColumnBaseTypes[5] = sBaseTypesTable6;
                sTableColumnBaseTypes[6] = sBaseTypesTable7;
                sTableColumnBaseTypes[7] = sBaseTypesTable8;
                sTableColumnBaseTypes[8] = sBaseTypesTable9;
                sTableColumnBaseTypes[9] = sBaseTypesTable10;
                sTableColumnBaseTypes[10] = sBaseTypesTable11;
            }

            public bool CreateBaseTables()
            {
                int i;
                FillBaseTableArrays();

                for (i = 0; i < iTableNamesLength; i++)
                {
                    DB.CreateTable(sTableNames[i], sColumns[i], sTableColumnTypes[i]);
                }

                //Now fill the tables with records where appropriate
                FillSectionTypeTable();
                FillItemTypeTable();
                FillParameterTable();
                FillVersionTable();
                FillColorsTable();
                FillReservedTablesTable();

                return true;
            }

            public void FillSectionTypeTable()
            {
                int i;
                string[] sValues = new string[2];
                string sSQL;
                string sRtnMsg = "";

                for (i = 0; i < iSectionTypes.Length; i++)
                {
                    sValues[0] = iSectionTypes[i].ToString();
                    sValues[1] = sSectionTypeDescriptions[i];

                    sSQL = "Select * from " + sTableNames[2] + " where ID = " + sValues[0];
                    if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                    {
                        DB.AddRecord(sTableNames[2], sColumnsTable3, sBaseTypesTable3, sValues);
                    }
                }
            }

            public void FillItemTypeTable()
            {
                int i;
                string[] sValues = new string[2];
                string sSQL;
                string sRtnMsg = "";

                for (i = 0; i < iItemTypes.Length; i++)
                {
                    sValues[0] = iItemTypes[i].ToString();
                    sValues[1] = sItemTypeDescriptions[i];

                    sSQL = "Select * from " + sTableNames[4] + " where ID = " + sValues[0];
                    if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                    {
                        DB.AddRecord(sTableNames[4], sColumnsTable5, sBaseTypesTable5, sValues);
                    }
                }
            }

            public void FillColorsTable()
            {
                int i;
                string[] sValues = new string[1];
                string sSQL;
                string sRtnMsg = "";

                for (i = 0; i < sColors.Length; i++)
                {
                    sValues[0] = sColors[i];

                    sSQL = "Select * from " + sTableNames[8] + " where Color = '" + sValues[0] + "'"; 
                    if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                    {
                        DB.AddRecord(sTableNames[8], sColumnsTableNoAutoId9, sBaseTypesTableNoAutoId9, sValues);
                    }
                }
            }


            public void FillReservedTablesTable()
            {
                int i;
                string[] sValues = new string[1];
                string sSQL;
                string sRtnMsg = "";

                for (i = 0; i < sTableNames.Length; i++)
                {
                    sValues[0] = sTableNames[i];
                    sSQL = "Select * from " + sTableNames[9] + " where TableName = '" + sValues[0] + "'";
                    if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                    {
                        DB.AddRecord(sTableNames[9], sColumnsTableNoAutoId10, sBaseTypesTableNoAutoId10, sValues);
                    }
                }
            }

            public void FillParameterTable()
            {
                int i;
                string[] sValues = new string[12];
                string sSQL;
                string sRtnMsg = "";

                for (i = 0; i < iParameterIds.Length; i++)
                {
                    sValues[0] = iParameterIds[i].ToString();
                    sValues[1] = iParameterSectionTypes[i].ToString();
                    sValues[2] = iParameterItemTypes[i].ToString();
                    sValues[3] = sParameterNames[i].ToString();
                    sValues[4] = sParameterDescriptions[i].ToString();
                    sValues[5] = iParameterTypes[i].ToString();
                    sValues[6] = iVersionAvailable[i].ToString();
                    sValues[7] = iParameterOrder[i].ToString();
                    sValues[8] = sDropdownSQL[i].ToString();
                    sValues[9] = sDefaultValue[i].ToString();
                    sValues[10] = sOnBlur[i].ToString();
                    sValues[11] = sExtraButton[i].ToString();

                    sSQL = "Select * from " + sTableNames[6] + " where ID = " + sValues[0];
                    if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                    {
                        DB.AddRecord(sTableNames[6], sColumnsTable7, sBaseTypesTable7, sValues);
                    }
                    else
                    {
                        DB.UpdateRecord(sTableNames[6], sColumnsTable7, sBaseTypesTable7, sValues, "ID = " + sValues[0]);
                    }
                }
            }

            public void FillVersionTable()
            {
                int i;
                string[] sValues = new string[4];
                string sSQL;
                string sRtnMsg = "";

                for (i = 0; i < iVersionTypes.Length; i++)
                {
                    sValues[0] = iVersionTypes[i].ToString();
                    sValues[1] = sVersionDescriptions[i];
                    sValues[2] = dVersionMonthly[i].ToString();
                    sValues[3] = dVersionYearly[i].ToString();

                    sSQL = "Select * from " + sTableNames[7] + " where ID = " + sValues[0];
                    if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                    {
                        DB.AddRecord(sTableNames[7], sColumnsTable8, sBaseTypesTable8, sValues);
                    }
                }
            }

            public ArrayList GetDetailDialogItems(int iFormId, int iSectionType, int iItemId, int iCellId, int iSectionId)
            {
                ArrayList rtnArray = new ArrayList();
                string sSQL;
                string[] colNames = { "ParameterId", "ParameterName", "ParameterDescription", "ParameterType", "ParameterValue", "DropdownSQL", "OnBlur", "ExtraButton" };
                string sRtnMsg = "";
                ArrayList arrId = new ArrayList();
                ArrayList arrName = new ArrayList();
                ArrayList arrDesc = new ArrayList();
                ArrayList arrType = new ArrayList();
                ArrayList arrValue = new ArrayList();
                ArrayList arrDropdownSQL = new ArrayList();
//                ArrayList arrDefaultValue = new ArrayList();
                ArrayList arrOnBlur = new ArrayList();
                ArrayList arrExtraButton = new ArrayList();
                try
                {
                    if (DB.TableExists(sTableNames[6]))
                    {
                        if (iItemId <= 0 && iSectionType != (int)SectionType.HeaderRow && iSectionType != (int)SectionType.HeaderColumn && iSectionType != (int)SectionType.DetailRow && iSectionType != (int)SectionType.DetailColumn && iSectionType != (int)SectionType.FooterRow && iSectionType != (int)SectionType.FooterColumn)
                        {
                            sSQL = "select P.Id as ParameterId, P.ParameterName, P.ParameterDescription, P.ParameterType, ifnull(I.ParameterValue, ifnull(P.DefaultValue,'')) as ParameterValue, " +
                                   "ifnull(P.DropdownSQL,'') as DropdownSQL, ifnull(P.OnBlur,'') as OnBlur, ifnull(P.ExtraButton,'') as ExtraButton, SortOrder  " +
                                   "from " + sTableNames[6] + " P " +
                                   "left outer join " + sTableNames[5] + " I " +
                                   "on P.Id = I.ParameterId " +
                                   "and P.SectionType = I.SectionType " +
                                   "and I.FormId = " + iFormId + " " +
                                   "where P.SectionType = " + iSectionType + " ";

                            //Some extra bits for the form dialog
                            if (iSectionType == (int)SectionType.Form)
                            {
                                sSQL += "UNION ALL " +
                                        "Select -99, 'FormName', 'Name', " + (int)ItemType.Label + ",Name, '', 'ValidateFormName(this)', '',-99 from " + sTableNames[0] + " where Id = " + iFormId + " " +
                                        "UNION ALL " +
                                        "Select -98, 'FormDescription', 'Description', " + (int)ItemType.TextArea + ",Description, '', 'ValidateFormName(this)', '', -98 from " + sTableNames[0] + " where Id = " + iFormId + " ";
                            }

                            sSQL += "Order by SortOrder";

                        }
                        else
                        {
                            int iUniqueItemId = GetGridItemId(iFormId, iSectionId, iCellId, ref sRtnMsg);
                            switch (iSectionType)
                            {



                                case (int)SectionType.HeaderRow:
                                case (int)SectionType.HeaderColumn:
                                case (int)SectionType.DetailRow:
                                case (int)SectionType.DetailColumn:
                                case (int)SectionType.FooterRow:
                                case (int)SectionType.FooterColumn:
                                    sSQL = "select P.Id as ParameterId, P.ParameterName, P.ParameterDescription, P.ParameterType, ifnull(I.ParameterValue, ifnull(P.DefaultValue,'')) as ParameterValue, " +
                                           "ifnull(P.DropdownSQL,'') as DropdownSQL, ifnull(P.OnBlur,'') as OnBlur, ifnull(P.ExtraButton,'') as ExtraButton  " +
                                           "from " + sTableNames[6] + " P " +
                                           "left outer join " + sTableNames[5] + " I " +
                                           "on P.Id = I.ParameterId " +
                                           "and P.SectionType = I.SectionType " +
                                           "and I.FormId = " + iFormId + " " +
                                           "and I.ItemId = " + iUniqueItemId + " " +
                                           "where P.SectionType = " + iSectionType + " " +
                                           "Order by SortOrder";
                                    break;
                                default:
                                    sSQL = "select P.Id as ParameterId, P.ParameterName, P.ParameterDescription, P.ParameterType, ifnull(I.ParameterValue, ifnull(P.DefaultValue,'')) as ParameterValue, " +
                                           "ifnull(P.DropdownSQL,'') as DropdownSQL, ifnull(P.OnBlur,'') as OnBlur, ifnull(P.ExtraButton,'') as ExtraButton  " +
                                           "from " + sTableNames[6] + " P " +
                                           "left outer join " + sTableNames[5] + " I " +
                                           "on P.Id = I.ParameterId " +
                                           "and P.SectionType = I.SectionType " +
                                           "and I.FormId = " + iFormId + " " +
                                           "and I.ItemId = " + iUniqueItemId + " " +
                                           "where P.SectionType = " + iSectionType + " " +
                                           "and P.ItemType = " + iItemId + " " +
                                           "Order by SortOrder";
                                    break;
                            }
                        }

                        DataSet ds = new DataSet();
                        ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);

                        for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                        {
                            int iId = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[0]);
                            string sParameterName = ds.Tables[0].Rows[i].ItemArray[1].ToString();
                            string sParameterDesc = ds.Tables[0].Rows[i].ItemArray[2].ToString();
                            int iParamType = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[3]);
                            string sParameterValue = ds.Tables[0].Rows[i].ItemArray[4].ToString();
                            string sDropdownSQL = ds.Tables[0].Rows[i].ItemArray[5].ToString();
                            string sOnBlur = ds.Tables[0].Rows[i].ItemArray[6].ToString();
                            string sExtraButton = ds.Tables[0].Rows[i].ItemArray[7].ToString();

                            arrId.Add(iId);
                            arrName.Add(sParameterName);
                            arrDesc.Add(sParameterDesc);
                            arrType.Add(iParamType);
                            arrValue.Add(sParameterValue);
                            arrDropdownSQL.Add(sDropdownSQL);
                            arrOnBlur.Add(sOnBlur);
                            arrExtraButton.Add(sExtraButton);
                        }

                        rtnArray.Add(arrId);
                        rtnArray.Add(arrName);
                        rtnArray.Add(arrDesc);
                        rtnArray.Add(arrType);
                        rtnArray.Add(arrValue);
                        rtnArray.Add(arrDropdownSQL);
                        rtnArray.Add(arrOnBlur);
                        rtnArray.Add(arrExtraButton);
                        return rtnArray;
                    }
                    else
                    {
                        return rtnArray;
                    }
                }
                catch (Exception ex)
                {
                    rtnArray.Clear();
                    rtnArray.Add("Failure");
                    rtnArray.Add(ex.Message.ToString());
                    return rtnArray;
                }
                

            }

            public bool SaveItemAttribute(int iFormId, int iItemId, int iSectionType, int iParameterId, string sParameterValue, ref string sRtnMsg)
            {
                try
                {
                    string sSQL;
                    bool bSQL;
                    string[] sValues = new string[5];
                    sValues[0] = iFormId.ToString();
                    sValues[1] = iItemId.ToString();
                    sValues[2] = iSectionType.ToString();
                    sValues[3] = iParameterId.ToString();
                    sValues[4] = sParameterValue.ToString();

                    sSQL = "Select * from " + sTableNames[5] + " where FormId = " + sValues[0] +  " and ItemId = " + sValues[1] + " and SectionType = " + sValues[2] + " and ParameterId = " + sValues[3];
                    if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                    {
                        bSQL = DB.AddRecord(sTableNames[5], sColumnsTableNoAutoId6, sBaseTypesTableNoAutoId6, sValues);
                    }
                    else
                    {
                        bSQL = DB.UpdateRecord(sTableNames[5], sColumnsTableNoAutoId6, sBaseTypesTableNoAutoId6, sValues, "FormId = " + sValues[0] +  " and ItemId = " + sValues[1] + " and SectionType = " + sValues[2] + " and ParameterId = " + sValues[3]);
                    }

                    return bSQL;
                }
                catch(Exception ex)
                {
                    sRtnMsg = ex.Message.ToString();
                    return false;
                }

            }

            public string GetItemAttribute(int iFormId, int iSectionType, int iItemId, string sParameterName, ref string sRtnMsg)
            {
                string sSQL;
                string[] colNames = {"ParameterValue"};
                string sParameterValue = "";

                sSQL = "Select ParameterValue from " + sTableNames[5] + " I, " + sTableNames[6] + " P " +
                       "where P.ParameterName = '" + sParameterName + "' " +
                       "and P.Id = I.ParameterId " +
                       "and I.FormId = " + iFormId + " " +
                       "and I.SectionType = " + iSectionType + " " +
                       "and I.ItemId = " + iItemId;

                DataSet ds = new DataSet();
                ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);
                if (ds.Tables.Count > 0)
                {
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        sParameterValue = ds.Tables[0].Rows[0].ItemArray[0].ToString();
                    }
                }
                return sParameterValue;
            }

            public ArrayList GetGridInfo(int iFormId, int iSectionType)
            {
                ArrayList rtnArray = new ArrayList();
                string sSQL;
                string[] colNames = { "ParameterId", "ParameterValue", "ParameterName"};
                string sAllowedNames = "'Rows', 'Columns', 'Gridlines', 'GridBackgroundColor', 'GridlineColor', 'GridlineWeight' ";
                string sRtnMsg = "";
                ArrayList arrId = new ArrayList();
                ArrayList arrValue = new ArrayList();
                ArrayList arrParameterName = new ArrayList();
                try
                {
                    if (DB.TableExists(sTableNames[6]))
                    {
                        sSQL = "select I.ParameterId as ParameterId, I.ParameterValue, P.ParameterName " +
                                   "from " + sTableNames[5] + " I, " +  sTableNames[6] + " P " +
                                   "where I.FormId = " + iFormId + " and I.SectionType = " + iSectionType + " " +
                                   "and I.SectionType = P.SectionType " +
                                   "and I.ParameterId = P.Id " +
                                   "and P.ParameterName in (" + sAllowedNames + ")";

                        DataSet ds = new DataSet();
                        ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);

                        for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                        {
                            int iId = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[0]);
                            string sParameterValue = ds.Tables[0].Rows[i].ItemArray[1].ToString();
                            string sParameterName = ds.Tables[0].Rows[i].ItemArray[2].ToString();

                            arrId.Add(iId);
                            arrValue.Add(sParameterValue);
                            arrParameterName.Add(sParameterName);
                        }

                        if (arrId.Count > 0)
                        {
                            rtnArray.Add(arrId);
                            rtnArray.Add(arrValue);
                            rtnArray.Add(arrParameterName);
                        }
                        return rtnArray;
                    }
                    else
                    {
                        return rtnArray;
                    }
                }
                catch (Exception ex)
                {
                    rtnArray.Clear();
                    rtnArray.Add("Failure");
                    rtnArray.Add(ex.Message.ToString());
                    return rtnArray;
                }


            }

            public bool CopyMatchingItemAttributes(int iFormId, int iSectionId, int iRowId, int iColumnId, int iOldItemTypeId, int iNewItemTypeId)
            {
                string sSQL;
                int iItemId;
                string sRtnMsg = "";

                iItemId = GetGridItemId(iFormId, iSectionId, iRowId, iColumnId, ref sRtnMsg);

                sSQL =  "insert into tblItemAttributes (FormId, ItemId, Sectiontype, ParameterId, ParameterValue) " +
                        "select I.FormId, I.ItemId, I.SectionType, P_New.Id, I.ParameterValue " +
                        "from tblParameters P_Old, tblParameters P_New, tblItemAttributes I " +
                        "where P_Old.SectionType = P_New.SectionType " +
                        "and P_Old.ParameterName = P_New.ParameterName " +
                        "and P_Old.ItemType <> P_New.ItemType "  +
                        "and P_Old.ItemType = " + iOldItemTypeId + " " +
                        "and P_New.ItemType = " + iNewItemTypeId + " " +
                        "and I.ParameterId = P_Old.Id " +
                        "and I.ItemId = " + iItemId + " " +
                        "and I.SectionType = P_Old.SectionType";


                return DB.ExecuteSQL(sSQL, ref sRtnMsg);
            }

            public bool DeleteAllItemAttributes(int iFormId, int iSectionId, int iRowId, int iColumnId)
            {
                string sSQL;
                int iItemId;
                string sRtnMsg = "";

                iItemId = GetGridItemId(iFormId, iSectionId, iRowId, iColumnId, ref sRtnMsg);

                sSQL = "Delete from " + sTableNames[5] + " where FormId = " + iFormId + " and SectionType = " + iSectionId + " and ItemId = " + iItemId;

                return DB.ExecuteSQL(sSQL, ref sRtnMsg);
            }

            public bool DeleteAllItemAttributesOfType(int iFormId, int iSectionId, int iRowId, int iColumnId, int iItemTypeId)
            {
                string sSQL;
                int iItemId;
                string sRtnMsg = "";

                iItemId = GetGridItemId(iFormId, iSectionId, iRowId, iColumnId, ref sRtnMsg);

                sSQL = "Delete from " + sTableNames[5] + " where FormId = " + iFormId + " and SectionType = " + iSectionId + " and ItemId = " + iItemId +
                       " and ParameterId in (Select ID from tblParameters where SectionType = " + iSectionId + " and ItemType = " + iItemTypeId + ")";

                return DB.ExecuteSQL(sSQL, ref sRtnMsg);
            }

            public bool DeleteAllItemAttributesNotOfType(int iFormId, int iSectionId, int iSectionItemType, int iRowId, int iColumnId, int iItemTypeId)
            {
                string sSQL;
                int iItemId;
                string sRtnMsg = "";

                iItemId = GetGridItemId(iFormId, iSectionId, iRowId, iColumnId, ref sRtnMsg);

                sSQL = "Delete from " + sTableNames[5] + " where FormId = " + iFormId + " and SectionType = " + iSectionItemType + " and ItemId = " + iItemId +
                       " and ParameterId not in (Select ID from tblParameters where SectionType = " + iSectionId + " and ItemType = " + iItemTypeId + ")";

                return DB.ExecuteSQL(sSQL, ref sRtnMsg);
            }


            public ArrayList GetGridItemAttributes(int iFormId, int iSectionId, int iRowId, int iColumnId)
            {
                ArrayList rtnArray = new ArrayList();
                string sSQL;
                string[] colNames = { "ItemType", "ParameterId", "ParameterValue", "ParameterName" };
                string sRtnMsg = "";
                ArrayList arrItemType = new ArrayList();
                ArrayList arrId = new ArrayList();
                ArrayList arrValue = new ArrayList();
                ArrayList arrParameterName = new ArrayList();
                try
                {
                    if (DB.TableExists(sTableNames[3]))
                    {
                        sSQL = "select G.ItemType, I.ParameterId as ParameterId, I.ParameterValue, P.ParameterName " +
                                   "from " + sTableNames[3] + " G, " + sTableNames[5] + " I, " + sTableNames[6] + " P " +
                                   "where G.FormId = " + iFormId + " and G.SectionId = " + iSectionId + " " +
                                   "and G.RowId = " + iRowId + " and G.ColumnId = " + iColumnId + " " +
                                   "and G.Id = I.ItemId " +
                                   "and I.ParameterId = P.Id ";

                        DataSet ds = new DataSet();
                        ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);

                        for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                        {
                            int iItemType = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[0]);
                            int iId = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[1]);
                            string sParameterValue = ds.Tables[0].Rows[i].ItemArray[2].ToString();
                            string sParameterName = ds.Tables[0].Rows[i].ItemArray[3].ToString();

                            arrItemType.Add(iItemType);
                            arrId.Add(iId);
                            arrValue.Add(sParameterValue);
                            arrParameterName.Add(sParameterName);
                        }

                        if (arrId.Count > 0)
                        {
                            rtnArray.Add(arrItemType);
                            rtnArray.Add(arrId);
                            rtnArray.Add(arrValue);
                            rtnArray.Add(arrParameterName);
                        }
                        return rtnArray;
                    }
                    else
                    {
                        return rtnArray;
                    }
                }
                catch (Exception ex)
                {
                    rtnArray.Clear();
                    rtnArray.Add("Failure");
                    rtnArray.Add(ex.Message.ToString());
                    return rtnArray;
                }


            }

            public bool SaveFormDetails(string sFormName, string sFormDescription, ref int iFormId, ref string sRtnMsg)
            {
                try
                {
                    bool bSQL;
                    string[] sValues = new string[2];
                    sValues[0] = sFormName;
                    sValues[1] = sFormDescription;
                    SqliteConnection conn = DB.OpenConnection();
                     DB.SetOpenConnection(conn);

                    if (iFormId <= 0)
                    {
                        bSQL = DB.AddRecordOpenConnection(sTableNames[0], sColumnsTableNoAutoId1, sBaseTypesTableNoAutoId1, sValues, conn);
                        iFormId = DB.GetAutoIdValue(conn, ref sRtnMsg);
                    }
                    else
                    {
                        bSQL = DB.UpdateRecordOpenConnection(sTableNames[0], sColumnsTableNoAutoId1, sBaseTypesTableNoAutoId1, sValues, "Id = " + iFormId, conn);
                    }

                    DB.CloseConnection(conn);
                    return bSQL;
                }
                catch (Exception ex)
                {
                    sRtnMsg = ex.Message.ToString();
                    return false;
                }

            }


            public int GetGridItemId(int iFormId, int iSectionId, int iRowId, int iColumnId, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "Id";
                sSQL = "Select Id from " + sTableNames[3] + " where FormId = " + iFormId + " and SectionId = " + iSectionId + " and RowId = " + iRowId + " and ColumnId = " + iColumnId;
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return -1;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return -1;
                    }
                    else
                    {
                        return Convert.ToInt32(ds.Tables[0].Rows[0].ItemArray[0]);
                    }
                }
            }

            public int GetGridItemId(int iFormId, int iSectionId, int iCellId,  ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "Id";
                sSQL = "Select Id from " + sTableNames[3] + " where FormId = " + iFormId + " and SectionId = " + iSectionId + " and ItemId = " + iCellId ;
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return -1;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return -1;
                    }
                    else
                    {
                        return Convert.ToInt32(ds.Tables[0].Rows[0].ItemArray[0]);
                    }
                }
            }


            public bool IsColumnSpanned(int iFormId, int iSectionId, int iRowId, int iColumnId, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "Id";
                sSQL =  "select G1.Id " +
                        "from tblGridItems G1, tblItemAttributes I, tblParameters P " +
                        "where G1.FormId = " + iFormId + " " +
                        "and G1.SectionId = " + iSectionId + " " +
                        "and G1.RowId = " + iRowId + " " +
                        "and G1.ColumnId < " + iColumnId + " " +
                        "and G1.FormId = I.FormId " +
                        "and I.ItemId = G1.Id " +
                        "and I.ParameterId = P.Id " +
                        "and P.ParameterName = 'ColumnSpan' " +
                        "and P.ItemType = G1.ItemType " +
                        "and G1.ColumnId + cast(I.ParameterValue as int) > " + iColumnId;
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return false;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
            }

            public int ColumnsHiddenInSection(int iFormId, int iSectionId, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                int iHiddenCols = 0;
                sColNames[0] = "Counter";
                sSQL = "select count(*) as Counter " +
                        "from tblItemAttributes I, tblParameters P " +
                        "where I.FormId = " + iFormId + " " +
                        "and I.SectionType = " + iSectionId + " " +
                        "and I.ParameterId = P.Id " +
                        "and P.ParameterName = 'Visible' " +
                        "and P.SectionType = I.SectionType " +
                        "and I.ParameterValue = 'No'";
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return 0;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return 0;
                    }
                    else
                    {
                        clsLocalUtils utils = new clsLocalUtils();
                        string sHiddenCols = ds.Tables[0].Rows[0].ItemArray[0].ToString();
                        if (utils.IsNumeric(sHiddenCols))
                        {
                            iHiddenCols = Convert.ToInt32(sHiddenCols);
                        }
                        return iHiddenCols;
                    }
                }
            }

            public int ColumnsHiddenInSectionFromColumn(int iFormId, int iSectionId, int iThisColumn, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                int iHiddenCols = 0;
                sColNames[0] = "Counter";
                sSQL = "select count(*) as Counter " +
                        "from tblItemAttributes I, tblParameters P, tblGridItems G " +
                        "where I.FormId = " + iFormId + " " +
                        "and I.SectionType = " + iSectionId + " " +
                        "and I.ParameterId = P.Id " +
                        "and P.ParameterName = 'Visible' " +
                        "and P.SectionType = I.SectionType " +
                        "and I.ParameterValue = 'No' " +
                        "and G.ID = I.ItemId " +
                        "and G.ColumnId >= " + iThisColumn;
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return 0;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return 0;
                    }
                    else
                    {
                        clsLocalUtils utils = new clsLocalUtils();
                        string sHiddenCols = ds.Tables[0].Rows[0].ItemArray[0].ToString();
                        if (utils.IsNumeric(sHiddenCols))
                        {
                            iHiddenCols = Convert.ToInt32(sHiddenCols);
                        }
                        return iHiddenCols;
                    }
                }
            }

            public int MinimumSpannedColumnNumberForSection(int iFormId, int iSectionId, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                int iMinColNo = 999;
                sColNames[0] = "MinColNo";
                sSQL = "select min(ColumnId) as MinColNo " +
                        "from tblItemAttributes I, tblParameters P, tblGridItems G " +
                        "where I.FormId = " + iFormId + " " +
                        "and I.SectionType = " + (int)SectionType.GridItem + " " +
                        "and I.ParameterId = P.Id " +
                        "and P.ParameterName = 'ColumnSpan' " +
                        "and P.SectionType = I.SectionType " +
                        "and I.ParameterValue > 1 " +
                        "and G.ID = I.ItemId " +
                        "and G.SectionId = " + iSectionId;
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return 999;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return 999;
                    }
                    else
                    {
                        clsLocalUtils utils = new clsLocalUtils();
                        string sMinColNo = ds.Tables[0].Rows[0].ItemArray[0].ToString();
                        if (utils.IsNumeric(sMinColNo))
                        {
                            iMinColNo = Convert.ToInt32(sMinColNo);
                        }
                        else
                        {
                            iMinColNo = 999;
                        }
                        return iMinColNo;
                    }
                }
            }

            public int[] GetHiddenColumns(int iFormId, int iSectionId, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "ColumnId";
                sSQL = "select ColumnId " +
                        "from tblItemAttributes I, tblParameters P, tblGridItems G " +
                        "where I.FormId = " + iFormId + " " +
                        "and I.SectionType = " + iSectionId + " " +
                        "and I.ParameterId = P.Id " +
                        "and P.ParameterName = 'Visible' " +
                        "and P.SectionType = I.SectionType " +
                        "and I.ParameterValue = 'No' " +
                        "and G.ID = I.ItemId ";
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return null;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return null;
                    }
                    else
                    {
                        clsLocalUtils utils = new clsLocalUtils();
                        int[] rtnInt = new int[ds.Tables[0].Rows.Count];
                        for (int i = 0; i < rtnInt.Length; i++)
                        {
                            rtnInt[i] = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[0]);
                        }
                        return rtnInt;
                    }
                }
            }

            public int[] GetHiddenRows(int iFormId, int iSectionId, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "RowId";
                sSQL = "select RowId " +
                        "from tblItemAttributes I, tblParameters P, tblGridItems G " +
                        "where I.FormId = " + iFormId + " " +
                        "and I.SectionType = " + iSectionId + " " +
                        "and I.ParameterId = P.Id " +
                        "and P.ParameterName = 'Visible' " +
                        "and P.SectionType = I.SectionType " +
                        "and I.ParameterValue = 'No' " +
                        "and G.ID = I.ItemId ";
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return null;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return null;
                    }
                    else
                    {
                        clsLocalUtils utils = new clsLocalUtils();
                        int[] rtnInt = new int[ds.Tables[0].Rows.Count];
                        for (int i = 0; i < rtnInt.Length; i++)
                        {
                            rtnInt[i] = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[0]);
                        }
                        return rtnInt;
                    }
                }
            }

            public int GetHiddenRowCount(int iFormId, int iSectionId, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "Counter";
                sSQL = "select count(*) as Counter " +
                        "from tblItemAttributes I, tblParameters P, tblGridItems G " +
                        "where I.FormId = " + iFormId + " " +
                        "and I.SectionType = " + iSectionId + " " +
                        "and I.ParameterId = P.Id " +
                        "and P.ParameterName = 'Visible' " +
                        "and P.SectionType = I.SectionType " +
                        "and I.ParameterValue = 'No' " +
                        "and G.ID = I.ItemId ";
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return 0;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return 0;
                    }
                    else
                    {
                        int rtnInt = Convert.ToInt32(ds.Tables[0].Rows[0].ItemArray[0]);
                        return rtnInt;
                    }
                }
            }

            public bool SaveGridItemDetails(int iFormId, int iSectionId, int iItemType, int iRowId, int iColumnId, int iItemProgramId, ref int iUniqueItemId, ref string sRtnMsg)
            {
                try
                {
                    bool bSQL;
                    string[] sValues = new string[6];
                    sValues[0] = iFormId.ToString();
                    sValues[1] = iSectionId.ToString();
                    sValues[2] = iItemType.ToString();
                    sValues[3] = iRowId.ToString();
                    sValues[4] = iColumnId.ToString();
                    sValues[5] = iItemProgramId.ToString();

                    iUniqueItemId = GetGridItemId(iFormId, iSectionId, iRowId, iColumnId, ref sRtnMsg);
                    SqliteConnection conn = DB.OpenConnection();
                    DB.SetOpenConnection(conn);

                    if (iUniqueItemId < 0)
                    {
                        bSQL = DB.AddRecordOpenConnection(sTableNames[3], sColumnsTableNoAutoId4, sBaseTypesTableNoAutoId4, sValues, conn);
                        iUniqueItemId = DB.GetAutoIdValue(conn, ref sRtnMsg);
                    }
                    else
                    {
                        bSQL = DB.UpdateRecordOpenConnection(sTableNames[3], sColumnsTableNoAutoId4, sBaseTypesTableNoAutoId4, sValues, "Id = " + iUniqueItemId, conn);
                    }

                    DB.CloseConnection(conn);
                    return bSQL;
                }
                catch (Exception ex)
                {
                    sRtnMsg = ex.Message.ToString();
                    return false;
                }

            }

            public ArrayList GetAllForms()
            {
                ArrayList rtnArray = new ArrayList();
                string sSQL;
                string[] colNames = sColumnsTable1;
                string sRtnMsg = "";
                ArrayList arrId = new ArrayList();
                ArrayList arrName = new ArrayList();
                ArrayList arrDesc = new ArrayList();
                try
                {
                    if (DB.TableExists(sTableNames[0]))
                    {
                        sSQL = "select ID, Name, Description " +
                                   "from " + sTableNames[0];

                        DataSet ds = new DataSet();
                        ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);

                        for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                        {
                            int iId = Convert.ToInt32(ds.Tables[0].Rows[i].ItemArray[0]);
                            string sName = ds.Tables[0].Rows[i].ItemArray[1].ToString();
                            string sDesc = ds.Tables[0].Rows[i].ItemArray[2].ToString();

                            arrId.Add(iId);
                            arrName.Add(sName);
                            arrDesc.Add(sDesc);
                        }

                        rtnArray.Add(arrId);
                        rtnArray.Add(arrName);
                        rtnArray.Add(arrDesc);
                        return rtnArray;
                    }
                    else
                    {
                        return rtnArray;
                    }
                }
                catch (Exception ex)
                {
                    rtnArray.Clear();
                    rtnArray.Add("Failure");
                    rtnArray.Add(ex.Message.ToString());
                    return rtnArray;
                }


            }


            public ArrayList GetFormDetails(int iFormId)
            {
                ArrayList rtnArray = new ArrayList();
                string sSQL;
                string[] colNames = sColumnsTable1;
                string sRtnMsg = "";
                try
                {
                    if (DB.TableExists(sTableNames[0]))
                    {
                        sSQL = "select ID, Name, Description " +
                                   "from " + sTableNames[0] + " " +
                                   "where ID =" + iFormId ;

                        DataSet ds = new DataSet();
                        ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);

                        if (ds.Tables.Count > 0)
                        {
                            if (ds.Tables[0].Rows.Count > 0)
                            {
                                int iId = Convert.ToInt32(ds.Tables[0].Rows[0].ItemArray[0]);
                                string sName = ds.Tables[0].Rows[0].ItemArray[1].ToString();
                                string sDesc = ds.Tables[0].Rows[0].ItemArray[2].ToString();

                                rtnArray.Add(iId);
                                rtnArray.Add(sName);
                                rtnArray.Add(sDesc);
                            }
                            else
                            {
                                rtnArray.Add(-1);
                                rtnArray.Add("Form");
                                rtnArray.Add("");
                            }
                        }
                        else
                        {
                            rtnArray.Add(-1);
                            rtnArray.Add("Form");
                            rtnArray.Add("");
                        }
                        return rtnArray;
                    }
                    else
                    {
                        rtnArray.Add(-1);
                        rtnArray.Add("Form");
                        rtnArray.Add("");

                        return rtnArray;
                    }
                }
                catch (Exception ex)
                {
                    rtnArray.Clear();
                    rtnArray.Add("Failure");
                    rtnArray.Add(ex.Message.ToString());
                    return rtnArray;
                }


            }

            public int GetVersion(string sDeviceId, int iDebugging)
            {
                string sSQL;
                string[] colNames = {"VersionType"};
                string sRtnMsg = "";
                try
                {
                    if (iDebugging > 0)
                    {
                        return 99; //Dumb way of getting around the versioning. Very important to have this turned off in release.
                    }

                    if (DB.TableExists("tblVersion"))
                    {
                        sSQL = "select VersionType from tblVersion where DeviceId = '" + sDeviceId + "'";

                        DataSet ds = new DataSet();
                        ds = DB.ReadSQLDataSet(sSQL, colNames, ref sRtnMsg);

                        int iVersion = Convert.ToInt32(ds.Tables[0].Rows[0].ItemArray[0]);
                        return iVersion;
                    }
                    else
                    {
                        //Create the table and put in the base version of zero
                        DB.CreateTable("tblVersion", sVersionTableColumns, sVersionTableColumnTypes);
                        //Now insert the record into the table as a free version only
                        sSQL = "INSERT INTO tblVersion (DeviceId, VersionType) values ('" + sDeviceId + "',0)";
                        DB.ExecuteSQL(sSQL, ref sRtnMsg);
                        return 0;
                    }
                }
                catch (Exception ex)
                {
                    sRtnMsg = ex.Message.ToString();
                    return -1;
                }


            }

            public bool ReservedTableExists(string sUserTableName, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "TableName";
                sSQL = "select TableName from " + sTableNames[9] + " where TableName = '" + sUserTableName + "'";
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return false;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
            }

            public bool UserTableExists(string sUserTableName, ref string sRtnMsg)
            {
                string sSQL;
                string[] sColNames = new string[1];
                sColNames[0] = "TableName";
                sSQL = "select TableName from " + sTableNames[10] + " where TableName = '" + sUserTableName + "'";
                DataSet ds = DB.ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count <= 0)
                {
                    return false;
                }
                else
                {
                    if (ds.Tables[0].Rows.Count <= 0)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
            }

            public bool SetUserTableRecord(string sTableName, ref string sRtnMsg)
            {
                string[] sValues = new string[1];
                string sSQL;

                sSQL = "Select * from " + sTableNames[10] + " where TableName = '" + sTableName + "'";
                if (DB.GetSQLRecordCount(sSQL, ref sRtnMsg) <= 0)
                {
                    if (sRtnMsg != "")
                    {
                        return false;
                    }
                    sValues[0] = sTableName;
                    return DB.AddRecord(sTableNames[10], sColumnsTableNoAutoId11, sBaseTypesTableNoAutoId11, sValues);
                }
                else
                {
                    return true;
                }
            }

            public bool DeleteUserTableRecord(string sTableName, ref string sRtnMsg)
            {
                string sSQL;
                sSQL = "Delete from " + sTableNames[10] + " where TableName = '" + sTableName + "'";
                return DB.ExecuteSQL(sSQL, ref sRtnMsg);

            }

        } //End GridUtilities class
        
    }


    public class LocalDB
    {
        SqliteConnection m_conn;

        public SqliteConnection OpenConnection()
        {
            return Connection();
        }

        public SqliteConnection Connection()
        {
            string dbPath = Path.Combine(
                            System.Environment.GetFolderPath(System.Environment.SpecialFolder.MyDocuments),
                            "BuilderAppDB1.db3");
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

        public void SetOpenConnection(SqliteConnection conn)
        {
            m_conn = conn;
        }

        public void CloseConnection(SqliteConnection conn)
        {
            conn.Dispose();
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
                        sCols += "[" + sColumns[i] + "] " + sTypes[i] + " COLLATE NOCASE,";
                    }

                    sCols = sCols.Substring(0, sCols.Length - 1) + ");";
                    sCommand += sCols;

                    using (var c = conn.CreateCommand())
                    {
                        c.CommandText = sCommand;
                        c.ExecuteNonQuery();
                        c.Dispose();

                    }

                }
                else
                {
                    //Update the table
                    sCommand = "ALTER TABLE [" + sTableName + "] ADD COLUMN ";
                    for (i = 0; i < sTypes.Length; i++)
                    {
                        if(!TableColumnExists(sTableName, sColumns[i]))
                        {
                            sCols = "ALTER TABLE [" + sTableName + "] ADD COLUMN [" + sColumns[i] + "] " + sTypes[i] + " COLLATE NOCASE;";
                            sCommand = sCols;

                            using (var c = conn.CreateCommand())
                            {
                                c.CommandText = sCommand;
                                c.ExecuteNonQuery();
                                c.Dispose();

                            }
                        }
                    }

                    //if (sCols.Length > 0)
                    //{
                    //    sCols = sCols.Substring(0, sCols.Length - 1) + ";";

                    //}
                }
                CloseConnection(conn);
                return true;
            }
            catch(Exception ex)
            {
                string sRtnMsg = ex.Message.ToString();
                CloseConnection(conn);
                return false;
            }
        }

        public bool DropTable(string sTableName, ref string sRtnMsg)
        {
            string sSQL = "Drop table " + sTableName;
            return ExecuteSQL(sSQL, ref sRtnMsg);
        }

        public bool RenameTable(string sFromTableName, string sToTableName, ref string sRtnMsg)
        {
            string sSQL = "Alter table " + sFromTableName + " to " + sToTableName;
            return ExecuteSQL(sSQL, ref sRtnMsg);
        }

        public bool AddRecord(string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues)
        {
            return AddRecordConnection(sTableName, sColNames, sColTypes, objColValues, true);
        }

        public bool AddRecordOpenConnection(string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, SqliteConnection conn)
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
                            if (objColValues[i].ToString() == "")
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
                            if (objColValues[i].ToString() == "")
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
                            if (objColValues[i].ToString() == "")
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
                if (bConnectionType)
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


        public bool UpdateRecord(string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, string sWhereClause)
        {
            return UpdateRecordConnection(sTableName, sColNames, sColTypes, objColValues, sWhereClause, true);
        }

        public bool UpdateRecordOpenConnection(string sTableName, string[] sColNames, string[] sColTypes, object[] objColValues, string sWhereClause, SqliteConnection conn)
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
                            if (objColValues[i].ToString() == "")
                            {
                                sSQL += "NULL";
                            }
                            else
                            {
                                sDate = Dte.Get_Date_String(Convert.ToDateTime(objColValues[i]), "yyyymmdd");
                                sSQL += "'" + sDate + "'";
                            }
                            break;
                        case "datetime":
                            if (objColValues[i].ToString() == "")
                            {
                                sSQL += "NULL";
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
                            if (objColValues[i].ToString() == "")
                            {
                                sSQL += "NULL";
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

                sSQL = sSQL.Substring(0, sSQL.Length - 1) + " where " + sWhereClause;

                if (bConnectionType)
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
                c.Dispose();
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
                c.Dispose();
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

        public bool TableColumnExists(string sTableName, string sColumnName)
        {
            string sSQL = "SELECT sql FROM sqlite_master WHERE type='table' AND name='" + sTableName + "' and sql like '%[" + sColumnName + "]%'";
            DataSet ds = new DataSet();
            string[] sColNames = new string[1];
            sColNames[0] = "sql";
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

        public ArrayList ReadSQLArray(string sSQL, ref string sRtnMsg)
        {
            ArrayList arrColNames = GetColumnNamesFromSQL(sSQL, ref sRtnMsg);
            int i;
            int j;
            ArrayList rtnArray = new ArrayList();
            ArrayList arrRowValue = new ArrayList();

            if (sRtnMsg == "")
            {
                string[] sColNames = new string[arrColNames.Count];

                for (i = 0; i < arrColNames.Count; i++)
                {
                    sColNames[i] = arrColNames[i].ToString();
                }

                DataSet ds = ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);
                if (ds.Tables.Count > 0)
                {
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        rtnArray.Add(arrColNames);

                        for (i = 0; i < ds.Tables[0].Rows.Count; i++)
                        {
                            ArrayList arrValues = new ArrayList();
                            for (j = 0; j < ds.Tables[0].Columns.Count; j++)
                            {
                                arrValues.Add(ds.Tables[0].Rows[i].ItemArray[j]);
                            }

                            arrRowValue.Add(arrValues);
                        }

                        rtnArray.Add(arrRowValue);
                    }

                }
            }

            return rtnArray;
        }

        public DataSet ReadSQLDataSet(string sSQL, string[] sColNames, ref string sRtnMsg)
        {
            return ReadSQLDataSetConnection(sSQL, sColNames, ref sRtnMsg, false);
        }

        public DataSet ReadSQLDataSetOpenConnection(string sSQL, string[] sColNames, ref string sRtnMsg, SqliteConnection conn)
        {
            m_conn = conn;
            return ReadSQLDataSetConnection(sSQL, sColNames, ref sRtnMsg, true);
        }

        public DataSet ReadSQLDataSetConnection(string sSQL, string[] sColNames, ref string sRtnMsg, bool bConnection)
        {
            if (!bConnection)
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
                c.Dispose();
                if (!bConnection)
                {
                    CloseConnection(m_conn);
                }
                return ds;
            }
            catch (Exception e)
            {
                sRtnMsg = e.Message.ToString();
                if (!bConnection)
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

        public int GetAutoIdValue(SqliteConnection conn, ref string sRtnMsg)
        {
            string sSQL;
            string[] colNames = new string[1];
            int iAutoId = -1;

            try
            {
                sSQL = "Select last_insert_rowid() as NewRow";
                DataSet ds = new DataSet();
                colNames[0] = "NewRow";
                ds = ReadSQLDataSetOpenConnection(sSQL, colNames, ref sRtnMsg, conn);
                string sReturn = ds.Tables[0].Rows[0].ItemArray[0].ToString();
                iAutoId = Convert.ToInt32(sReturn);

                return iAutoId;
            }
            catch (Exception ex)
            {
                sRtnMsg = ex.Message.ToString();
                return -1;
            }
        }

        public ArrayList GetTableNamesFromSQL(string sSQL, ref string sRtnMsg)
        {
            int iStart;
            int iEnd;
            int iLength;
            int i;
            string sSubSQL;
            string sTableName;
            ArrayList rtnArray = new ArrayList();
            string sAlias;
            ArrayList arrTableName = new ArrayList();
            ArrayList arrAlias = new ArrayList();

            sSQL = sSQL.ToUpper();

            iStart = sSQL.IndexOf(" FROM ");
            if (sSQL.Contains(" WHERE "))
            {
                iEnd = sSQL.IndexOf(" WHERE ");
            }
            else
            {
                iEnd = sSQL.Length;
            }
            iLength = iEnd - iStart - 6;

            if (iLength > 0)
            {
                sSubSQL = sSQL.Substring(iStart + 6, iLength).Trim();
                sSubSQL = sSubSQL.Replace("INNER JOIN", ",");
                sSubSQL = sSubSQL.Replace("CROSS JOIN", ",");
                sSubSQL = sSubSQL.Replace("NATURAL JOIN", ",");
                sSubSQL = sSubSQL.Replace("LEFT OUTER JOIN", ",");
                sSubSQL = sSubSQL.Replace("RIGHT OUTER JOIN", ",");
                string[] sTables = sSubSQL.Split(',');

                //Now pull out the on clauses fro any inner or outer joins
                for (i = 0; i < sTables.Length; i++)
                {
                    sTableName = sTables[i];
                    if (sTableName.Contains(" ON "))
                    {
                        sTableName = sTableName.Substring(sTableName.IndexOf(" ON "));
                    }
                    sTableName = sTableName.Trim();

                    //Now get rid of the alias if it exists
                    iEnd = sTableName.IndexOf(" ");
                    if (iEnd < 0)
                    {
                        iEnd = sTableName.Length;
                        sAlias = sTableName.Trim();
                    }
                    else
                    {
                        iLength = sTableName.Length - iEnd;
                        sAlias = sTableName.Substring(iEnd, iLength).Trim(); 

                    }

                    iLength = iEnd;

                    sTableName = sTableName.Substring(0, iLength);

                    arrAlias.Add(sAlias);
                    arrTableName.Add(sTableName);
                }

                rtnArray.Add(arrTableName);
                rtnArray.Add(arrAlias);

            }

            return rtnArray;

        }

        public ArrayList GetTableColumnNames(string sTableName, ref string sRtnMsg)
        {
            string sColName = "";
            string sSQL;
            DataSet ds = new DataSet();
            string[] sColNames = new string[1];
            sColNames[0] = "sql";
            ArrayList rtnArray = new ArrayList();
            int iStart;
            int iEnd;
            int iLength;
            int i;

            sSQL = "SELECT sql FROM sqlite_master " +
                   "WHERE tbl_name = '" + sTableName + "' COLLATE NOCASE";

            ds = ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);

            if(ds==null)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            if(ds.Tables.Count <= 0)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            if(ds.Tables[0].Rows.Count <= 0)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            sColName = ds.Tables[0].Rows[0].ItemArray[0].ToString();
            sColName = sColName.Substring(sColName.IndexOf("("));

            //Now parse out the column name from the start of each bit
            string[] sColInfo = sColName.Split(',');
            //The column name is the bit between the first set of square brackets
            for(i=0;i<sColInfo.Length;i++)
            {
                iStart = sColInfo[i].IndexOf("[") + 1;
                iEnd = sColInfo[i].IndexOf("]");
                iLength = iEnd - iStart;
                if (iLength > 0)
                {
                    rtnArray.Add(sColInfo[i].Substring(iStart,iLength).Trim());
                }
            }
            return rtnArray;
        }

        public ArrayList GetTableColumnDetails(string sTableName, ref string sRtnMsg)
        {
            string sColName = "";
            string sSQL;
            DataSet ds = new DataSet();
            string[] sColNames = new string[1];
            sColNames[0] = "sql";
            ArrayList rtnArray = new ArrayList();
            ArrayList arrColNames = new ArrayList();
            ArrayList arrColTypes = new ArrayList();
            ArrayList arrColSize = new ArrayList();
            ArrayList arrNull = new ArrayList();
            int iStart;
            int iEnd;
            int iLength;
            int i;
            DataSet ds2 = new DataSet();
            string[] sColNames2 = new string[6];
            sColNames2[0] = "cid";
            sColNames2[1] = "name";
            sColNames2[2] = "type";
            sColNames2[3] = "notnull";
            sColNames2[4] = "dflt_value";
            sColNames2[5] = "pk";
            string sType = "";
            string sSize = "";
            string sThisColName = "";


            sSQL = "SELECT sql FROM sqlite_master " +
                   "WHERE tbl_name = '" + sTableName + "' COLLATE NOCASE";

            ds = ReadSQLDataSet(sSQL, sColNames, ref sRtnMsg);

            if (ds == null)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            if (ds.Tables.Count <= 0)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            if (ds.Tables[0].Rows.Count <= 0)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            sColName = ds.Tables[0].Rows[0].ItemArray[0].ToString();

            sSQL = "PRAGMA table_info('" + sTableName + "')";
            ds2 = ReadSQLDataSet(sSQL, sColNames2, ref sRtnMsg);

            if (ds2 == null)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            if (ds2.Tables.Count <= 0)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            if (ds2.Tables[0].Rows.Count <= 0)
            {
                sRtnMsg = "There is no table in the database called " + sTableName;
                return rtnArray;
            }

            for (i = 0; i < ds2.Tables[0].Rows.Count; i++)
            {
                sThisColName = ds2.Tables[0].Rows[i].ItemArray[1].ToString();
                arrColNames.Add(sThisColName);
                sType = ds2.Tables[0].Rows[i].ItemArray[2].ToString();
                arrColTypes.Add(sType);
                if(sType.ToUpper() == "NVARCHAR")
                {
                    sSize = sColName.Substring(sColName.IndexOf("[" + sThisColName + "]") + sThisColName.Length + 2);
                    sSize = sSize.Substring(sSize.IndexOf("[" + sType + "]") + 2 + sType.Length);
                    sSize = sSize.Substring(sSize.IndexOf("(") + 1);
                    sSize = sSize.Substring(0, sSize.IndexOf(")"));
                }
                else
                {
                    sSize = "";
                }
                arrColSize.Add(sSize);
                arrNull.Add(ds2.Tables[0].Rows[i].ItemArray[3].ToString());
            }

            rtnArray.Add(arrColNames);
            rtnArray.Add(arrColTypes);
            rtnArray.Add(arrColSize);
            rtnArray.Add(arrNull);

            return rtnArray;
        }

        public bool CopyTableStructureAndData(string sTableFrom, string sTableTo, string[] sFromColNamesToReplace, string[] sToColNamesToReplace, string[] sNewReplaceTypes, bool bCopyData, ref string sRtnMsg)
        {
            int i, j;
            int iCounter = 0;
            ArrayList arrColDetails = GetTableColumnDetails(sTableFrom, ref sRtnMsg);
            ArrayList arrColNames = new ArrayList();
            ArrayList arrColTypes = new ArrayList();
            ArrayList arrColSize = new ArrayList();
            ArrayList arrNull = new ArrayList();
            
            //Now for each piece of info in the array we need to build a row
            arrColNames = (ArrayList)arrColDetails[0];
            arrColTypes = (ArrayList)arrColDetails[1];
            arrColSize = (ArrayList)arrColDetails[2];
            arrNull = (ArrayList)arrColDetails[3];
            int iRows = arrColNames.Count;
            string[] sColumns = new string[iRows];
            string[] sTypes = new string[iRows];
            bool bFoundReplacement = false;
            string sSQL = "INSERT INTO " + sTableTo + "(";
            string sSQL1 = "SELECT ";
            bool bRtn = true;

            for (i = 0; i < iRows; i++)
            {
                bFoundReplacement = false;
                if (sFromColNamesToReplace != null && sToColNamesToReplace != null && sNewReplaceTypes != null)
                {
                    if (sFromColNamesToReplace.Length == sToColNamesToReplace.Length)
                    {
                        for (j = 0; j < sFromColNamesToReplace.Length; j++)
                        {
                            if (arrColNames[i].ToString().ToUpper() == sFromColNamesToReplace[j].ToUpper())
                            {
                                sTypes[iCounter] = sNewReplaceTypes[j];
                                sColumns[i] = sToColNamesToReplace[j];
                                bFoundReplacement = true;
                                break;
                            }

                        }
                    }
                }

                string sSize = arrColSize[i].ToString();

                if(sSize != "")
                {
                    sSize = "(" + sSize + ") ";
                }

                string sNull = "";
                if (arrNull[i].ToString() == "0")
                {
                    sNull = " NULL";
                }
                else
                {
                    sNull = " NOT NULL";
                }

                if (!bFoundReplacement)
                {
                    sColumns[iCounter] = arrColNames[i].ToString();
                    sTypes[iCounter] = "[" + arrColTypes[i].ToString() + "] " + sSize + " " + sNull;
                }

                if (sColumns[iCounter] != "")
                {
                    sSQL = sSQL + sColumns[iCounter] + ",";
                    sSQL1 = sSQL1 + arrColNames[i].ToString() + ",";
                    iCounter++;
                }
            }

            string[] sColumnsToAdd = new string[iCounter];
            string[] sTypesToAdd = new string[iCounter];

            for (i = 0; i < iCounter; i++)
            {
                sColumnsToAdd[i] = sColumns[i];
                sTypesToAdd[i] = sTypes[i];
            }

            bRtn = CreateTable(sTableTo, sColumnsToAdd, sTypesToAdd);

            sSQL = sSQL.Substring(0, sSQL.Length - 1) + ") ";
            sSQL1 = sSQL1.Substring(0, sSQL1.Length - 1) + " FROM " + sTableFrom;

            sSQL = sSQL + sSQL1;

            if (bRtn && bCopyData)
            {
                bRtn = ExecuteSQL(sSQL, ref sRtnMsg);
            }

            return bRtn;
        }

        public ArrayList GetColumnNamesFromSQL(string sSQL, ref string sRtnMsg)
        {
            string sColumnNames;
            int i;
            int j;
            int iStart;
            int iEnd;
            int iLength;
            ArrayList sTableName = new ArrayList();
            ArrayList sColNames = new ArrayList();
            ArrayList rtnArray = new ArrayList();

            //Get the columnNames
            sSQL = sSQL.ToUpper();
            iStart = sSQL.IndexOf("SELECT ") + 7;
            iEnd = sSQL.IndexOf(" FROM ");
            iLength =iEnd - iStart;
            if (iLength > 0)
            {
                sColumnNames = sSQL.Substring(iStart, iLength).Trim();
                if (sColumnNames.Contains("*"))
                {
                    sTableName = GetTableNamesFromSQL(sSQL, ref sRtnMsg);
                    string[] arrColNames = new string[0];
                    ArrayList arrTableNamesOnly = (ArrayList)sTableName[0];
                    ArrayList arrTableAlias = (ArrayList)sTableName[1];
                    for (i = 0; i < arrTableNamesOnly.Count; i++)
                    {
                        //Then get the column names from the table
                        sColNames = GetTableColumnNames(arrTableNamesOnly[i].ToString(), ref sRtnMsg);
                        for (j = 0; j < sColNames.Count; j++)
                        {
                            rtnArray.Add(arrTableAlias[i].ToString().Trim() + "." + sColNames[j].ToString().Trim());
                        }
                    }
                }
                else
                {
                    string[] arrColNames = sColumnNames.Split(',');
                    for (i = 0; i < arrColNames.Length; i++)
                    {
                        string sThisColName = arrColNames[i].Trim();

                        //Now want the bit after the AS if the column has been aliased
                        if (sThisColName.Contains(" AS "))
                        {
                            sThisColName = sThisColName.Substring(sThisColName.IndexOf(" AS ") + 4).Trim();
                        }
                        rtnArray.Add(sThisColName);
                    }
                }
            }

            return rtnArray;
        }
    }


    public class clsLocalUtils
    {
        public int iEnvironment = 1; //0 = Development, 1 = Test, 2 = Production
        public string sUser = "dcscanner";
        public string sPassword = "Welcome1";
        public string sDomain = "SILCAR";
        public string sDTUser = "DesignTool";
        public string sDTPassword = "DesignTool";
        public int m_iSecureFlag = 0;

        public string GetEnvironment_wbsURL(string sWBSType)
        {
            string sURL;
            switch (sWBSType)
            {
                case "wbsITP_External":
                    switch (iEnvironment)
                    {
                        case 0:
                            sURL = "http://silcar-ws21.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        case 1:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        case 2:
                            if (m_iSecureFlag == 0)
                            {
                                sURL = "http://scms.silcar.com.au/wbsITP_External.asmx";
//                                sURL = "http://silcar-ws01.silcar.com.au:8003/wbsITP_External.asmx";
                            }
                            else
                            {
                                sURL = "https://scms.silcar.com.au/wbsITP_External.asmx";
//                                sURL = "http://silcar-ws01.silcar.com.au:8003/wbsITP_External.asmx";
                            }
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
                            sURL = "http://silcar-ws21.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        case 1:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        case 2:
                            sURL = "https://silcar-ws01.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                        default:
                            sURL = "http://silcar-ws11.silcar.com.au:8003/wbsITP_External.asmx";
                            break;
                    }
                    break;
            }

            return sURL;
        }

        public void SetSecureFlag(int iSecureFlag)
        {
            m_iSecureFlag = iSecureFlag;
        }

        public bool SCMSLogout(string sUserId, string sSessionId)
        {
            string sURL = GetEnvironment_wbsURL("wbsITP_External");
            wbsITP_External ws = new wbsITP_External();
            ws.Url = sURL;
            object[] obj = ws.LogoutUser(sSessionId, sUserId);
            return Convert.ToBoolean(obj[0]);
        }

        public string GetFileUploadURL()
        {
            string sURL;
            switch(iEnvironment)
            {
                case 0:
                    sURL = "http://silcar-ws21.silcar.com.au:8003/FileServer.aspx";
                    break;
                case 1:
                    sURL = "http://silcar-ws11.silcar.com.au:8003/FileServer.aspx";
                    break;
                case 2:
                    if (m_iSecureFlag == 0)
                    {
                        sURL = "http://scms.silcar.com.au/FileServer.aspx";
                    }
                    else
                    {
                        sURL = "https://scms.silcar.com.au/FileServer.aspx";
                    }
                    break;
                default:
                    sURL = "http://silcar-ws11.silcar.com.au:8003/FileServer.aspx";
                    break;

            }

            return sURL;
        }

        public bool IsNumeric(System.Object Expression)
        {
            if (Expression == null || Expression is DateTime)
                return false;

            if (Expression is Int16 || Expression is Int32 || Expression is Int64 || Expression is Decimal || Expression is Single || Expression is Double || Expression is Boolean)
                return true;

            try
            {
                if (Expression is string)
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

        public string Get_Tag_Name_Prefix(string sTag)
        {
            string sPrefix;
            int iUnder = 0;

            iUnder = sTag.IndexOf("_");

            if (iUnder <= 0)
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
                if (int.TryParse(sPrefix, out iReturn))
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

        public string StripMultiSlashes(string sInputString)
        {
            string sReturn;

            sReturn = sInputString.Replace("http://", "http:!!");
            sReturn = sReturn.Replace("https://", "https:!!");
            sReturn = sReturn.Replace("///////", "/");
            sReturn = sReturn.Replace("//////", "/");
            sReturn = sReturn.Replace("/////", "/");
            sReturn = sReturn.Replace("////", "/");
            sReturn = sReturn.Replace("///", "/");
            sReturn = sReturn.Replace("//", "/");
            sReturn = sReturn.Replace("http:!!", "http://");
            sReturn = sReturn.Replace("https:!!", "https://");

            return sReturn;
        }

        public string StripMultiBackslashes(string sInputString)
        {
            string sReturn;

            if (sInputString.StartsWith(@"\\"))
            {
                sReturn = @"!!" + sInputString.Substring(2);
            }
            else
            {
                sReturn = sInputString;
            }
            sReturn = sReturn.Replace(@"\\\\\\\", @"\");
            sReturn = sReturn.Replace(@"\\\\\\", @"\");
            sReturn = sReturn.Replace(@"\\\\\", @"\");
            sReturn = sReturn.Replace(@"\\\\", @"\");
            sReturn = sReturn.Replace(@"\\\", @"\");
            sReturn = sReturn.Replace(@"\\", @"\");

            return sReturn;
        }


        public ArrayList Get_Root_Info(int iRootSiteId)
        {
            ArrayList arrReturn = new ArrayList();
            clsLocalUtils clsUtil = new clsLocalUtils();
            int iEnvironment = clsUtil.iEnvironment;

            switch (iEnvironment)
            {
                case 0:
                    switch (iRootSiteId)
                    {
                        case 1:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra/");
                            break;
                        case 2:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/NonTelstra/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/NonTelstra/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/NonTelstra/");
                            break;
                        case 3:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/SCMS/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/dc/SCMS/");
                            arrReturn.Add(1);
                            arrReturn.Add("SCMS");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/");
                            break;
                        case 4:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/businessdevelopment/tendermanagement/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("businessdevelopment/tendermanagement/");
                            arrReturn.Add(2);
                            arrReturn.Add("businessdevelopment/tendermanagement");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/");
                            break;
                        case 5:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2011/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2011/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2011/");
                            break;
                        case 6:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2012/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2012/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2012/");
                            break;
                        case 7:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2013/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2013/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2013/");
                            break;
                        case 8:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2014/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2014/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2014/");
                            break;
                        case 33:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/dc/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/");
                            break;
                        default:
                            break;
                    }
                    break;

                case 1:
                    switch (iRootSiteId)
                    {
                        case 1:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra/");
                            break;
                        case 2:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/NonTelstra/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/NonTelstra/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/NonTelstra/");
                            break;
                        case 3:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/SCMS/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/dc/SCMS/");
                            arrReturn.Add(1);
                            arrReturn.Add("SCMS");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/");
                            break;
                        case 4:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/businessdevelopment/tendermanagement/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("businessdevelopment/tendermanagement/");
                            arrReturn.Add(2);
                            arrReturn.Add("businessdevelopment/tendermanagement");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/");
                            break;
                        case 5:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2011/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2011/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2011/");
                            break;
                        case 6:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2012/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2012/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2012/");
                            break;
                        case 7:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2013/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2013/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2013/");
                            break;
                        case 8:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2014/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2014/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/Telstra_2014/");
                            break;
                        case 33:
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/dc/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://silcar-sp11.silcar.com.au/units/dc/");
                            break;
                        default:
                            break;
                    }
                    break;

                case 2:
                    switch (iRootSiteId)
                    {
                        case 1:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra/");
                            break;
                        case 2:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/NonTelstra/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/NonTelstra/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/NonTelstra/");
                            break;
                        case 3:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/dc/SCMS/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/dc/SCMS/");
                            arrReturn.Add(1);
                            arrReturn.Add("SCMS");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/dc/");
                            break;
                        case 4:
                            arrReturn.Add("http://mysilcar.silcar.com.au/businessdevelopment/tendermanagement/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("businessdevelopment/tendermanagement/");
                            arrReturn.Add(2);
                            arrReturn.Add("businessdevelopment/tendermanagement");
                            arrReturn.Add("http://mysilcar.silcar.com.au/");
                            break;
                        case 5:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2011/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2011/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2011/");
                            break;
                        case 6:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2012/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2012/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2012/");
                            break;
                        case 7:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2013/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2013/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2013/");
                            break;
                        case 8:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2014/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/Telstra_2014/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/Telstra_2014/");
                            break;
                        case 33:
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/dc/");
                            arrReturn.Add(sUser);
                            arrReturn.Add(sPassword);
                            arrReturn.Add(sDomain);
                            arrReturn.Add("units/dc/");
                            arrReturn.Add(1);
                            arrReturn.Add("");
                            arrReturn.Add("http://mysilcar.silcar.com.au/units/dc/");
                            break;
                        default:
                            break;
                    }
                    break;


            }

            return arrReturn;
        }

        public string GetURLPrefix(string sPath, int iRootSiteId)
        {
            string sURLString;
            ArrayList sAdminInfo = new ArrayList();

            sAdminInfo = Get_Root_Info(iRootSiteId);
            if (sPath.ToString().StartsWith("/"))
            {
                sPath = sPath.Substring(1, sPath.Length - 1);
            }

            if (sPath.ToString().EndsWith("/"))
            {
                sPath = sPath.Substring(0, sPath.Length - 1);
            }

            if (sAdminInfo[0].ToString().EndsWith("/"))
            {
                sAdminInfo[0] = sAdminInfo[0].ToString().Substring(0, sAdminInfo[0].ToString().Length - 1);
            }

            if (sPath == "")
            {
                sURLString = sAdminInfo[0].ToString() + "/";
            }
            else
            {
                sURLString = sAdminInfo[0].ToString() + "/" + sPath + "/";
            }

            //Get rid of any double slashes
            sURLString = StripMultiSlashes(sURLString);

            return sURLString;

        }

        //You cannot upload directly to sharepoint unless you are on the same domain. So won't work for mobility
        ////sFileUrl = the source file you want to upload (eg J:/Design Tool/General/temp/file1234.xls)
        ////sSiteUrl = the destination library or folder on Sharepoint you want to upload into (eg Telstra/CP20002200/SP10156011/WO862/Design)
        ////sDocument is the document folder on Sharepoint you want to upload into (eg Shared Documents)
        //public bool WSSUploadFile2DocumentLib(string sFileUrl, string sSiteUrl, string sDocument, int iRootSiteId, ref string sRtnMsg)
        //{            
        //    Copy SPCopy = new Copy();
        //    int iLastSlash;
        //    string sServerName;
        //    ArrayList sAdminInfo = new ArrayList();
        //    NetworkCredential sCred = new NetworkCredential();


        //    if (!File.Exists(sFileUrl))
        //    {
        //        return false;
        //    }

        //    FileStream fStream = File.OpenRead(sFileUrl);
        //    string sfileName;

        //    //Get the actual file name itself from the full path
        //    sfileName = fStream.Name;
        //    iLastSlash = sfileName.LastIndexOf(@"/");
        //    sfileName = sfileName.Substring(iLastSlash + 1);

        //    byte[] contents = new byte[fStream.Length];
        //    fStream.Read(contents, 0, Convert.ToInt32(fStream.Length));
        //    fStream.Close();

        //    SPCopy.PreAuthenticate = true;
        //    sCred.UserName = sUser;
        //    sCred.Password = sPassword;
        //    //sCred.Domain = sDomain;

        //    sAdminInfo = Get_Root_Info(iRootSiteId);
        //    SPCopy.Credentials = sCred;
        //    sServerName = sAdminInfo[0].ToString();
        //    SPCopy.Url = GetURLPrefix(sSiteUrl, iRootSiteId) + "_vti_bin/Copy.asmx";

        //    FieldInformation F1 = new FieldInformation();

        //    FieldInformation[] myFieldInfoArray = { F1 };
        //    string sDestURL = sServerName + "/" + sSiteUrl + "/" + sDocument + "/" + sfileName;
        //    sDestURL = StripMultiSlashes(sDestURL);
        //    string[] myDestinationArray = { sDestURL };
        //    CopyResult MyRslt = new CopyResult();
        //    CopyResult[] MyRsltArray = { MyRslt };

        //    try
        //    {
        //        System.UInt32 myCopyUint = SPCopy.CopyIntoItems(sFileUrl, myDestinationArray, myFieldInfoArray, contents, out MyRsltArray);
        //        return true;
        //    }
        //    catch (Exception ex)
        //    {
        //        sRtnMsg = ex.Message.ToString();
        //        return false;
        //    }

        //}

            public bool UploadFileToDTNetwork(string sSourcePathAndFilename, string sID, string sTargetRelativePath, string sTargetFileNameOnly, string sSubSiteName, string sSubSiteFolder, ref string sRtnMessage)
            {
            
                WebClient wc = new WebClient();
                string sTargetFile;
                string sTargetURLPath;
                ArrayList arrCred = new ArrayList();
                NetworkCredential sCred = new NetworkCredential(sDTUser, sDTPassword);

                try
                {
                    if (sTargetFileNameOnly.Contains(@"/") || sTargetFileNameOnly.Contains(@"\"))
                    {
                        sRtnMessage = "The target file name must be a file name only and NOT include any path";
                        return false;
                    }
                    else
                    {
                        sTargetRelativePath = sTargetRelativePath.Trim();
                        sTargetRelativePath = sTargetRelativePath.Replace(@"/", @"\");
                        if(!sTargetRelativePath.EndsWith(@"\"))
                        {
                            sTargetRelativePath = sTargetRelativePath + @"\";
                        }
                        sTargetURLPath = GetFileUploadURL();
                        sTargetFile = sTargetURLPath + "?SaveType=2&FilePath=" + sTargetRelativePath + "&ProjectId=" + sID + "&SubSiteName=" + sSubSiteName + "&SubSiteFolder=" + sSubSiteFolder;


                        wc.Credentials = sCred;
                        wc.Encoding = System.Text.Encoding.ASCII;
                        wc.Headers.Add("Content-Type", "application/x-www-form-urlencoded");
                        wc.UploadFile(sTargetFile, "POST", sSourcePathAndFilename);
                        wc.Dispose();
                        return true;
                    }
                }
                catch( Exception ex)
                {
                    sRtnMessage = ex.Message.ToString() + "Stack Trace: " + ex.StackTrace.ToString();
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

            bReturn = DateTime.TryParseExact(sDate, formats, new System.Globalization.CultureInfo("en-AU"), System.Globalization.DateTimeStyles.None, out dateValue);
            dtReturnDate = dateValue;
            return bReturn;
        }

        public DateTime GetDateFromString(string sDate, string sFormat)
        {
            if(sFormat.Contains("/"))
            {
                string[] sDateInfo = sDate.Split('/');
                int iYearSlash = Convert.ToInt32(sDateInfo[2]);
                int iMonthSlash = Convert.ToInt32(sDateInfo[1]);
                int iDaySlash = Convert.ToInt32(sDateInfo[0]);
                if (sFormat.ToUpper() == "MM/DD/YYYY")
                {
                    iMonthSlash = Convert.ToInt32(sDateInfo[0]);
                    iDaySlash = Convert.ToInt32(sDateInfo[1]);
                }
                if (sFormat.ToUpper() == "YYYY/MM/DD")
                {
                    iMonthSlash = Convert.ToInt32(sDateInfo[0]);
                    iDaySlash = Convert.ToInt32(sDateInfo[1]);
                }
                DateTime dateSlash = new DateTime(iYearSlash, iMonthSlash, iDaySlash); //Months start at 0
                return dateSlash;
            }

            DateTime dtRtn = Convert.ToDateTime(sDate);
            return dtRtn;
        }

    }

    public class clsAttachWBS
    {
        public object[] GetWBSMethods(string sSessionId, string sUser, string sWBSURL)
        {
            clsLocalUtils util = new clsLocalUtils();
            string sURL = util.GetEnvironment_wbsURL("wbsITP_External");
            wbsITP_External ws = new wbsITP_External();
            ws.Url = sURL;
            object[] objMethods = ws.wbsGetWebMethodsOfWebService(sSessionId, sUser, sWBSURL);

            return objMethods;
        }
    }

}










