using System;
using System.IO;
using System.Data;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;

using MySql.Data.MySqlClient;
using MathNet.Numerics;

﻿﻿namespace TableVerification
{
    public struct FlaggingProfile
    {
        public string ProfileColumnName;
        public string SearchAxis;
        public string FilesLocation;
        public Dictionary<string, float> TableParams;
        public float[] flaggingProfileValues;
        public float[] existentSearchAxisValues;
    }

    public class YamlConfig
    {
        public Dictionary<string, Object> table_axes {get; set;}
    }

    public class TableVerificationTool
    {
        public T[] GetColumnFromTable<T>(in DataTable dataTable, in string columnName)
        {
            var column = new T[dataTable.Rows.Count];
            int i = 0;
            foreach(DataRow row in dataTable.Rows)
            {
                column[i] = (T)row[columnName];
                i++;
            }

            return column;
        }

        public float[] GetFloatColumnFromTable(in DataTable dataTable, in string columnName)
        {
            var column = new float[dataTable.Rows.Count];
            int i = 0;
            foreach(DataRow row in dataTable.Rows)
            {
                column[i] = (float)(double)row[columnName];
                i++;
            }

            return column;
        }

        protected string getTableAxesYamlFilename(in string tableName)
        {
            string getTableAxesYamlFilenameCommandString =
             $"SELECT YamlFilename, FilesLocation FROM {_databaseName}." + _metadataTableName + " WHERE TableName = '" + tableName + "'";
            
            using var connection = new MySqlConnection(_connectionString);
            connection.Open();
            using var getTableAxesYamlFilenameCommand = new MySqlCommand(getTableAxesYamlFilenameCommandString, connection);

            using MySqlDataReader reader = getTableAxesYamlFilenameCommand.ExecuteReader();

            reader.Read();
            var tableAxesYamlFilename = reader.GetString("YamlFilename");
            var fileLocation = reader.GetString("FilesLocation");
            fileLocation = fileLocation.Substring(0, fileLocation.Length - 6);

            return Path.Combine(Environment.GetEnvironmentVariable("FS_ARCHIVE_ROOT"), fileLocation, tableAxesYamlFilename);
        }

        protected Dictionary<string, float[]> getTableAxesValues(in string tableName)
        {

            string tableAxesYamlFilename = getTableAxesYamlFilename(tableName);

            var deserializer = new YamlDotNet.Serialization.DeserializerBuilder().WithNamingConvention(YamlDotNet.Serialization.NamingConventions.UnderscoredNamingConvention.Instance).Build();

            string yamlText = "";

            Dictionary<string, float[]> tableAxesValues = new Dictionary<string, float[]>();

            if(File.Exists(tableAxesYamlFilename)){
                yamlText = File.ReadAllText(tableAxesYamlFilename);
            }else{
                Console.WriteLine("ERROR yaml file to read table axes cannot be found: ", tableAxesYamlFilename);
                return tableAxesValues;
            }

            yamlText = yamlText.Substring(yamlText.IndexOf("table_axes:"), yamlText.IndexOf("num_equilibria:") - yamlText.IndexOf("table_axes:"));
            var content = deserializer.Deserialize<YamlConfig>(yamlText);

            foreach(var pair in content.table_axes){
                if(pair.Value.GetType() == typeof(String)){
                    // this is NevinsB: NevinsA which we can just skip
                    continue;
                }
                else if(pair.Value.GetType() == typeof(List<Object>)){
                    var obj_list = (List<Object>)pair.Value;
                    var vals = new float[obj_list.Count];
                    int i = 0;
                    foreach(var val in obj_list){
                        if(val.GetType() == typeof(String)){
                            vals[i] = Convert.ToSingle(val);
                            i++;
                        }
                    }
                    var key = pair.Key;
                    if(pair.Key == "CurrentRatio"){
                        for(int j = 0; j<vals.Count(); j++){
                            vals[j] = Convert.ToSingle(Math.Round(vals[j] * 1e6, 0));
                        }
                        key = "Ipl_setpoint";
                    }
                    Array.Sort(vals); // sort in ascending order
                    tableAxesValues.Add(key, vals);
                }
                else{
                    Console.WriteLine("ERROR unexpected type encountered reading yaml file");
                }
            }

            return tableAxesValues;
        }

        protected const string _connectionString = @"server=gfyvrmysql01.gf.local; userid=RSB; password=; database=GradShafranov";
        protected const string _databaseName = "gradshafranov";
        protected const string _metadataTableName = "lut_metadata";
    }

    /*
    Class used for performing table verification tasks. Two main functionalities are: calculating best to average point removed fit quality (BTAPRFQ), 
        and checking for holes (table axis value combinations which do not exist)
    */
    public class TableVerifier : TableVerificationTool
    {
        // public static void Main()
        // {
        //     string columns_of_interest = "q020,q050,q080,WBPolNoDCInFC,phiPlInFC,B161087,B211100,B291060,B215008,B261008,B291008,B261087";
        //     string tableName = "pi3b_v25_pfc17500_2022-05-27";
        //     TableVerifier verifier = new TableVerifier(tableName, columns_of_interest);

        //     var sw = new Stopwatch();
        //     sw.Start();

        //     // Check profiles
        //     // verifier.GenerateProfileScores();
        //     // Console.WriteLine($"Took {sw.ElapsedMilliseconds} to generate profile scores");
        //     // sw.Stop();
        //     // var failing_profiles = verifier.GetFlaggingProfiles(0.3);

        //     // Check holes
        //     DataTable failing = verifier.CheckForHoles();
        //     sw.Stop();
        //     Console.WriteLine($"{sw.ElapsedMilliseconds}");

        //     // Test that all holes at least don't have their values in the table
        //     foreach(DataRow row in failing.Rows)
        //     {
        //         string selectString = "";
        //         foreach(string searchAxisName in verifier.TableAxesValues.Keys)
        //         {
        //             if(searchAxisName != (string)row[SearchAxisColumnName]){
        //                 var val = Convert.ToSingle(row[searchAxisName]);
        //                 if(searchAxisName != "Ipl_setpoint"){
        //                     val = (float)Math.Round(val * 100f) / 100f;
        //                 }
        //                 selectString += $"{searchAxisName} = {val} AND ";

        //             }
        //         }
        //         selectString = selectString.Substring(0, selectString.Count()-5);
        //         DataRow[] selectedRows = verifier.GetDataTable().Select(selectString);

        //         var selectTable = verifier.GetDataTable().Clone();
        //         foreach(DataRow roww in selectedRows)
        //             selectTable.ImportRow(roww);

        //         string offendingSearchAxisName = Convert.ToString(row[SearchAxisColumnName]);
        //         float[] axisValues = selectTable.AsEnumerable().Select(s => (float)s.Field<double>(offendingSearchAxisName)).ToArray();
        //         if(! axisValues.Contains(Convert.ToSingle(row[offendingSearchAxisName]))){
        //             Console.WriteLine("miss");
        //         }else{
        //             Console.WriteLine("hit");
        //         }
        //     }
        // }

        /*
        @brief: Populates TableAxesValues from yaml file and loads all table axis and profile columns from the SQL table
        @param: profileNamesOfInterest comma-seperated string containing column names in the SQL table that will be
                    checked for best to average point removed fit quality
        */
        public TableVerifier(string tableName, string profileNamesOfInterest)
        {
            TableName = tableName;
            ProfileNamesOfInterest = profileNamesOfInterest;

            TableAxesValues = getTableAxesValues(TableName);
            loadTable();
        }

        #region Getters
        public DataTable GetDataTable()
        {
            return _dataTable;
        }

        public DataTable GetProfileScoreTable()
        {
            return _profileScoresDataTable;
        }

        public int GetNumRowsInDatatable()
        {
            return _dataTable.Rows.Count;
        }
        #endregion

        /*
        @brief: Checks for combinations of table axis values which are not present in the table
        @returns: Datatable containing table axis combinations which are not present in the table. Has a column for each table axis.
        */
        public DataTable CheckForHoles()
        {
            string[] tableAxesNames = TableAxesValues.Keys.ToArray();
            DataTable justTableAxesTable = new DataView(_dataTable).ToTable(false, tableAxesNames);

            DataTable holeAxesValuesTable = justTableAxesTable.Clone();
            holeAxesValuesTable.Columns.Add(SearchAxisColumnName, typeof(string));

            holeAxesValuesTable.BeginLoadData();

            var tasks = new List<Task>();
            foreach(string searchAxisName in TableAxesValues.Keys.ToArray())
            {
                tasks.Add(Task.Factory.StartNew(() => checkForHolesHelper(searchAxisName, justTableAxesTable, ref holeAxesValuesTable)));
            }
            Task.WaitAll(tasks.ToArray());

            holeAxesValuesTable.EndLoadData();
            holeAxesValuesTable.AcceptChanges();

            return holeAxesValuesTable;
        }

        private void checkForHolesHelper(in string searchAxisName, in DataTable justTableAxesTable, ref DataTable holeAxesValuesTable)
        {
            var nonSearchTableAxes = new Dictionary<string, float[]>();

            foreach(var pair in TableAxesValues)
            {
                if(pair.Key == searchAxisName) continue;
                nonSearchTableAxes.Add(pair.Key, pair.Value);
            }

            string[] nonSearchAxisNames = nonSearchTableAxes.Keys.ToArray();
            
            // Check all combinations of table parameters for the current search axis
            foreach(var val0 in nonSearchTableAxes[nonSearchAxisNames[0]])
            {
                var arr0 = justTableAxesTable.Select($"{nonSearchAxisNames[0]} = {val0}");
                if(arr0.Length == 0){
                    continue;
                }
                
                var select0 = justTableAxesTable.Clone();
                foreach(var row in arr0)
                    select0.ImportRow(row);

            foreach(var val1 in nonSearchTableAxes[nonSearchAxisNames[1]])
            {
                var arr1 = select0.Select($"{nonSearchAxisNames[1]} = {val1}");
                if(arr1.Length == 0){
                    continue;
                }
                
                var select1 = justTableAxesTable.Clone();
                foreach(var row in arr1)
                    select1.ImportRow(row);

            foreach(var val2 in nonSearchTableAxes[nonSearchAxisNames[2]])
            {
                var arr2 = select1.Select($"{nonSearchAxisNames[2]} = {val2}");
                if(arr2.Length == 0){
                    continue;
                }
                
                var select2 = justTableAxesTable.Clone();
                foreach(var row in arr2)
                    select2.ImportRow(row);

            foreach(var val3 in nonSearchTableAxes[nonSearchAxisNames[3]])
            {
                var arr3 = select2.Select($"{nonSearchAxisNames[3]} = {val3}");
                if(arr3.Length == 0){
                    continue;
                }
                
                var select3 = justTableAxesTable.Clone();
                foreach(var row in arr3)
                    select3.ImportRow(row);

            foreach(var val4 in nonSearchTableAxes[nonSearchAxisNames[4]])
            {
                var arr4 = select3.Select($"{nonSearchAxisNames[4]} = {val4}");
                if(arr4.Length == 0){
                    continue;
                }
                
                var select4 = justTableAxesTable.Clone();
                foreach(var row in arr4)
                    select4.ImportRow(row);

            foreach(var val5 in nonSearchTableAxes[nonSearchAxisNames[5]])
            {
                var arr5 = select4.Select($"{nonSearchAxisNames[5]} = {val5}");
                if(arr5.Length == 0)
                {
                    continue;
                }
                
                var singleSearchAxisDataTable = justTableAxesTable.Clone();
                foreach(var row in arr5)
                    singleSearchAxisDataTable.ImportRow(row);

                var searchAxisNameCopy = searchAxisName;

                singleSearchAxisDataTable.DefaultView.Sort = $"{searchAxisName} asc";
                singleSearchAxisDataTable = singleSearchAxisDataTable.DefaultView.ToTable();

                float[] searchAxisValues = TableAxesValues[searchAxisName];

                float[] existentSearchAxisVals = GetFloatColumnFromTable(singleSearchAxisDataTable, searchAxisName);
                var dataExistsAtSearchAxisValue = new bool[searchAxisValues.Count()];
                for(int i = 0; i < searchAxisValues.Count(); i++)
                {
                    dataExistsAtSearchAxisValue[i] = existentSearchAxisVals.Contains(searchAxisValues[i]);
                }

                bool possiblyInHole = false;
                for(int i = 1; i < dataExistsAtSearchAxisValue.Count(); i++)
                {
                    if(dataExistsAtSearchAxisValue[i] && possiblyInHole)
                    {
                        possiblyInHole = false;
                        _mtx.WaitOne();
                        {
                            var holeRow = holeAxesValuesTable.NewRow();
                            holeRow[nonSearchAxisNames[0]] = val0;
                            holeRow[nonSearchAxisNames[1]] = val1;
                            holeRow[nonSearchAxisNames[2]] = val2;
                            holeRow[nonSearchAxisNames[3]] = val3;
                            holeRow[nonSearchAxisNames[4]] = val4;
                            holeRow[nonSearchAxisNames[5]] = val5;
                            var hole_val = searchAxisValues[i-1];
                            holeRow[searchAxisName] = searchAxisValues[i-1];
                            holeRow[SearchAxisColumnName] = searchAxisName;

                            holeAxesValuesTable.Rows.Add(holeRow);
                        }
                        _mtx.ReleaseMutex();
                    }else if(! dataExistsAtSearchAxisValue[i] && dataExistsAtSearchAxisValue[i-1])
                    {
                        possiblyInHole = true;
                    }
                }
            }}}}}}
        }

        /*
        @brief: Stores a best to average point removed fit quality (BTAPRFQ) score for each profile for each table axis combination
        @returns: Nothing, but populates _profileScoresDataTable DataTable, which has columns for each table axis, each profile to
                    store their BTAPRFQ score, and another set of columns for each profile to store
                    the search axis value which resulted in the highest BTAPRFQ score
        @detail: Columns with the search axis value resulting in highest BTAPRFQ scores have names: ~profile name~ + '_offending_val'
        @detail: Profile scores can be acquired through GetProfileScoreTable
        @detail: Ignores NevinsN for speed of execution purposes. Just uses a single NevinsN value defined by index _nevinsNIndex
        */
        public void GenerateProfileScores()
        {
            var tableKeys = TableAxesValues.Keys.ToArray();

            // Add column to table to store the search axis value which resulted in highest BTAPRFQ
            _profileScoresDataTable = _dataTable.Clone();
            foreach(var col in ProfileNamesOfInterest.Split(','))
            {
                _profileScoresDataTable.Columns.Add(col + OffendingValueSuffix, typeof(float));
            }

            // Ignore all cases with no dc flux, all NevinsN values except 1
            var singleNevinsNRows = _dataTable.Select($"NevinsN = {TableAxesValues["NevinsN"][_nevinsNIndex]} AND psieq_dc <> 0");
            _singleNevinsNDataTable = _dataTable.Clone();

            foreach(var row in singleNevinsNRows)
            {
                _singleNevinsNDataTable.ImportRow(row);
            }
            
            _profileScoresDataTable.BeginLoadData();

            // Calculate BTAPRFQ scores for each search axis
            var tasks = new List<Task>();
            foreach(var searchAxisName in tableKeys)
            {
                if(searchAxisName == "NevinsN")
                    continue;
                
                tasks.Add(Task.Factory.StartNew(() => generateProfileScoresHelper(searchAxisName)));
            }
            
            Task.WaitAll(tasks.ToArray());

            _profileScoresDataTable.EndLoadData();
            _profileScoresDataTable.AcceptChanges();
        }

        /*
        @brief: Determines which profiles for which table axis combinations have best to average point removed fit
                    quality (BTAPRFQ) scores higher than an inputted threshold
        @returns: List<FlaggingProfiles> denoting which profiles and table axis combinations are flagged
        @detail: Multiple FlaggingProfiles may have the same table axes combinations as different profiles could be
                    flagged for the same combination
        */
        public List<FlaggingProfile> GetFlaggingProfiles(double threshold){
            var flaggingProfiles = new List<FlaggingProfile>();

            if(! _profileScoresDataTable.IsInitialized)
            {
                Console.WriteLine("Must generate profile scores before getting flagging profiles");
                return flaggingProfiles;
            }

            var columnNames = ProfileNamesOfInterest.Split(',');
            foreach(var columnName in columnNames)
            {
                var flaggingRows = _profileScoresDataTable.Select($"{columnName} > {threshold}");
                foreach(var flaggingRow in flaggingRows)
                {
                    var flaggingProfile = new FlaggingProfile();

                    string searchAxisName = "";
                    string selectProfileString = "";
                    var tableParams = new Dictionary<string, float>();
                    foreach(var axisName in TableAxesValues.Keys){
                        
                        if(flaggingRow[axisName] == DBNull.Value){
                            searchAxisName = axisName;
                            tableParams.Add(axisName, (float)flaggingRow[columnName + OffendingValueSuffix]);
                        }else{
                            var tableAxisValue = (float)(double)flaggingRow[axisName];
                            tableParams.Add(axisName, tableAxisValue);
                            selectProfileString += $"{axisName} = {tableAxisValue} AND ";
                        }   
                    }
                    selectProfileString = selectProfileString.Substring(0, selectProfileString.Count() - 5);

                    var temp_rows = _singleNevinsNDataTable.Select(selectProfileString);
                    DataTable temp_dt = _singleNevinsNDataTable.Clone();
                    foreach(var row in temp_rows)
                    {
                        temp_dt.ImportRow(row);
                    }

                    float[] flaggingProfileValues = GetFloatColumnFromTable(temp_dt, columnName);
                    float[] existentSearchAxisValues = GetFloatColumnFromTable(temp_dt, searchAxisName);   

                    flaggingProfile.SearchAxis = searchAxisName;
                    flaggingProfile.ProfileColumnName = columnName;
                    flaggingProfile.TableParams = tableParams;
                    flaggingProfile.FilesLocation = GetFilesLocation();
                    flaggingProfile.flaggingProfileValues = flaggingProfileValues;
                    flaggingProfile.existentSearchAxisValues = existentSearchAxisValues;

                    flaggingProfiles.Add(flaggingProfile);
                }
            }
            
            return flaggingProfiles;
        }

        private void generateProfileScoresHelper(in string searchAxisName)
        {
            string[] profileNames = ProfileNamesOfInterest.Split(',');

            var nonSearchTableAxes = new Dictionary<string, float[]>();

            foreach(var pair in TableAxesValues){
                if(pair.Key == searchAxisName || pair.Key == "NevinsN") continue;
                nonSearchTableAxes.Add(pair.Key, pair.Value);
            }

            string[] nonSearchAxisNames = nonSearchTableAxes.Keys.ToArray();
            if(nonSearchAxisNames.Count() != 5){ // everything but NevinsN and search axis
                Console.WriteLine("ERROR producing non-search axes for checking profiles");
                return;
            }

            // Select table values corresponding to non-search axis values
            foreach(var val0 in nonSearchTableAxes[nonSearchAxisNames[0]])
            {
                var arr0 = _singleNevinsNDataTable.Select($"{nonSearchAxisNames[0]} = {val0}");
                if(arr0.Length == 0){
                    continue;
                }
                
                var sort0 = _singleNevinsNDataTable.Clone();
                foreach(var row in arr0)
                    sort0.ImportRow(row);
            foreach(var val1 in nonSearchTableAxes[nonSearchAxisNames[1]])
            {
                var arr1 = sort0.Select($"{nonSearchAxisNames[1]} = {val1}");
                if(arr1.Length == 0){
                    continue;
                }
                
                var sort1 = _singleNevinsNDataTable.Clone();
                foreach(var row in arr1)
                    sort1.ImportRow(row);
            foreach(var val2 in nonSearchTableAxes[nonSearchAxisNames[2]])
            {
                var arr2 = sort1.Select($"{nonSearchAxisNames[2]} = {val2}");
                if(arr2.Length == 0){
                    continue;
                }
                
                var sort2 = _singleNevinsNDataTable.Clone();
                foreach(var row in arr2)
                    sort2.ImportRow(row);
            foreach(var val3 in nonSearchTableAxes[nonSearchAxisNames[3]])
            {
                var arr3 = sort2.Select($"{nonSearchAxisNames[3]} = {val3}");
                if(arr3.Length == 0){
                    continue;
                }
                
                var sort3 = _singleNevinsNDataTable.Clone();
                foreach(var row in arr3)
                    sort3.ImportRow(row);
            foreach(var val4 in nonSearchTableAxes[nonSearchAxisNames[4]])
            {
                var arr4 = sort3.Select($"{nonSearchAxisNames[4]} = {val4}");
                if(arr4.Length == 0){
                    continue;
                }
                
                var profilesVersusSearchAxesDatatable = _singleNevinsNDataTable.Clone();
                foreach(var row in arr4)
                    profilesVersusSearchAxesDatatable.ImportRow(row);

                profilesVersusSearchAxesDatatable.DefaultView.Sort = $"{searchAxisName} asc";
                profilesVersusSearchAxesDatatable = profilesVersusSearchAxesDatatable.DefaultView.ToTable();

                string subtitle = "";

                Dictionary<string, double> nonSearchAxesValues = new Dictionary<string, double>(){
                    {nonSearchAxisNames[0], val0},
                    {nonSearchAxisNames[1], val1},
                    {nonSearchAxisNames[2], val2},
                    {nonSearchAxisNames[3], val3},
                    {nonSearchAxisNames[4], val4},
                    {"NevinsN", TableAxesValues["NevinsN"][_nevinsNIndex]}
                };

                string axesValuesString = "";
                int i = 0;
                foreach(var pair in nonSearchAxesValues){
                    axesValuesString += $"{pair.Key} = {pair.Value} ";
                    i++;
                    if(i%2 == 0){
                        axesValuesString += "<br>";
                    }
                }

                Dictionary<string, double[]> allProfileValues = new Dictionary<string, double[]>();
                foreach(string profileName in profileNames){
                    allProfileValues.Add(profileName, new double[profilesVersusSearchAxesDatatable.Rows.Count]);
                }

                int rowNum = 0;
                var searchAxisValues = new float[profilesVersusSearchAxesDatatable.Rows.Count];
                foreach(DataRow row in profilesVersusSearchAxesDatatable.Rows){
                    foreach(var profileName in profileNames){
                        if(row[profileName] != DBNull.Value){
                            allProfileValues[profileName][rowNum] = Convert.ToDouble(row[profileName]);
                        }else
                        {
                            allProfileValues[profileName][rowNum] = 0;
                        }
                    }
                    searchAxisValues[rowNum] = Convert.ToSingle(row[searchAxisName]);
                    rowNum ++;
                }

                // Necessary to treat NewRow operation as thread-unsafe
                _mtx.WaitOne();
                var profileScoreRow = _profileScoresDataTable.NewRow();
                _mtx.ReleaseMutex();
                
                // Set search axis value to NULL to denote which axis was the search axis
                profileScoreRow[searchAxisName] = DBNull.Value;
                foreach(var pr in nonSearchAxesValues){
                    profileScoreRow[pr.Key] = pr.Value;
                }

                var profileScores = new ConcurrentDictionary<string, float>();

                // Calculate and store BTAPRFQ for each profile
                var tasks = new List<Task>();
                string searchAxisNameCopy = searchAxisName;
                foreach(var profileName in profileNames)
                {
                    tasks.Add(Task.Factory.StartNew(() => 
                        fitHelper(allProfileValues[profileName], searchAxisValues, profileName, searchAxisNameCopy, ref profileScores, ref subtitle)));
                }

                Task.WaitAll(tasks.ToArray());

                foreach(var pair in profileScores){
                    profileScoreRow[pair.Key] = pair.Value;
                }
  
                _mtx.WaitOne();  
                {
                    _profileScoresDataTable.Rows.Add(profileScoreRow);
                }
                _mtx.ReleaseMutex();
            }}}}}
        }

        private void loadTable()
        {
            // string tableName = "xmltable.xml";
            // string schemaName = "schema.xsd";
            // if(File.Exists(tableName) && File.Exists(schemaName)){
            //     _dataTable.ReadXmlSchema(schemaName);
            //     _dataTable.ReadXml(tableName);
            //     return;
            // }

            var tableKeys = TableAxesValues.Keys.ToArray();

            var queryColumns = "";
            
            HashSet<string> tableColumnNames = getTableColumnNames();
            foreach(string columnOfInterest in ProfileNamesOfInterest.Split(',')){
                if(tableColumnNames.Contains(columnOfInterest))
                {
                    queryColumns += columnOfInterest + ",";
                }
            }

            ProfileNamesOfInterest = queryColumns.Substring(0, queryColumns.Count()-1);

            foreach(var tableKey in tableKeys){
                queryColumns += tableKey + ",";
            }
        
            queryColumns = queryColumns.Substring(0, queryColumns.Length-1);

            var getTableCommandString = $"SELECT {queryColumns} FROM {_databaseName}.`{TableName}`";
            using(var connection = new MySqlConnection(_connectionString))
            {
                connection.Open();
                using var getTableCommand = new MySqlCommand(getTableCommandString, connection);
                getTableCommand.CommandTimeout = 1000;
                _dataTable.Load(getTableCommand.ExecuteReader());
                // _dataTable.WriteXmlSchema(schemaName);
                // _dataTable.WriteXml(tableName);
                connection.Close();
            }
        }
            
        private void fitHelper(in double[] profileValues, in float[] searchAxisValues, in string profileName, in string searchAxisName, ref ConcurrentDictionary<string, float> profileScores, ref string subtitle)
        {
            (float metricScore, float offendingSearchAxisValue) = getBestToAveragePointRemovedFit(profileValues, searchAxisValues, ref subtitle);

            profileScores[profileName] = metricScore;
            profileScores[profileName + OffendingValueSuffix] = offendingSearchAxisValue;
        }

        private (float, float) getBestToAveragePointRemovedFit(in double[] profileValues, in float[] searchAxisValues, ref string subtitle)
        {
            if((profileValues.Max() - profileValues.Min()) < (_minProfileRange * profileValues.Max())) // if range is less than x% of range then don't bother checking
                return (0, 0);

            if(profileValues.Count() < 5) // After removing a point, need at least 4 values otherwise fit will be underconstrained or perfect
                return (0, 0);

            var xs = new double[profileValues.Count()-1];
            for(int i = 0; i < profileValues.Count()-1; i++)
                xs[i] = Convert.ToDouble(i);

            double[] pointRemovedQuadraticFitGoodness = new double[profileValues.Count()];

            for(int j = 0; j < profileValues.Count(); j++)
            {
                var dataWithValueRemoved = profileValues.Where((source, index) =>index != j).ToArray(); // could also try having that point as averge of two on either end
                
                double[] quadraticFitParams = Fit.Polynomial(xs, dataWithValueRemoved, 2);

                var quadraticFitQuality = GoodnessOfFit.RSquared(xs.Select(
                    x => quadraticFitParams[0] + quadraticFitParams[1]*x + quadraticFitParams[2]*Math.Pow(x, 2)), dataWithValueRemoved);
                                
                pointRemovedQuadraticFitGoodness[j] = quadraticFitQuality;
            }

            double meanFitQuality = pointRemovedQuadraticFitGoodness.Average();
            double BestToMeanFitDifference = pointRemovedQuadraticFitGoodness.Max() - meanFitQuality;

            var offendingValue = searchAxisValues[Array.IndexOf(pointRemovedQuadraticFitGoodness, pointRemovedQuadraticFitGoodness.Max())];

            return ((float)BestToMeanFitDifference, (float)offendingValue);
        }

        private bool quadraticFitChecker(double[] col, ref string subtitle)
        {
            var xs = new double[col.Count()];
            for(int i = 0; i < col.Count(); i++)
                xs[i] = Convert.ToDouble(i);

            double[] quadraticFitParams = Fit.Polynomial(xs, col, 2);
            var quadraticFitQuality = GoodnessOfFit.RSquared(xs.Select(
                x => quadraticFitParams[0] + quadraticFitParams[1]*x + quadraticFitParams[2]*Math.Pow(x, 2)), col);

            double fit_threshold = .6;
            if(quadraticFitQuality < fit_threshold){
                subtitle = $"Fit Goodness: {quadraticFitQuality}"; 
                return false;
            }

            return true;
        }

        private int getNumberOfTableRows()
        {
            string getNumberOfRowsCommandString = $"SELECT COUNT(*) FROM FROM {_databaseName}.`{TableName}`";

            using var connection = new MySqlConnection(_connectionString);
            connection.Open();
            using (var getNumberOfRowsCommand = new MySqlCommand(getNumberOfRowsCommandString, connection))
            {
                using var rdr = getNumberOfRowsCommand.ExecuteReader();
                rdr.Read();
                var ret = rdr.GetInt32(0);
                return ret;
            }
        }

        private HashSet<string> getTableColumnNames(){

            var tableColumnNames = new HashSet<string>();

            string getColumnNamesCommandString = $"SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='{_databaseName}' AND `TABLE_NAME`='{TableName}'";
            
            using var _connection = new MySqlConnection(_connectionString);
            _connection.Open();
            using(var getColumnsCommand = new MySqlCommand(getColumnNamesCommandString, _connection))
            {
                using var rdr = getColumnsCommand.ExecuteReader();
                while(rdr.Read()){
                    tableColumnNames.Add(rdr.GetString(0));
                }
            }

            return tableColumnNames;
        }

        public string GetFilesLocation()
        {
            string getFilesLoationCommandString =
             $"SELECT FilesLocation FROM {_databaseName}.{_metadataTableName} WHERE TableName = '{TableName}'";
            
            using var connection = new MySqlConnection(_connectionString);
            connection.Open();
            using var getFilesLoationCommand = new MySqlCommand(getFilesLoationCommandString, connection);

            using MySqlDataReader reader = getFilesLoationCommand.ExecuteReader();

            reader.Read();
            var filesLocation = reader.GetString("FilesLocation");

            return filesLocation;
        }

        #region Public Members
        public Dictionary<String, float[]> TableAxesValues = new Dictionary<string, float[]>();
        public string TableName;
        public string ProfileNamesOfInterest;
        public const string OffendingValueSuffix = "_offending_val";
        public const string SearchAxisColumnName = "search axis";
        #endregion

        #region Private Members
        
        // Contains all data from the table
        private DataTable _dataTable = new DataTable();

        // Has columns for each table axis, for each profile name denoting the best to mean fit difference, another for each
        // profile denoting the table axis value which resulted in the best fit with that point removed
        private DataTable _profileScoresDataTable;    
        private DataTable _singleNevinsNDataTable;
        private Mutex _mtx = new Mutex();
        private const int _nevinsNIndex = 1;
        private const double _minProfileRange = 2e-2; // Threshold for minimum profile range as fraction of the profiles maximum
        #endregion
    }

    public class TableComparer : TableVerificationTool
    {
        public static void Main()
        {
            // var tableComparer = new TableComparer("pi3b_v25_21oct-pfc_2022-03-03", "pi3b_v25_pfc17500_2022-05-27"); //1098254
            var tableComparer = new TableComparer("pi3b_asbuilt_pfc_g486a_2022-09-18", "pi3b_asbuilt_pfc17500ab_2022-06-09_b"); //122962 filenames diff
            // var tableComparer = new TableComparer("pi3b_asbuilt_pfc_g486a_2022-09-18_0", "pi3b_asbuilt_pfc_g486a_2022-09-18"); //1098254

            var sw = new Stopwatch();
            sw.Start();

            var differingFileNames = tableComparer.ViewDifferingFileNames();
            sw.Stop();
            var filenames = tableComparer.GetColumnFromTable<string>(differingFileNames, TableComparer.FileNameColumnName);
            var ownerTable = tableComparer.GetColumnFromTable<string>(differingFileNames, TableComparer.OwnerTableColumnName);
            var n = filenames.Count();
            
            // DataTable dt = tableComparer.getColumnDifferences("q020, q050");
            // sw.Stop();
            // int n = dt.Rows.Count;
        }

        public TableComparer(string table1Name, string table2Name)
        {
            Table1Name = table1Name;
            Table2Name = table2Name;
        }

        public DataTable getColumnDifferences(in string columns)
        {
            string noWhiteSpaceColumns = columns.Replace(" ", string.Empty);
            string[] tableNames = new string[] {Table1Name, Table2Name};

            DataTable table1 = new DataTable();
            DataTable table2 = new DataTable();
            DataTable[] dataTables = new DataTable[] {table1, table2};

            for(int i = 0; i < tableNames.Count(); i++)
            {
                string selectTableColumnsCommandString = $"SELECT {columns}, {FileNameColumnName} FROM {_databaseName}.`{tableNames[i]}`";
                using(var connection = new MySqlConnection(_connectionString))
                {
                    connection.Open();
                    using var selectTableColumnsCommand = new MySqlCommand(selectTableColumnsCommandString, connection);
                    selectTableColumnsCommand.CommandTimeout = 200;
                    dataTables[i].Load(selectTableColumnsCommand.ExecuteReader());
                }
            }

            table1.PrimaryKey = new DataColumn[] {table1.Columns[FileNameColumnName]};
            table2.PrimaryKey = new DataColumn[] {table2.Columns[FileNameColumnName]};

            string[] table1FileNamesWDate = GetColumnFromTable<string>(table1, FileNameColumnName);
            string[] table2FileNamesWDate = GetColumnFromTable<string>(table2, FileNameColumnName);

            string[] table1FileNames = new string[table1FileNamesWDate.Count()];
            string[] table2FileNames = new string[table2FileNamesWDate.Count()];

            int table1DateSuffixLength = getFileSuffixLength(table1FileNamesWDate[0]);
            int table2DateSuffixLength = getFileSuffixLength(table1FileNamesWDate[1]);

            string table1DateSuffix = table1FileNamesWDate[0].Substring(table1FileNamesWDate[0].Count() - table1DateSuffixLength, table1DateSuffixLength);
            string table2DateSuffix = table2FileNamesWDate[0].Substring(table2FileNamesWDate[0].Count() - table2DateSuffixLength, table2DateSuffixLength);

            for(int i = 0; i<table1FileNamesWDate.Count(); i++){
                table1FileNames[i] = table1FileNamesWDate[i].Substring(0, table1FileNamesWDate[i].Count() - table1DateSuffixLength);
            }

            for(int i = 0; i<table2FileNamesWDate.Count(); i++){
                table2FileNames[i] = table2FileNamesWDate[i].Substring(0, table2FileNamesWDate[i].Count() - table2DateSuffixLength);
            }

            var table1FileNamesSet = new HashSet<string>(table1FileNames);
            var table2FileNamesSet = new HashSet<string>(table2FileNames);

            var sharedFileNames = table1FileNames.Intersect(table2FileNames); 

            const string table1FirstDifferingFileNameColumnName = "Table1FirstDiffering" + FileNameColumnName;
            const string table2FirstDifferingFileNameColumnName = "Table2FirstDiffering" + FileNameColumnName;
            const string numberOfDifferingColumnEntriesColumnName = "NumberOfDifferingEntries";
            const string averageDifferenceColumName = "AverageDifference";
            const string comparingColumnNameColumnName = "ColumnName";

            DataTable differingValues = new DataTable();
            differingValues.Columns.Add(table1FirstDifferingFileNameColumnName, typeof(string));
            differingValues.Columns.Add(table2FirstDifferingFileNameColumnName, typeof(string));
            differingValues.Columns.Add(numberOfDifferingColumnEntriesColumnName, typeof(float));
            differingValues.Columns.Add(averageDifferenceColumName, typeof(float));
            differingValues.Columns.Add(comparingColumnNameColumnName, typeof(string));

            foreach(string columnName in noWhiteSpaceColumns.Split(","))
            {
                int numberDiffering = 0;
                float summedDifference = 0;
                string firstDifferingFileName = "";
                foreach(var fileName in sharedFileNames)
                {
                    DataRow table1Row = table1.Rows.Find(fileName + table1DateSuffix);
                    DataRow table2Row = table2.Rows.Find(fileName + table2DateSuffix);

                    if(table1Row == null || table2Row == null)
                        continue;

                    if(table1Row[columnName] == DBNull.Value || table2Row[columnName] == DBNull.Value)
                        continue;

                    float table1Value = (float)(double)table1Row[columnName];
                    float table2Value = (float)(double)table2Row[columnName];
                    if(table1Value != table2Value)
                    {
                        if(firstDifferingFileName == "")
                            firstDifferingFileName = fileName;

                        numberDiffering++;
                        summedDifference += Math.Abs(table1Value - table2Value);
                    }
                }

                DataRow rowForColumnName = differingValues.NewRow();
                rowForColumnName[table2FirstDifferingFileNameColumnName] = firstDifferingFileName + table2DateSuffix;
                rowForColumnName[table1FirstDifferingFileNameColumnName] = firstDifferingFileName + table1DateSuffix;
                rowForColumnName[numberOfDifferingColumnEntriesColumnName] = numberDiffering;
                rowForColumnName[averageDifferenceColumName] = summedDifference / numberDiffering;
                rowForColumnName[comparingColumnNameColumnName] = columnName;

                differingValues.Rows.Add(rowForColumnName);
            }

            return differingValues;
        }

        private int getFileSuffixLength(in string fileName)
        {
            char searchChar = '.';
            int occurrencePosition = fileName.Count(f => (f == searchChar)) - 1; 
            var result = fileName.Select((c, i) => new { Char = c, Index = i })
                            .Where(item => item.Char == searchChar)
                            .Skip(occurrencePosition - 1)
                            .FirstOrDefault();
            return fileName.Count() - result.Index;
        }

        public DataTable ViewDifferingFileNames()
        {
            DataTable differingFileNames = new DataTable();

            differingFileNames.Columns.Add(FileNameColumnName);
            differingFileNames.Columns.Add(OwnerTableColumnName);

            if(Table1AxesValues == null)
                Table1AxesValues = getTableAxesValues(Table1Name);

            if(Table2AxesValues == null)
                Table2AxesValues = getTableAxesValues(Table2Name);

            var differingTableAxesValues = new Dictionary<string, List<float>>();
            foreach(var table1AxisPair in Table1AxesValues)
            {
                differingTableAxesValues.Add(table1AxisPair.Key, new List<float>());
                var table1Set = new HashSet<float>(table1AxisPair.Value);
                var table2Set = new HashSet<float>(Table2AxesValues[table1AxisPair.Key]);

                var uniqueValues = table1Set;
                uniqueValues.SymmetricExceptWith(table2Set);
                foreach(var uniqueValue in uniqueValues)
                {
                    differingTableAxesValues[table1AxisPair.Key].Add(uniqueValue);
                }
            }

            // Ignore table axis values not present in both tables
            string removeDifferentAxisValuesString = "";
            foreach(var differingValuesPair in differingTableAxesValues)
            {
                if(differingValuesPair.Value.Count() != 0)
                {
                    foreach(float differingValue in differingValuesPair.Value)
                    {
                        removeDifferentAxisValuesString += $"NOT {differingValuesPair.Key} = {differingValue} AND ";
                    }
                }
            }
            if(removeDifferentAxisValuesString.Count() > 0)
            {
                removeDifferentAxisValuesString = "WHERE " + removeDifferentAxisValuesString.Substring(0, removeDifferentAxisValuesString.Count() - 5);
            }else
            {
                removeDifferentAxisValuesString = "";
            }

            string[] tableNames = new string[] {Table1Name, Table2Name};
            List<string>[] fileNameColumns = new List<string>[2];

            for(int i = 0; i < tableNames.Count(); i++)
            {
                string selectFileNamesColumnCmdString = $"SELECT {FileNameColumnName} FROM {_databaseName}.`{tableNames[i]}` {removeDifferentAxisValuesString}";
                using(var connection = new MySqlConnection(_connectionString))
                {
                    connection.Open();
                    using var selectFileNamesColumnCmd = new MySqlCommand(selectFileNamesColumnCmdString, connection);
                    DataTable fileNamesDataTable = new DataTable();
                    selectFileNamesColumnCmd.CommandTimeout = 200;
                    fileNamesDataTable.Load(selectFileNamesColumnCmd.ExecuteReader());
                    fileNameColumns[i] = GetColumnFromTable<string>(fileNamesDataTable, "FileName").ToList();
                }
            }

            var table1FileNamesWDate = fileNameColumns[0];
            var table2FileNamesWDate = fileNameColumns[1];

            string[] table1FileNames = new string[table1FileNamesWDate.Count()];
            string[] table2FileNames = new string[table2FileNamesWDate.Count()];

            int table1DateSuffixLength = getFileSuffixLength(table1FileNamesWDate[0]);
            int table2DateSuffixLength = getFileSuffixLength(table1FileNamesWDate[1]);

            string table1DateSuffix = table1FileNamesWDate[0].Substring(table1FileNamesWDate[0].Count() - table1DateSuffixLength, table1DateSuffixLength);
            string table2DateSuffix = table2FileNamesWDate[0].Substring(table2FileNamesWDate[0].Count() - table2DateSuffixLength, table2DateSuffixLength);

            for(int i = 0; i<table1FileNamesWDate.Count(); i++){
                table1FileNames[i] = table1FileNamesWDate[i].Substring(0, table1FileNamesWDate[i].Count() - table1DateSuffixLength);
            }

            for(int i = 0; i<table2FileNamesWDate.Count(); i++){
                table2FileNames[i] = table2FileNamesWDate[i].Substring(0, table2FileNamesWDate[i].Count() - table2DateSuffixLength);
            }

            var table1Values = new HashSet<string>(table1FileNames);
            var table2Values = new HashSet<string>(table2FileNames);

            var uniqueFileNames = new HashSet<string>(table1Values);
            uniqueFileNames.SymmetricExceptWith(table2Values);

            foreach(string uniqueFileName in uniqueFileNames)
            {   
                DataRow uniqueFileNameRow = differingFileNames.NewRow();
                if(table1Values.Contains(uniqueFileName))
                {
                    uniqueFileNameRow[OwnerTableColumnName] = Table1Name;
                    uniqueFileNameRow[FileNameColumnName] = uniqueFileName + table1DateSuffix;
                }else{
                    uniqueFileNameRow[OwnerTableColumnName] = Table2Name;
                    uniqueFileNameRow[FileNameColumnName] = uniqueFileName + table2DateSuffix;
                }

                differingFileNames.Rows.Add(uniqueFileNameRow);
            }

            return differingFileNames;
        }

        public readonly string Table1Name;
        public readonly string Table2Name;
        public Dictionary<string, float[]> Table1AxesValues;
        public Dictionary<string, float[]> Table2AxesValues;

        public const string FileNameColumnName = "FileName";
        public const string OwnerTableColumnName = "OwnerTable";
    }
}