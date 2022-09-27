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
    public struct FailingProfile
    {
        public string ProfileColumnName;
        public string SearchAxis;
        public string FilesLocation;
        public Dictionary<string, float> TableParams;
    }

    public class YamlConfig
    {
        public Dictionary<string, Object> table_axes {get; set;}
    }

    /*
    Class used for performing table verification tasks. Two main functionalities are: calculating best to average point removed fit quality (BTAPRFQ), 
        and checking for holes (table axis value combinations which do not exist)
    */
    public class TableVerifier
    {
        public static void Main()
        {
            string columns_of_interest = "q020,q050,q080,WBPolNoDCInFC,phiPlInFC,B161087,B211100,B291060,B215008,B261008,B291008,B261087, B261087_d05,B261087_d10,B261087_d15,B261087_d20,B261087_d25,B261087_d30,B261087_d35,B261087_d40,B261087_d45,B261087_d50";
            TableVerifier verifier = new TableVerifier("pi3b_asbuilt_pfc17500ab_2022-06-09_b", columns_of_interest);

            var sw = new Stopwatch();
            sw.Start();

            // Check profiles
            verifier.GenerateProfileScores();
            Console.WriteLine($"Took {sw.ElapsedMilliseconds} to generate profile scores");
            sw.Stop();
            var failing_profiles = verifier.GetFlaggingProfiles(0.5);

            // // Check holes
            // DataTable failing = verifier.CheckForHoles();
            // sw.Stop();
            // Console.WriteLine($"{sw.ElapsedMilliseconds}");
        }

        /*
        @brief: Populates TableAxesValues from yaml file and loads all table axis and profile columns from the SQL table
        @param: profileNamesOfInterest comma-seperated string containing column names in the SQL table that will be
                    checked for best to average point removed fit quality
        */
        public TableVerifier(string tableName, string profileNamesOfInterest)
        {
            TableName = tableName;
            ProfileNamesOfInterest = profileNamesOfInterest;

            populateTableAxesValues();
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
            // Reduce table to contain only table axes
            string[] selectedColumns = TableAxesValues.Keys.ToArray();
            DataTable justTableAxesTable = new DataView(_dataTable).ToTable(false, selectedColumns);

            DataTable holeAxesValuesTable = _dataTable.Clone();
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
                
                var select5 = justTableAxesTable.Clone();
                foreach(var row in arr5)
                    select5.ImportRow(row);

                // Check if values exist for the the search axis with the other table values
                var dataExistsAtSearchAxisValue = new bool[TableAxesValues[searchAxisName].Count()];
                for(int i = 0; i < TableAxesValues[searchAxisName].Count(); i++)
                {
                    var selectSearchAxisVal = select5.Select($"{searchAxisName} = {TableAxesValues[searchAxisName][i]}");
                    dataExistsAtSearchAxisValue[i] = selectSearchAxisVal.Count() > 0;
                }

                // Find holes in form Exists -> Does Not Exist -> Exists
                for(int i = 1; i < dataExistsAtSearchAxisValue.Count() - 1; i++)
                {
                    if(dataExistsAtSearchAxisValue[i-1] && ! dataExistsAtSearchAxisValue[i] && dataExistsAtSearchAxisValue[i+1])
                    {
                        _mtx.WaitOne();
                        {
                            var holeRow = holeAxesValuesTable.NewRow();
                            holeRow[nonSearchAxisNames[0]] = val0;
                            holeRow[nonSearchAxisNames[1]] = val1;
                            holeRow[nonSearchAxisNames[2]] = val2;
                            holeRow[nonSearchAxisNames[3]] = val3;
                            holeRow[nonSearchAxisNames[4]] = val4;
                            holeRow[nonSearchAxisNames[5]] = val5;
                            holeRow[searchAxisName] = TableAxesValues[searchAxisName][i];

                            holeAxesValuesTable.Rows.Add(holeRow);
                        }
                        _mtx.ReleaseMutex();
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
            DataTable singleNevinsNDataDatable = _dataTable.Clone();

            foreach(var row in singleNevinsNRows)
            {
                singleNevinsNDataDatable.ImportRow(row);
            }
            
            _profileScoresDataTable.BeginLoadData();

            // Calculate BTAPRFQ scores for each search axis
            var tasks = new List<Task>();
            foreach(var searchAxisName in tableKeys)
            {
                if(searchAxisName == "NevinsN")
                    continue;
                
                tasks.Add(Task.Factory.StartNew(() => generateProfileScoresHelper(searchAxisName, singleNevinsNDataDatable)));
            }
            
            Task.WaitAll(tasks.ToArray());

            _profileScoresDataTable.EndLoadData();
            _profileScoresDataTable.AcceptChanges();
        }

        /*
        @brief: Determines which profiles for which table axis combinations have best to average point removed fit
                    quality (BTAPRFQ) scores higher than an inputted threshold
        @returns: List<FailingProfiles> denoting which profiles and table axis combinations are flagged
        @detail: Multiple FailingProfiles may have the same table axes combinations as different profiles could be
                    flagged for the same combination
        */
        public List<FailingProfile> GetFlaggingProfiles(double threshold){
            var failingProfiles = new List<FailingProfile>();

            if(! _profileScoresDataTable.IsInitialized)
            {
                Console.WriteLine("Must generate profile scores before getting failing profiles");
                return failingProfiles;
            }

            var columnNames = ProfileNamesOfInterest.Split(',');
            foreach(var columnName in columnNames)
            {
                var failingRows = _profileScoresDataTable.Select($"{columnName} > {threshold}");
                foreach(var failingRow in failingRows)
                {
                    var failingProfile = new FailingProfile();

                    string SearchAxisName = "";
                    var tableParams = new Dictionary<string, float>();
                    foreach(var axisName in TableAxesValues.Keys){
                        
                        if(failingRow[axisName] == DBNull.Value){
                            SearchAxisName = axisName;
                            tableParams.Add(axisName, (float)failingRow[columnName + OffendingValueSuffix]);
                        }else{
                            tableParams.Add(axisName, (float)(double)failingRow[axisName]);
                        }   
                    }
                    failingProfile.SearchAxis = SearchAxisName;
                    failingProfile.ProfileColumnName = columnName;
                    failingProfile.TableParams = tableParams;
                    failingProfile.FilesLocation = GetFilesLocation();

                    failingProfiles.Add(failingProfile);
                }
            }
            
            return failingProfiles;
        }

        private void generateProfileScoresHelper(in string searchAxisName, in DataTable singleNevinsNDataTable)
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
                var arr0 = singleNevinsNDataTable.Select($"{nonSearchAxisNames[0]} = {val0}");
                if(arr0.Length == 0){
                    continue;
                }
                
                var sort0 = singleNevinsNDataTable.Clone();
                foreach(var row in arr0)
                    sort0.ImportRow(row);
            foreach(var val1 in nonSearchTableAxes[nonSearchAxisNames[1]])
            {
                var arr1 = sort0.Select($"{nonSearchAxisNames[1]} = {val1}");
                if(arr1.Length == 0){
                    continue;
                }
                
                var sort1 = singleNevinsNDataTable.Clone();
                foreach(var row in arr1)
                    sort1.ImportRow(row);
            foreach(var val2 in nonSearchTableAxes[nonSearchAxisNames[2]])
            {
                var arr2 = sort1.Select($"{nonSearchAxisNames[2]} = {val2}");
                if(arr2.Length == 0){
                    continue;
                }
                
                var sort2 = singleNevinsNDataTable.Clone();
                foreach(var row in arr2)
                    sort2.ImportRow(row);
            foreach(var val3 in nonSearchTableAxes[nonSearchAxisNames[3]])
            {
                var arr3 = sort2.Select($"{nonSearchAxisNames[3]} = {val3}");
                if(arr3.Length == 0){
                    continue;
                }
                
                var sort3 = singleNevinsNDataTable.Clone();
                foreach(var row in arr3)
                    sort3.ImportRow(row);
            foreach(var val4 in nonSearchTableAxes[nonSearchAxisNames[4]])
            {
                var arr4 = sort3.Select($"{nonSearchAxisNames[4]} = {val4}");
                if(arr4.Length == 0){
                    continue;
                }
                
                var profilesVersusSearchAxesDatatable = singleNevinsNDataTable.Clone();
                foreach(var row in arr4)
                    profilesVersusSearchAxesDatatable.ImportRow(row);

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
                getTableCommand.CommandTimeout = 500;
                _dataTable.Load(getTableCommand.ExecuteReader()); 
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
        private void populateTableAxesValues()
        {
            string tableAxesYamlFilename = getTableAxesYamlFilename();

            var deserializer = new YamlDotNet.Serialization.DeserializerBuilder().WithNamingConvention(YamlDotNet.Serialization.NamingConventions.UnderscoredNamingConvention.Instance).Build();

            string yamlText = "";

            if(File.Exists(tableAxesYamlFilename)){
                yamlText = File.ReadAllText(tableAxesYamlFilename);
            }else{
                Console.WriteLine("ERROR yaml file to read table axes cannot be found: ", tableAxesYamlFilename);
                return;
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
                            vals[j] *= Convert.ToSingle(1e6);
                        }
                        key = "Ipl_setpoint";
                    }
                    TableAxesValues.Add(key, vals);
                }
                else{
                    Console.WriteLine("ERROR unexpected type encountered reading yaml file");
                }
            }
        }

        private string getTableAxesYamlFilename()
        {
            string getTableAxesYamlFilenameCommandString =
             $"SELECT YamlFilename, FilesLocation FROM {_databaseName}." + _metadataTableName + " WHERE TableName = '" + TableName + "'";
            
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
        public string OffendingValueSuffix = "_offending_val";
        #endregion

        #region Private Members
        private const string _connectionString = @"server=gfyvrmysql01.gf.local; userid=RSB; password=; database=GradShafranov";
        private const string _databaseName = "gradshafranov";
        private const string _metadataTableName = "lut_metadata";
        
        // Contains all data from the table
        private DataTable _dataTable = new DataTable();

        // Has columns for each table axis, for each profile name denoting the best to mean fit difference, another for each
        // profile denoting the table axis value which resulted in the best fit with that point removed
        private DataTable _profileScoresDataTable;    
        private Mutex _mtx = new Mutex();
        private const int _nevinsNIndex = 1;
        private const double _minProfileRange = 2e-2; // Threshold for minimum profile range as fraction of the profiles maximum
        #endregion
    }
}