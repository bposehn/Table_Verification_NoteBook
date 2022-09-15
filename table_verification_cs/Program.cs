using System;
using System.IO;
using System.Data;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using MySql.Data.MySqlClient;
using System.Threading;
using System.IO;
using System.Timers;
using YamlDotNet;
using Plotly.NET;
using MathNet.Numerics;
using MathNet.Numerics.Statistics;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;

﻿﻿namespace TableVerification
{
    public struct FailingProfile
    {
        public string ProfileColumnName;
        public string SearchAxis;
        public double OffendingValue;
        public Dictionary<string, double> TableParams;
    }

    public class YamlConfig{
        public Dictionary<string, Object> table_axes {get; set;}
    }

    public class TableVerifier
    {
        public static void Main()
        {
            TableVerifier verifier = new TableVerifier("pi3b_asbuilt_pfc17500ab_2022-06-09", "B161087,B211100,B261087,B291060,B215008,B261008");
            verifier.GenerateProfileScores();
            // Console.WriteLine(profile_check_result);
        }

        public TableVerifier(string tableName, string columnsOfInterest){
            TableName = tableName;
            ColumnsOfInterest = columnsOfInterest;

            populateTableAxesValues();
            loadTable();
        }

        public List<FailingProfile> TestFailingProfiles(){
            FailingProfile f1 = new FailingProfile();
            f1.ProfileColumnName = "TestName";
            f1.SearchAxis = "TestAxis";

            var tp = new Dictionary<string, double>();
            tp.Add("a", 1.11);
            f1.TableParams = tp;

            f1.OffendingValue = 1;

            var ret_list = new List<FailingProfile>();
            ret_list.Add(f1);
            ret_list.Add(f1);

            return ret_list;
        }

        public int GetNumRowsInDatatable()
        {
            return _dataTable.Rows.Count;
        }

        private void loadTable(){
            string tablename = "xmltable.xml";
            string schemaname = "schema.xsd";
            if(File.Exists(tablename) && File.Exists(schemaname)){
                _dataTable.ReadXmlSchema(schemaname);
                _dataTable.ReadXml(tablename);
                return;
            }

            var tableKeys = TableAxesValues.Keys.ToArray();

            var queryColumns = "";
            foreach(var tableKey in tableKeys){
                queryColumns += tableKey + ", ";
            }
            foreach(var columnOfInterest in ColumnsOfInterest.Split(',')){
                queryColumns += columnOfInterest + ", ";
            }
            queryColumns = queryColumns.Substring(0, queryColumns.Length-2);

            var get_table_cmd_string = $"SELECT {queryColumns} FROM {_databaseName}.`{TableName}`";
            using(var connection = new MySqlConnection(_connectionString))
            {
                connection.Open();
                using var get_table_cmd = new MySqlCommand(get_table_cmd_string, connection);
                get_table_cmd.CommandTimeout = 200;
                _dataTable.Load(get_table_cmd.ExecuteReader()); 
                _dataTable.WriteXmlSchema(schemaname);
                _dataTable.WriteXml(tablename);
                connection.Close();
            }
        }

        public void HasHoles() //need to determine what the proper return type will be here
        {
            // working fine just dont want to wait for
            // if(! hasAnyNulls()){
            //     return; // table has no null values, no need to check for holes
            // }

            var tableVals = TableAxesValues.Values.ToArray();
            var tableKeys = TableAxesValues.Keys.ToArray();

            int n = 1;
            foreach(var vals in tableVals){
                n *= vals.Count();
            }
            Console.WriteLine("{0} Table Axis Combinations", n);

            var sw = new Stopwatch();
            sw.Start();

            //currently would take around 48 hours just to get data
            for(int i = 0; i < TableAxesValues.Count(); i++)
            {
                Dictionary<string, double[]> non_search_table_axes = new Dictionary<string, double[]>();
                foreach(var pair in TableAxesValues){
                    if(pair.Key == tableKeys[i]) continue;
                    non_search_table_axes.Add(pair.Key, pair.Value);
                }

                var non_search_keys = non_search_table_axes.Keys.ToArray();
                if(non_search_keys.Count() != 6){
                    // error
                }

                var search_axis = tableVals[i];

                foreach(var val0 in non_search_table_axes[non_search_keys[0]]){
                foreach(var val1 in non_search_table_axes[non_search_keys[1]]){
                foreach(var val2 in non_search_table_axes[non_search_keys[2]]){
                foreach(var val3 in non_search_table_axes[non_search_keys[3]]){
                foreach(var val4 in non_search_table_axes[non_search_keys[4]]){
                foreach(var val5 in non_search_table_axes[non_search_keys[5]]){
                    var get_search_axis_val_cmd_string = "SELECT * FROM gradshafranov.`" + TableName + "` WHERE " +
                     non_search_keys[0] + " = " + val0.ToString() + " AND " +
                     non_search_keys[1] + " = " + val1.ToString() + " AND " +
                     non_search_keys[2] + " = " + val2.ToString() + " AND " +
                     non_search_keys[3] + " = " + val3.ToString() + " AND " +
                     non_search_keys[4] + " = " + val4.ToString() + " AND " +
                     non_search_keys[5] + " = " + val5.ToString();

                    using var _connection = new MySqlConnection(_connectionString);
                    _connection.Open();
                    using(var get_search_axis_val_cmd = new MySqlCommand(get_search_axis_val_cmd_string, _connection))
                    {
                        get_search_axis_val_cmd.CommandTimeout = 200;
                        using var dt = new DataTable();
                        dt.Load(get_search_axis_val_cmd.ExecuteReader());

                        Console.WriteLine("one query takes {0}: ", sw.ElapsedMilliseconds);
                        sw.Restart();

                        // var rows = dt.AsEnumerable().ToArray();
                        // var col_names = getColumnNames();

                        // // check for holes along columns
                        // foreach(var row in rows){
                        //     for(int j = 1; i<col_names.Count()-1; i++){
                        //         if(row[col_names[j-1]] == null && row[col_names[j]] == null //this will likely have to be a different comparison
                        //                                        && row[col_names[j+1]] == null){
                        //             // do what?
                        //         }
                        //     }
                        // }

                        // var dt_array = dt.AsEnumerable().ToArray();
                        // var num_rows = dt.Rows.Count;

                        // foreach(var col_name in col_names){
                        //     for(int j = 1; j < num_rows - 1; j++){
                        //         if(dt_array[j-1][col_name] == null && dt_array[j][col_name] == null
                        //                                            && dt_array[j+1][col_name] == null){
                        //             //do what? 
                        //         }
                        //     }
                        // }

                        //check for holes along rows

                    }
                } } } } } }
            }

            sw.Stop();
            Console.WriteLine("Done in: {0}", sw.Elapsed);
        }

        /*
        @brief: Returns csv string that denotes the params which did not pass the threshold values
        @detail: A profile would fail if greater than e
        @detail: String returned where each line is a failed profile with: col name, search param name, (param values,)
                    where param values are in order (psieq_soak, beta_pol1_setpoint, psieq_dc, NevinsA(B), NevinsC, NevinsN, CurrentRatio )
        */
        public void GenerateProfileScores()
        {
            var tableKeys = TableAxesValues.Keys.ToArray();

            var total_sw = new Stopwatch();
            total_sw.Start();

            _failingProfiles = new ConcurrentBag<FailingProfile>();

            _profileScores = _dataTable.Clone();
            foreach(var col in ColumnsOfInterest.Split(',')){
                _profileScores.Columns.Add(col + OffendingValueSuffix, typeof(float));
            }
            
            List<Task> tasks = new List<Task>();
            for(int i = 0; i < tableKeys.Count(); i++)
            {
                var search_axis_name = tableKeys[i];
                tasks.Add(Task.Factory.StartNew(checkProfilesHelper, search_axis_name));
            }
            
            Task.WaitAll(tasks.ToArray());

            // foreach(var search_axis in tableKeys){
            //     checkProfilesHelper(search_axis);
            // }

            Console.WriteLine("All profile check duration: {0}", total_sw.ElapsedMilliseconds);
            total_sw.Stop();
        }

        public List<FailingProfile> GetFailingProfiles(float threshold){
            var failingProfiles = new List<FailingProfile>();

            return failingProfiles;
        }

        private void checkProfilesHelper(Object search_axis_name_obj){
            var search_axis_name = search_axis_name_obj as string;

            var tableVals = TableAxesValues.Values.ToArray();
            var tableKeys = TableAxesValues.Keys.ToArray();
            string[] column_arr = ColumnsOfInterest.Split(',');
            var dt_copy = _dataTable.Copy();

            Dictionary<string, double[]> non_search_table_axes = new Dictionary<string, double[]>();
            var search_axis_vals = TableAxesValues[search_axis_name];

            int subarr_length = 2;
            foreach(var pair in TableAxesValues){
                if(pair.Key == search_axis_name) continue;
                double[] partial_vals = new double[subarr_length];
                Array.Copy(pair.Value, partial_vals, subarr_length);
                non_search_table_axes.Add(pair.Key, partial_vals);
            }

            string[] non_search_keys = non_search_table_axes.Keys.ToArray();
            if(non_search_keys.Count() != 6){
                // error
            }

            int its = 0;
            foreach(var val0 in non_search_table_axes[non_search_keys[0]]){
                // var arr0 = _dataTable.Select($"{non_search_keys[0]} = {val0}");
                var arr0 = dt_copy.Select($"{non_search_keys[0]} = {val0}");
                if(arr0.Length == 0){
                    continue;
                }
                
                var sort0 = _dataTable.Clone();
                foreach(var row in arr0)
                    sort0.ImportRow(row);
            foreach(var val1 in non_search_table_axes[non_search_keys[1]]){
                var arr1 = sort0.Select($"{non_search_keys[1]} = {val1}");
                if(arr1.Length == 0){
                    continue;
                }
                
                var sort1 = _dataTable.Clone();
                foreach(var row in arr1)
                    sort1.ImportRow(row);
            foreach(var val2 in non_search_table_axes[non_search_keys[2]]){
                var arr2 = sort1.Select($"{non_search_keys[2]} = {val2}");
                if(arr2.Length == 0){
                    continue;
                }
                
                var sort2 = _dataTable.Clone();
                foreach(var row in arr2)
                    sort2.ImportRow(row);
            foreach(var val3 in non_search_table_axes[non_search_keys[3]]){
                var arr3 = sort2.Select($"{non_search_keys[3]} = {val3}");
                if(arr3.Length == 0){
                    continue;
                }
                
                var sort3 = _dataTable.Clone();
                foreach(var row in arr3)
                    sort3.ImportRow(row);
            foreach(var val4 in non_search_table_axes[non_search_keys[4]]){
                var arr4 = sort3.Select($"{non_search_keys[4]} = {val4}");
                if(arr4.Length == 0){
                    continue;
                }
                
                var sort4 = _dataTable.Clone();
                foreach(var row in arr4)
                    sort4.ImportRow(row);
            foreach(var val5 in non_search_table_axes[non_search_keys[5]]){

                var arr5 = sort4.Select($"{non_search_keys[5]} = {val5}");
                if(arr5.Length == 0){
                    continue;
                }

                var dt = _dataTable.Clone();
                foreach(var row in arr5)
                    dt.ImportRow(row);

                string subtitle = "";

                Dictionary<string, double> non_search_values = new Dictionary<string, double>(){
                    {non_search_keys[0], val0},
                    {non_search_keys[1], val1},
                    {non_search_keys[2], val2},
                    {non_search_keys[3], val3},
                    {non_search_keys[4], val4},
                    {non_search_keys[5], val5}
                };

                string table_param_values_str = "";
                int i = 0;
                foreach(var pair in non_search_values){
                    table_param_values_str += $"{pair.Key} = {pair.Value} ";
                    i++;
                    if(i%2 == 0){
                        table_param_values_str += "<br>";
                    }
                }

                if(dt.Rows.Count < 5)
                    continue;

                Dictionary<string, double[]> column_vals = new Dictionary<string, double[]>();
                foreach(string col_name in column_arr){
                    column_vals.Add(col_name, new double[dt.Rows.Count]);
                }

                int row_num = 0;
                foreach(DataRow row in dt.Rows){
                    foreach(var col_name in column_arr){
                        column_vals[col_name][row_num] = Convert.ToDouble(row[col_name]);
                    }
                    row_num ++;
                }

                var profileScoreRow = _profileScores.NewRow();
                profileScoreRow[search_axis_name] = DBNull.Value;
                profileScoreRow[non_search_keys[0]] = val0;  
                profileScoreRow[non_search_keys[1]] = val1;  
                profileScoreRow[non_search_keys[2]] = val2;  
                profileScoreRow[non_search_keys[3]] = val3;  
                profileScoreRow[non_search_keys[4]] = val4;  
                profileScoreRow[non_search_keys[5]] = val5;

                foreach(var col_name in column_arr){
                    var vals = fitWithPointRemovedChecker(column_vals[col_name], search_axis_vals, .35, ref subtitle);
                    var metric_score = vals.Item1;
                    var offending_value = vals.Item2;

                    profileScoreRow[col_name] = metric_score;
                    profileScoreRow[col_name + OffendingValueSuffix] = offending_value;
                }

  
                _mtx.WaitOne(); //Critical     
                {
                    _profileScores.Rows.Add(profileScoreRow);
                }
                _mtx.ReleaseMutex();

                its ++;
            }}}}}}
        }

        private bool concavityChecker(double[] col, ref string subtitle)
        {
            double[] second_derivs = new double[col.Count() - 2];
            for(int j = 1; j < col.Count() - 1; j++)
            {
                second_derivs[j-1] = col[j-1] - 2*col[j] + col[j+1];
            }

            double profile_range = col.Max() - col.Min();
            double second_deriv_diff_thresh = .5 * profile_range; //this should maybe be scaled to each profile? or maybe the average range of each profile for a single column

            int num_sign_changes = 0;
            double prev_concavity = second_derivs[0];
            List<double> diffs = new List<double>();
            foreach(double second_deriv in second_derivs){
                if((second_deriv > 0) != (prev_concavity > 0)){
                    var diff = Math.Abs(second_deriv - prev_concavity);
                    diffs.Add(diff);
                    if(diff > second_deriv_diff_thresh){
                        num_sign_changes ++;
                    }
                }
                prev_concavity = second_deriv;
            }

            if(num_sign_changes > 1){
                var max_diff_dist_to_avg = diffs.Max() - diffs.Average();
                subtitle = $"Number of significant sign changes: {num_sign_changes}<br> Max difference between changed signs: {diffs.Max()}<br> Diff from max to avg difference: {max_diff_dist_to_avg}";
            
                return false;
            }

            return true;
        }

        // return (metric, offending value)
        private (float, float) fitWithPointRemovedChecker(in double[] col, in double[] search_axis, in double thresh, ref string subtitle)
        {
            if((col.Max() - col.Min()) < (1e-2 * col.Max())) // if range is less than 1.5% of range then don't bother checking
            {
                return (0, 0);
            }

            var xs = new double[col.Count()-1];
            for(int i = 0; i < col.Count()-1; i++)
                xs[i] = Convert.ToDouble(i);

            double[] point_removed_quadratic_fit_goodness = new double[col.Count()];

            for(int j = 0; j < col.Count(); j++){
                var data_with_removed_value = col.Where((source, index) =>index != j).ToArray(); // could also try having that point as averge of two on either end
                
                double[] quadratic_fit_params = Fit.Polynomial(xs, data_with_removed_value, 2);
                var quadratic_fit_goodness = GoodnessOfFit.RSquared(xs.Select(
                    x => quadratic_fit_params[0] + quadratic_fit_params[1]*x + quadratic_fit_params[2]*Math.Pow(x, 2)), data_with_removed_value);
                point_removed_quadratic_fit_goodness[j] = quadratic_fit_goodness;
            }

            (double mean_fit_goodness, double std_dev) = point_removed_quadratic_fit_goodness.MeanStandardDeviation();
            double dist_from_best_fit_to_mean = point_removed_quadratic_fit_goodness.Max() - mean_fit_goodness;

            var offending_val = search_axis[Array.IndexOf(point_removed_quadratic_fit_goodness, point_removed_quadratic_fit_goodness.Max())];

            return ((float)dist_from_best_fit_to_mean, (float)offending_val);
        }

        private bool quadraticFitChecker(double[] col, ref string subtitle)
        {
            var xs = new double[col.Count()];
            for(int i = 0; i < col.Count(); i++)
                xs[i] = Convert.ToDouble(i);

            double[] quadratic_fit_params = Fit.Polynomial(xs, col, 2);
            var quadratic_fit_goodness = GoodnessOfFit.RSquared(xs.Select(
                x => quadratic_fit_params[0] + quadratic_fit_params[1]*x + quadratic_fit_params[2]*Math.Pow(x, 2)), col);

            double fit_threshold = .6;
            if(quadratic_fit_goodness < fit_threshold){
                subtitle = $"Fit Goodness: {quadratic_fit_goodness}"; 
                return false;
            }

            return true;
        }

        private bool hasAnyNulls()
        {
            string where_clause = "";
            var col_names = getColumnNames();

            foreach(var col_name in col_names){
                where_clause += col_name + " IS NULL OR ";
            }
            where_clause = where_clause.Substring(0, where_clause.Length - 4);

            using var connection = new MySqlConnection(_connectionString);
            string check_any_nulls_cmd_string = "SELECT COUNT(*) FROM gradshafranov.`pi3b_asbuilt_pfc17500ab_2022-06-09` WHERE " + where_clause;
            using (var check_any_nulls_cmd = new MySqlCommand(check_any_nulls_cmd_string, connection)){
                check_any_nulls_cmd.CommandTimeout = 200;

                using var rdr = check_any_nulls_cmd.ExecuteReader();
                rdr.Read();

                var num_nulls = rdr.GetInt32(0);

                if(num_nulls != 0){
                    return true;
                }

                return false;
            }
        }

        private int getNumRows()
        {
            string get_num_rows_cmd_string = "SELECT COUNT(*) FROM gradshafranov.`" + TableName + "`";

            using var _connection = new MySqlConnection(_connectionString);
            _connection.Open();
            using (var get_num_rows_cmd = new MySqlCommand(get_num_rows_cmd_string, _connection))
            {
                using var rdr = get_num_rows_cmd.ExecuteReader();
                rdr.Read();
                var ret = rdr.GetInt32(0);
                return ret;
            }
        }

        private string[] getColumnNames(){

            string get_num_columns_cmd_string = "SELECT COUNT(*) `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='GradShafranov' AND `TABLE_NAME`='pi3b_asbuilt_pfc17500ab_2022-06-09'";
            int num_columns;

            using var _connection = new MySqlConnection(_connectionString);
            _connection.Open();
            using(var get_num_columns_cmd = new MySqlCommand(get_num_columns_cmd_string, _connection))
            {
                using var rdr = get_num_columns_cmd.ExecuteReader();
                rdr.Read();
                num_columns = rdr.GetInt32(0);
            }

            string[] col_names = new string[num_columns];

            string get_columns_cmd_string = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='GradShafranov' AND `TABLE_NAME`='pi3b_asbuilt_pfc17500ab_2022-06-09'";
            using(var get_columns_cmd = new MySqlCommand(get_columns_cmd_string, _connection))
            {
                int i = 0;
                using var rdr = get_columns_cmd.ExecuteReader();
                while(rdr.Read()){
                    col_names[i] = rdr.GetString(0);
                    i++;
                }
            }

            return col_names;
        }

        private void createTableAxesIndex()
        {
            if(TableAxesValues.Count() == 0){
                // error
            }
            var table_axes_names = TableAxesValues.Keys.ToArray();
            string table_axes_name_string = "";
            foreach(var name in table_axes_names)
            {
                table_axes_name_string += name + ", ";
            }
            table_axes_name_string = table_axes_name_string.Substring(0, table_axes_name_string.Length - 2);

            string create_table_axes_index_cmd_string =
             "CREATE INDEX table_axes_index ON gradshafranov.`" + TableName + "`(" + table_axes_name_string + ")";

            using var _connection = new MySqlConnection(_connectionString);
            _connection.Open();
            using(var create_table_axes_index_cmd = new MySqlCommand(create_table_axes_index_cmd_string, _connection))
            {
                try{
                   create_table_axes_index_cmd.ExecuteReader();
                }catch(MySql.Data.MySqlClient.MySqlException  e){
                    // Do nothing if index already exists
                    if(! e.Message.StartsWith("Duplicate key name")){
                        throw e;
                    }
                }
            }
        }

        private void populateTableAxesValues()
        {
            string yaml_file_name = getYamlFilename();

            var deserializer = new YamlDotNet.Serialization.DeserializerBuilder().WithNamingConvention(YamlDotNet.Serialization.NamingConventions.UnderscoredNamingConvention.Instance).Build();

            string yaml_text = "";

            if(File.Exists(yaml_file_name)){
                yaml_text = File.ReadAllText(yaml_file_name);
            }else{
                return;
                // TODO add error handling
            }

            // Could just take the next 7 lines
            yaml_text = yaml_text.Substring(yaml_text.IndexOf("table_axes:"), yaml_text.IndexOf("num_equilibria:") - yaml_text.IndexOf("table_axes:"));

            var content = deserializer.Deserialize<YamlConfig>(yaml_text);

            foreach(var pair in content.table_axes){
                if(pair.Value.GetType() == typeof(String)){
                    // this is NevinsB: NevinsA which we can just skip
                    continue;
                }
                else if(pair.Value.GetType() == typeof(List<Object>)){
                    var obj_list = (List<Object>)pair.Value;
                    double[] vals = new double[obj_list.Count];
                    int i = 0;
                    foreach(var val in obj_list){
                        if(val.GetType() == typeof(String)){
                            vals[i] = Convert.ToDouble(val);
                            i++;
                        }
                    }
                    var key = pair.Key;
                    if(pair.Key == "CurrentRatio"){
                        for(int j = 0; j<vals.Count(); j++){
                            vals[j] *= 1e6;
                        }
                        key = "Ipl_setpoint";
                    }
                    TableAxesValues.Add(key, vals);
                }
                else{
                    // TODO add error handling
                }
            }
        }

        private string getYamlFilename()
        {
            string get_yaml_file_cmd_string =
             "SELECT YamlFilename, FilesLocation FROM gradshafranov." + _metadataTableName + " WHERE TableName = '" + TableName + "'";
            
            using var _connection = new MySqlConnection(_connectionString);
            _connection.Open();
            using var get_yaml_file_cmd = new MySqlCommand(get_yaml_file_cmd_string, _connection);

            using MySqlDataReader rdr = get_yaml_file_cmd.ExecuteReader();

            rdr.Read();
            string yaml_filename = rdr.GetString("YamlFilename");
            var file_location = rdr.GetString("FilesLocation");
            file_location = file_location.Substring(0, file_location.Length - 6); // TODO do this in a more programtic way

            return Path.Combine("/mnt/lut", file_location, yaml_filename);
        }

        public Dictionary<String, double[]> TableAxesValues = new Dictionary<string, double[]>();
        public string TableName;
        public string ColumnsOfInterest;
        public string OffendingValueSuffix = "_offending_val";
        public List<string> ProfileMetrics = new List<string>();

        private const string _connectionString = @"server=gfyvrmysql01.gf.local; userid=RSB; password=; database=GradShafranov";
        // private const string _connectionString = @"server=172.25.224.39; userid=lut; password=; database=GradShafranov; Connection Timeout=100";
        private const string _databaseName = "gradshafranov";
        private const string _metadataTableName = "lut_metadata";
        private string[] _tableAxesNames = new string[]{"psieq_soak", "beta_pol1_setpoint", "psieq_dc", "NevinsA", "NevinsC", "NevinsN", "Ipl_setpoint"};

        private ConcurrentBag<FailingProfile> _failingProfiles;
        private DataTable _dataTable = new DataTable();
        private DataTable _profileScores;
        private Mutex _mtx = new Mutex();
    }
}