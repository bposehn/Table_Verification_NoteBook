using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Data;
using System.Diagnostics;
using System.Timers;
using YamlDotNet;
using System.Text.RegularExpressions;  

﻿﻿namespace TableVerification
{
    public class YamlConfig{
        public Dictionary<string, Object> table_axes {get; set;}
    }

    public class TableVerifier
    {
        public static void Main()
        {
            TableVerifier verifier = new TableVerifier("pi3b_asbuilt_pfc17500ab_2022-06-09");
            verifier.HasHoles();
        }

        public TableVerifier(string table_name){
            TableName = table_name;
            _connection = new MySqlConnection(_connectionString);
            _connection.Open();

            populateTableAxesValues();
            createTableAxesIndex();
        }

        ~TableVerifier(){
            _connection.Close();
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

                // Parallel.ForEach(non_search_table_axes[non_search_keys[0]], val0 =>
                // {
                //     Console.WriteLine("aaa");
                // });

                foreach(var val0 in non_search_table_axes[non_search_keys[0]]){
                foreach(var val1 in non_search_table_axes[non_search_keys[1]]){
                foreach(var val2 in non_search_table_axes[non_search_keys[2]]){
                foreach(var val3 in non_search_table_axes[non_search_keys[3]]){
                foreach(var val4 in non_search_table_axes[non_search_keys[4]]){
                foreach(var val5 in non_search_table_axes[non_search_keys[5]]){
                    // TODO modify the column names as they are different in yaml vs table
                    var get_search_axis_val_cmd_string = "SELECT * FROM gradshafranov.`" + TableName + "` WHERE " +
                     non_search_keys[0] + " = " + val0.ToString() + " AND " +
                     non_search_keys[1] + " = " + val1.ToString() + " AND " +
                     non_search_keys[2] + " = " + val2.ToString() + " AND " +
                     non_search_keys[3] + " = " + val3.ToString() + " AND " +
                     non_search_keys[4] + " = " + val4.ToString() + " AND " +
                     non_search_keys[5] + " = " + val5.ToString();

                    using(var get_search_axis_val_cmd = new MySqlCommand(get_search_axis_val_cmd_string, _connection))
                    {
                        get_search_axis_val_cmd.CommandTimeout = 200;
                        var dt = new DataTable();
                        dt.Load(get_search_axis_val_cmd.ExecuteReader());
                        Console.WriteLine("one query takes {0}: ", sw.ElapsedMilliseconds);
                        sw.Restart();
                    }
                }
                }
                }
                }
                }
                }
            }

            sw.Stop();
            Console.WriteLine("Done in: {0}", sw.Elapsed);
        }

        public bool checkProfileAtColumn(String column_names)
        {
            return true;
        }


        private bool hasAnyNulls()
        {
            string get_num_columns_cmd_string = "SELECT COUNT(*) `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='GradShafranov' AND `TABLE_NAME`='pi3b_asbuilt_pfc17500ab_2022-06-09'";
            int num_columns;

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
            
            string where_clause = "";

            foreach(var col_name in col_names){
                where_clause += col_name + " IS NULL OR ";
            }
            where_clause = where_clause.Substring(0, where_clause.Length - 4);

            string check_any_nulls_cmd_string = "SELECT COUNT(*) FROM gradshafranov.`pi3b_asbuilt_pfc17500ab_2022-06-09` WHERE " + where_clause;
            using (var check_any_nulls_cmd = new MySqlCommand(check_any_nulls_cmd_string, _connection)){
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
            var get_num_rows_cmd = new MySqlCommand(get_num_rows_cmd_string, _connection);
            using(var rdr = get_num_rows_cmd.ExecuteReader())
            {
                rdr.Read();
                var ret = rdr.GetInt32(0);
                return ret;
            }
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

            var deserializer = new YamlDotNet.Serialization.DeserializerBuilder()
                .WithNamingConvention(YamlDotNet.Serialization.NamingConventions.UnderscoredNamingConvention.Instance)
                .Build();

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
                Console.Write(pair.Value.GetType());
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
            using var get_yaml_file_cmd = new MySqlCommand(get_yaml_file_cmd_string, _connection);

            using MySqlDataReader rdr = get_yaml_file_cmd.ExecuteReader();

            rdr.Read();
            string yaml_filename = rdr.GetString("YamlFilename");
            var file_location = rdr.GetString("FilesLocation");
            file_location = file_location.Substring(0, file_location.Length - 6); // TODO do this in a more programtic way

            return Path.Join("/mnt/lut", file_location, yaml_filename);
        } 

        public Dictionary<String, double[]> TableAxesValues = new Dictionary<string, double[]>();
        public string TableName;
        
        private const string _connectionString = @"server=gfyvrmysql01.gf.local; userid=RSB; password=; database=GradShafranov";
        private const string _metadataTableName = "lut_metadata";
        private MySql.Data.MySqlClient.MySqlConnection _connection;
    }
}