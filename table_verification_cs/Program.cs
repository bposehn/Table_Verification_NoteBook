using MySql.Data.MySqlClient;
using System;
using System.IO;
using System.Data;
using System.Diagnostics;
using System.Timers;
using YamlDotNet;
using System.Text.RegularExpressions;  

﻿﻿namespace TableVerifier
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
        }

        ~TableVerifier(){
            _connection.Close();
        }

        public void HasHoles() //need to determine what the proper return type will be here
        {
            // working fine just dont want to wait for
            if(! hasAnyNulls()){
                return; // table has no null values, no need to check for holes
            }

            // int num_rows = getNumRows();

            // const int batch_size = 100;
            // int current_batch_start = 0;

            // var sw = new Stopwatch();
            // sw.Start();

            // string get_table_batch_cmd_string = "";       
            // while(current_batch_start < num_rows)
            // {
            //     if(current_batch_start + batch_size - 1 > num_rows){
            //         get_table_batch_cmd_string = "SELECT * FROM gradshafranov.`" + TableName + "` ORDER BY FileName LIMIT " + (num_rows - current_batch_start).ToString() + " OFFSET " + (current_batch_start - 1).ToString(); 
            //     }else if(current_batch_start == 0){
            //         get_table_batch_cmd_string = "SELECT * FROM gradshafranov.`" + TableName + "` ORDER BY FileName LIMIT " + batch_size.ToString();
            //     }else{
            //         get_table_batch_cmd_string = "SELECT * FROM gradshafranov.`" + TableName + "` ORDER BY FileName LIMIT " + batch_size.ToString() + " OFFSET " + (current_batch_start - 1).ToString(); 
            //     }

            //     var sw_ = new Stopwatch();
            //     sw_.Start();
            //     using(var get_table_batch_cmd = new MySqlCommand(get_table_batch_cmd_string, _connection))
            //     {
            //         var dt = new DataTable();
            //         dt.Load(get_table_batch_cmd.ExecuteReader());

            //         sw_.Stop();
            //         Console.WriteLine("single batch time: {0}", sw_.ElapsedMilliseconds);
            //         // foreach(var row in dt.Rows){
            //         //     Console.WriteLine(row);
            //         // }
            //     }

            //     current_batch_start += batch_size;
            // }
            // sw.Stop();

            // Console.WriteLine("getting all data in chunks took: {0}", sw.ElapsedMilliseconds);

            // return;
        }

        public bool checkProfileAtColumn(String column_name){


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
                // rdr.Close();
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
                    var string_value = (String)(pair.Value);
                    if(TableAxesValues.ContainsKey(string_value)){
                        TableAxesValues.Add(pair.Key, TableAxesValues[string_value]);
                    }else{
                        // ? 
                    }
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
                    TableAxesValues.Add(pair.Key, vals);
                }
                else{
                    // TODO add error handling
                }
            }
        }

        private string getYamlFilename()
        {
            string get_yaml_file_cmd_string =
             "SELECT YamlFilename, FilesLocation FROM gradshafranov.lut_metadata WHERE TableName = '" + TableName + "'";
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