using MySql.Data.MySqlClient;
using System;
using System.IO;
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
        }

        public TableVerifier(string table_name){
            TableName = table_name;
            _connection = new MySqlConnection(_connectionString);
            _connection.Open();

            populateTableAxesValues();
            Console.WriteLine("done!");
        }
        public void CheckHoles()
        {

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
            Console.WriteLine(content.table_axes);

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
        
        private const string _connectionString = @"server=gfyvrmysql01.gf.local;userid=RSB;password=;database=GradShafranov";
        private const string _metadataTableName = "lut_metadata";
        private MySql.Data.MySqlClient.MySqlConnection _connection;
    }
}

// using var con = new MySqlConnection(cs);
// con.Open();

// using var cmd = new MySqlCommand("SHOW TABLES", con);

// using MySqlDataReader rdr = cmd.ExecuteReader();

// while (rdr.Read())
// {
//     Console.WriteLine(rdr.GetValue(0));
// }
