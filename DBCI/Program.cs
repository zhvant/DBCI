using System;

namespace DBCI
{
    class Program
    {
        static void Main(string[] args)
        {
            string HelpMessage =
$@" {System.AppDomain.CurrentDomain.FriendlyName} [-h] [-s SERVER] [-db DATABASE] [-o OBJECTS] [-f FILE]
  [-p PATH] [-u USER] [-pwd PASSWORD]
Service for scripting Database Objects for Continuous Integration.

    Argument List:
      -h   --help      Help
      -s   --server    Database Server
      -db  --database  Database
      -o   --objects   List of objects separated by commas
      -f   --file      File with list of objects or .sql script
      -p   --path      Path for unloading
      -u   --user      User for SQL Authorization
      -pwd --password  Password for SQL Authorization

    List of available objects for unloading:
    - Tables
    - Views
    - Functions
    - Stored Procedures
    - Jobs
";


            DataObject dbo = new DataObject(); //Database objects class initialization
            File InputFile = new File();       //Input file class initialization
            File OutputFile = new File();      //Upload Directory Class Initialization

            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower())
                {
                    case "-s":
                    case "--server":   dbo.Server = args[i + 1]; break;

                    case "-db": 
                    case "--database": dbo.Database = args[i + 1]; break;

                    case "-o":  
                    case "--objects": dbo.GetObjectsFromContent(args[i + 1]); break;

                    case "-f":  
                    case "--file":   InputFile.FilePath = args[i + 1]; break;

                    case "-p":   
                    case "--path":   OutputFile.FilePath = args[i + 1]; break;

                    case "-u":  
                    case "--user":   dbo.User = args[i + 1]; break;

                    case "-pwd":
                    case "--password": dbo.Password = args[i + 1]; break;

                    case "-h":   
                    case "--help": Console.WriteLine(HelpMessage); Environment.Exit(0); break;
                }
            }

            //If the server is not given as an argument, type in the console
            if (dbo.Server == null) {
                Console.Write("Database Server: ");
                dbo.Server = Console.ReadLine();
            }

            //If the database is not entered as an argument, enter in the console
            if (dbo.Database == null)
            {
                Console.Write("Database: ");
                dbo.Database = Console.ReadLine();
            }

            //If the argument does not include an input file and a list of objects, type in the console
            if (InputFile.FilePath == null && dbo.ObjectNames==null)
            {
                string Content;
                Console.WriteLine("Enter a list of objects or a path to a file/script: ");
                Content = Console.ReadLine();
                Content = Content.Replace("\"", "");

                if (Content.IndexOf('\\')>0 || Content.IndexOf(".txt") > 0 || Content.IndexOf(".sql") > 0)  //If the line contains a slash, .txt or .sql - Assume that the path to the file is entered
                {
                    InputFile.FilePath = Content;
                }
                else  
                {
                    dbo.GetObjectsFromContent(Content); //Else parse the list of objects
                }
            }
            //If the upload directory is not entered as an argument, read from the console
            if (OutputFile.FilePath == null)
            {
                Console.Write("Upload to folder: ");
                OutputFile.FilePath = Console.ReadLine();
            }

            //If the file path is not empty, read
            if (InputFile.FilePath != null)
            {
                InputFile.Read(InputFile.FilePath);
                //If the file extension is .sql then get the list from the script, otherwise consider it a list of objects
                if (InputFile.FilePath.Substring(InputFile.FilePath.Length - 4) == ".sql")
                    dbo.GetObjectsFromSQL(InputFile.Content);
                else
                    dbo.GetObjectsFromContent(InputFile.Content);
            }



            foreach (string ObjectName in dbo.ObjectNames)
            {
                bool isWrited = false;
                //For all objects except jobs
                dbo.GetObjectType(ObjectName);
                dbo.GetObjectSchema(ObjectName);
                if (dbo.ObjectType == "Table") { dbo.GetTableDefinition(dbo.ObjectSchema, ObjectName); }  //If the object is a table, then use the special method
                else dbo.GetObjectDefinition(ObjectName);
                if (dbo.ObjectSchema == "dbo")
                { OutputFile.FileName = $"{dbo.ObjectSchema}.{ObjectName}.{dbo.ObjectType}.sql"; } else { OutputFile.FileName = $"{ObjectName}.{dbo.ObjectType}.sql"; }
                OutputFile.Content = dbo.ObjectDefinition;
                if (OutputFile.Content != null)
                {
                    OutputFile.Write();
                    isWrited = true;
                }

                //For jobs, If there are no rights to msdb - do not search
                try
                {
                    dbo.GetJobDefinition(ObjectName);  //If a job is found, then use a special method
                    if (dbo.ObjectDefinition != null)
                    {
                        OutputFile.FileName = $"{ObjectName}.Job.sql";
                        OutputFile.Content = dbo.ObjectDefinition;
                        if (OutputFile.Content != null)
                        {
                            OutputFile.Write();
                            isWrited = true;
                        }                        
                    }
                }
                catch {;}
                if (isWrited == false) { Console.WriteLine($"No data for object {ObjectName}"); } //If no object
            }
            if (args.Length == 0)
            {
                Console.WriteLine("Press any key ...");
                Console.ReadKey();
            }
        }
    }
}
