using System;

namespace GetSpNet
{
    class Program
    { 
        static void Main(string[] args)
        {
            string HelpMessage =
$@" {System.AppDomain.CurrentDomain.FriendlyName} [-h] [-s SERVER] [-db DATABASE] [-o OBJECTS] [-f FILE]
  [-p PATH] [-u USER] [-pwd PASSWORD]
Программа для выгрузки create-скриптов объектов из БД MS SQL Server.

    Список аргументов:
      -h   --help      Справка
      -s   --server    Сервер базы данных
      -db  --database  База Данных
      -o   --objects   Список объектов через запятую
      -f   --file      Файл с объектами или .sql скриптом
      -p   --path      Путь для выгрузки
      -u   --user      Пользователь для Авторизации SQL
      -pwd --password  Пароль для Авторизации SQL

    Список доступных объектов для выгрузки:
    - Tables
    - Views
    - Functions
    - Stored Procedures
    - Jobs
";


            DataObject dbo = new DataObject(); //Инициализация класса объектов БД
            File InputFile = new File();       //Инициализация класса входного файла
            File OutputFile = new File();      //Инициализация класса директории выгрузки

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

            //Если сервер не введен аргументом, ввести в консоль
            if (dbo.Server == null) {
                Console.Write("Сервер: ");
                dbo.Server = Console.ReadLine();
            }

            //Если БД не введена аргументом, ввести в консоль
            if (dbo.Database == null)
            {
                Console.Write("БД: ");
                dbo.Database = Console.ReadLine();
            }

            //Если агрументом не введен входной файл и список объектов, ввести в консоль
            if (InputFile.FilePath == null && dbo.ObjectNames==null)
            {
                string Content;
                Console.WriteLine("Введите список объектов или путь к файлу/скрипту: ");
                Content = Console.ReadLine();
                Content = Content.Replace("\"", "");

                if (Content.IndexOf('\\')>0 || Content.IndexOf(".txt") > 0 || Content.IndexOf(".sql") > 0)  //Если в строке есть слеш, .txt или .sql - Считать что введен путь к файлу
                {
                    InputFile.FilePath = Content;
                }
                else  
                {
                    dbo.GetObjectsFromContent(Content); //Иначе парсить список объектов
                }
            }
            //Если агрументом не введена директория выгрузки, ввести в консоль
            if (OutputFile.FilePath == null)
            {
                Console.Write("Выгрузить в папку: ");
                OutputFile.FilePath = Console.ReadLine();
            }

            //Если путь к файлу не пустой, прочитать
            if (InputFile.FilePath != null)
            {
                InputFile.Read(InputFile.FilePath);
                //Если файл расширения .sql то получить список из скрипта, иначе считать списком объектов
                if (InputFile.FilePath.Substring(InputFile.FilePath.Length - 4) == ".sql")
                    dbo.GetObjectsFromSQL(InputFile.Content);
                else
                    dbo.GetObjectsFromContent(InputFile.Content);
            }



            foreach (string ObjectName in dbo.ObjectNames)
            {
                bool isWrited = false;
                // Для всех объектов кроме джобов
                dbo.GetObjectType(ObjectName);
                dbo.GetObjectSchema(ObjectName);
                if (dbo.ObjectType == "Table") { dbo.GetTableDefinition(dbo.ObjectSchema, ObjectName); }  //Если объект - таблица, то использовать специальный метод
                else dbo.GetObjectDefinition(ObjectName);
                if (dbo.ObjectSchema == "dbo")
                { OutputFile.FileName = $"{dbo.ObjectSchema}.{ObjectName}.{dbo.ObjectType}.sql"; } else { OutputFile.FileName = $"{ObjectName}.{dbo.ObjectType}.sql"; }
                OutputFile.Content = dbo.ObjectDefinition;
                if (OutputFile.Content != null)
                {
                    OutputFile.Write();
                    isWrited = true;
                }

                // Для джобов, Если нет прав на msdb - не искать
                try
                {
                    dbo.GetJobDefinition(ObjectName);  //Если найден джоб, то использовать специальный метод
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
                if (isWrited == false) { Console.WriteLine($"Нет данных для объекта {ObjectName}"); } //Если не записан никакой объект
            }
            if (args.Length == 0)
            {
                Console.WriteLine("Нажмите любую клавишу ...");
                Console.ReadKey();
            }
        }
    }
}
