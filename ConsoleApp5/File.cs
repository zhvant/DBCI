using System;
using System.IO;


/// <summary>
/// Класс для работы с файлами
/// </summary>
/// 
namespace GetSpNet
{
    class File
    {
        public string FilePath;
        public string FileName;
        public string Content;
        public void Write()
        {
            string FullPath = null;
            if (FilePath.Substring(FilePath.Length - 1, 1) == "\\")  //Если последний символ в пути не "\" , то добавить
            {
                FullPath = FilePath + FileName;
            }
            else
            {
                FullPath = FilePath + "\\" + FileName;
            }
            try
            {
                System.IO.Directory.CreateDirectory(FilePath);
                using (StreamWriter sw = new StreamWriter(FullPath, false, System.Text.Encoding.Default))
                {
                    if (Content != null)
                    {
                        sw.WriteLine(Content);
                        Console.WriteLine($"{FileName} успешно загружен в {FilePath}");
                    }
                    //else
                    //{
                    //    Console.WriteLine($"Нет данных для выгрузки");
                    //}

                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public string Read(string FullPath)
        {
            try
            {
                Console.WriteLine(FullPath);
                StreamReader sr = new StreamReader(FullPath);                
                String line = sr.ReadToEnd();
                return Content=line;
            }
            catch (IOException e)
            {
                Console.WriteLine("Не удалось прочитать файл: ");
                return e.Message;
            }
        }
    }
}