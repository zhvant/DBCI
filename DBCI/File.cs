using System;
using System.IO;


/// <summary>
/// Class for work with files
/// </summary>
/// 
namespace DBCI
{
    class File
    {
        public string FilePath;
        public string FileName;
        public string Content;
        public void Write()
        {
            string FullPath = null;
            if (FilePath.Substring(FilePath.Length - 1, 1) == "\\")  //If the last character in the path is not "\" then add
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
                using (StreamWriter sw = new StreamWriter(FullPath, false, System.Text.Encoding.UTF8))
                {
                    if (Content != null)
                    {
                        sw.WriteLine(Content);
                        Console.WriteLine($"{FileName} successfully uploaded to {FilePath}");
                    }
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
                Console.WriteLine("Failed to read file: ");
                return e.Message;
            }
        }
    }
}