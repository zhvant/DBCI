using System;
using System.Data.SqlClient;
using System.Linq;

/// <summary>
/// Класс для работы с объектами БД
/// </summary>
/// 
namespace GetSpNet
{
    class DataObject
    {
        public string Server;
        public string Database;
        public string User;
        public string Password;

        public string[] ObjectNames;
        public string ObjectDefinition;
        public string ObjectType;
        public string ObjectSchema;

        public string GetConnectionString()
        {
            string connectionString = "";
            if (User != null)
            {
                connectionString = $"Data Source={Server};Initial Catalog={Database};User ID={User};Password={Password}";
            }
            else connectionString = $"Data Source={Server};Initial Catalog={Database};Integrated Security = True";

            return connectionString;
        }

        public string GetObjectType(string ObjectName)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    connection.Open();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("Нажмите любую клавишу ...");
                    Console.ReadKey();
                    Environment.Exit(1);
                    return null;
                }

                // Поиск типа объекта в системных объектах
                SqlCommand getTypeSQL = new SqlCommand(
                $@"select 
                 case
                 when type = 'P' then 'StoredProcedure'
                 when type = 'U' then 'Table'
                 when type in ('FN', 'IF', 'FS', 'TF') then 'UserDefinedFunction'
                 when type = 'SN' then 'Synonym'
                 when type = 'V' then 'View'
                 else '_'
                 end as TypeName
                  from sys.objects
                 where object_id = OBJECT_ID(N'{ObjectName}');", connection);
                using (SqlDataReader reader = getTypeSQL.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            ObjectType = reader.GetString(0);
                        }
                    }
                    reader.Close();
                }

                //// Поиск типа объекта в джобах
                //SqlCommand getJobTypeSQL = new SqlCommand(
                //$@"select 'Job' type
                // from msdb.dbo.sysjobs
                // where name='{ObjectName}';", connection);
                
                //using (SqlDataReader reader = getJobTypeSQL.ExecuteReader())
                //{
                //    if (reader.HasRows)
                //    {
                //        while (reader.Read())
                //        {
                //            ObjectType = reader.GetString(0);
                //        }
                //    }
                //    reader.Close();
                //}
            }
            return ObjectType;
        }

        public string GetObjectSchema (string ObjectName)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    connection.Open();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("Нажмите любую клавишу ...");
                    Console.ReadKey();
                    Environment.Exit(1);
                    return null;
                }
                SqlCommand getTypeSQL = new SqlCommand(
                $@"select sh.name
                 from sys.objects o join sys.schemas sh on o.schema_id = sh.schema_id
                 where object_id = OBJECT_ID(N'{ObjectName}');", connection);
                using (SqlDataReader reader = getTypeSQL.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            ObjectSchema = reader.GetString(0);
                        }
                    }
                    reader.Close();
                }
            }
            return ObjectSchema;
        }

        public string GetObjectDefinition(string ObjectName)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    connection.Open();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("Нажмите любую клавишу ...");
                    Console.ReadKey();
                    Environment.Exit(1);
                    return null;
                }
                SqlCommand getDefinitionSQL = new SqlCommand($"SELECT OBJECT_DEFINITION (OBJECT_ID(N'{ObjectName}'));", connection);

                using (SqlDataReader reader = getDefinitionSQL.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            try
                            {
                                ObjectDefinition = reader.GetString(0);
                            }
                            catch
                            {
                                ObjectDefinition = null;
                            }
                        }
                    }
                    reader.Close();
                }
                return ObjectDefinition;
            }
        }

        public string GetTableDefinition(string SchemaName, string ObjectName)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    connection.Open();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("Нажмите любую клавишу ...");
                    Console.ReadKey();
                    Environment.Exit(1);
                    return null;
                }
                SqlCommand getDefinitionSQL = new SqlCommand(
$@"DECLARE @table_name SYSNAME
SELECT @table_name = '{SchemaName}.{ObjectName}'

DECLARE
@object_name SYSNAME
, @object_id INT

SELECT
@object_name = '[' + s.name + '].[' + o.name + ']'
, @object_id = o.[object_id]FROM sys.objects o WITH(NOWAIT)JOIN sys.schemas s WITH(NOWAIT) ON o.[schema_id] = s.[schema_id]WHERE s.name + '.' + o.name = @table_name
AND o.[type] = 'U'
AND o.is_ms_shipped = 0

DECLARE @SQL NVARCHAR(MAX) = ''

; WITH index_column AS
 (
 SELECT
 ic.[object_id]
 , ic.index_id
 , ic.is_descending_key
 , ic.is_included_column
 , c.name
 FROM sys.index_columns ic WITH(NOWAIT)
 JOIN sys.columns c WITH(NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
 WHERE ic.[object_id] = @object_id
 ),
fk_columns AS
(
SELECT
k.constraint_object_id
, cname = c.name
, rcname = rc.name
FROM sys.foreign_key_columns k WITH (NOWAIT)
JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id
JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
WHERE k.parent_object_id = @object_id
)SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF((
SELECT CHAR(9) + ', [' + c.name + '] ' +
CASE WHEN c.is_computed = 1
THEN 'AS ' + cc.[definition]
ELSE UPPER(tp.name) +
CASE WHEN tp.name IN('varchar', 'char', 'varbinary', 'binary', 'text')
THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
WHEN tp.name IN('nvarchar', 'nchar', 'ntext')
THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
WHEN tp.name IN('datetime2', 'time2', 'datetimeoffset')
THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
WHEN tp.name = 'decimal'
THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
ELSE ''
END +
--CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +
CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
CASE WHEN dc.[definition] IS NOT NULL THEN ' DEFAULT' + dc.[definition] ELSE '' END +
CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS CHAR(1)) + ',' + CAST(ISNULL(ic.increment_value, '1') AS CHAR(1)) + ')' ELSE '' END
END + CHAR(13)
FROM sys.columns c WITH(NOWAIT)
JOIN sys.types tp WITH(NOWAIT) ON c.user_type_id = tp.user_type_id
LEFT JOIN sys.computed_columns cc WITH(NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
LEFT JOIN sys.default_constraints dc WITH(NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
LEFT JOIN sys.identity_columns ic WITH(NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
WHERE c.[object_id] = @object_id
ORDER BY c.column_id
FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')
+ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' +
(SELECT STUFF((
SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
FROM sys.index_columns ic WITH(NOWAIT)
JOIN sys.columns c WITH(NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
WHERE ic.is_included_column = 0
AND ic.[object_id] = k.parent_object_id
AND ic.index_id = k.unique_index_id
FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))
+')' + CHAR(13)
FROM sys.key_constraints k WITH(NOWAIT)
WHERE k.parent_object_id = @object_id
AND k.[type] = 'PK'), '') + ')' + CHAR(13)
+ ISNULL((SELECT (
SELECT CHAR(13) +
'ALTER TABLE ' + @object_name + ' WITH'
+ CASE WHEN fk.is_not_trusted = 1
THEN ' NOCHECK'
ELSE ' CHECK'
END +
' ADD CONSTRAINT [' + fk.name + '] FOREIGN KEY('
+ STUFF((
SELECT ', [' + k.cname + ']'
FROM fk_columns k
WHERE k.constraint_object_id = fk.[object_id]
FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
+ ')' +
' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('
+ STUFF((
SELECT ', [' + k.rcname + ']'
FROM fk_columns k
WHERE k.constraint_object_id = fk.[object_id]
FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
+ ')'
+ CASE
WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE'
WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'
WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT'
ELSE ''
END
+ CASE
WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'
WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'
WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT'
ELSE ''
END
+ CHAR(13) + 'ALTER TABLE ' + @object_name + ' CHECK CONSTRAINT [' + fk.name + ']' + CHAR(13)
FROM sys.foreign_keys fk WITH (NOWAIT)
JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
WHERE fk.parent_object_id = @object_id
FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), '')
+ ISNULL(((SELECT
CHAR(13) + 'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END
+ ' NONCLUSTERED INDEX [' + i.name + '] ON ' + @object_name + ' (' +
STUFF((
SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
FROM index_column c
WHERE c.is_included_column = 0
AND c.index_id = i.index_id
FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
+ ISNULL(CHAR(13) + 'INCLUDE (' +
STUFF((
SELECT ', [' + c.name + ']'
FROM index_column c
WHERE c.is_included_column = 1
AND c.index_id = i.index_id
FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '') + CHAR(13)
FROM sys.indexes i WITH (NOWAIT)
WHERE i.[object_id] = @object_id
AND i.is_primary_key = 0
AND i.[type] = 2
FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
), '')

SELECT @SQL", connection);

                using (SqlDataReader reader = getDefinitionSQL.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            try
                            {
                                ObjectDefinition = reader.GetString(0);
                            }
                            catch
                            {
                                ObjectDefinition = null;
                            }
                        }
                    }
                    reader.Close();
                }
                return ObjectDefinition;
            }
        }

        public string GetJobDefinition(string ObjectName)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    connection.Open();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("Нажмите любую клавишу ...");
                    Console.ReadKey();
                    Environment.Exit(1);
                    return null;
                }
                SqlCommand getDefinitionSQL = new SqlCommand(
$@"
declare
@enabled nvarchar(max)
, @notify_level_eventlog nvarchar(max)
, @notify_level_email nvarchar(max)
, @notify_level_netsend nvarchar(max)
, @notify_level_page nvarchar(max)
, @delete_level nvarchar(max)
, @description nvarchar(max)
, @category_name nvarchar(max)
, @owner_login_name nvarchar(max)
, @job_id nvarchar(max)
, @category_class nvarchar(max)
, @start_step_id nvarchar(max)
, @job_name nvarchar(max)


select
@enabled =[enabled]
, @notify_level_eventlog = [notify_level_eventlog]
, @notify_level_email = [notify_level_email]
, @notify_level_netsend = [notify_level_netsend]
, @notify_level_page = [notify_level_page]
, @delete_level = [delete_level]
, @description = [description]
, @category_name = c.[name]
, @owner_login_name = sl.[name]
, @job_id = [job_id]
, @category_class = category_class
, @start_step_id = start_step_id
, @job_name = j.name
FROM[msdb].[dbo].[sysjobs] j
join[msdb].[dbo].[syscategories] c on j.[category_id] = c.[category_id]
left join sys.syslogins sl on j.owner_sid = sl.sid
where[j].[name] = '{ObjectName}'
                               

declare @Code nvarchar(max)
set @Code =
'USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS(SELECT name FROM msdb.dbo.syscategories WHERE name = N'''+@category_name+''' AND category_class = '+@category_class+')
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class = N''JOB'', @type = N''LOCAL'', @name = N'''+@category_name+'''
IF(@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name = N'''+@job_name+''',
        @enabled = '+@enabled+',
        @notify_level_eventlog = '+@notify_level_eventlog+',
        @notify_level_email = '+@notify_level_email+',
        @notify_level_netsend = '+@notify_level_netsend+',
        @notify_level_page = '+@notify_level_page+',
        @delete_level = '+@delete_level+',
        @description = N'''+@description+''',
        @category_name = N'''+@category_name+''',
        @owner_login_name = N'''+@owner_login_name+''', @job_id = @jobId OUTPUT
IF(@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback'


declare
 @step_name               nvarchar(max)
, @step_id                 nvarchar(max)
, @cmdexec_success_code    nvarchar(max)
, @on_success_action       nvarchar(max)
, @on_success_step_id      nvarchar(max)
, @on_fail_action          nvarchar(max)
, @on_fail_step_id         nvarchar(max)
, @retry_attempts          nvarchar(max)
, @retry_interval          nvarchar(max)
, @os_run_priority         nvarchar(max)
, @command                 nvarchar(max)
, @database_name           nvarchar(max)
, @flags                   nvarchar(max)
, @subsystem               nvarchar(max)

, @steps int = (select MAX(step_id) from msdb.dbo.sysjobsteps where job_id = @job_id)
, @current_step int = 1


while @current_step <= @steps
begin
select
@step_name = step_name
,@step_id = step_id
,@cmdexec_success_code = cmdexec_success_code
,@on_success_action = on_success_action
,@on_success_step_id = on_success_step_id
,@on_fail_action = on_fail_action
,@on_fail_step_id = on_fail_step_id
,@retry_attempts = retry_attempts
,@retry_interval = retry_interval
,@os_run_priority = os_run_priority
,@command = REPLACE(command, '''', '''''')
,@database_name = database_name
,@flags = flags
,@subsystem = subsystem
from
msdb.dbo.sysjobsteps
where
job_id = @job_id
and
step_id = @current_step


set @Code = @Code + '
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id = @jobId, @step_name = N'''+@step_name+''', 
		@step_id = '+@step_id+', 
		@cmdexec_success_code = '+@cmdexec_success_code+', 
		@on_success_action = '+@on_success_action+', 
		@on_success_step_id = '+@on_success_step_id+', 
		@on_fail_action = '+@on_fail_action+', 
		@on_fail_step_id = '+@on_fail_step_id+', 
		@retry_attempts = '+@retry_attempts+', 
		@retry_interval = '+@retry_interval+', 
		@os_run_priority = '+@os_run_priority+', @subsystem = N'''+@subsystem+''', 
		@command = N'''+@command+''', 
		@database_name = N'''+@database_name+''', 
		@flags = '+@flags+'
IF(@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
'
set @current_step = @current_step + 1
end

set @Code = @Code + '
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = ' + @start_step_id + '
IF(@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback'


declare
@name nvarchar(max)
,@schedule_enabled nvarchar(max)
,@freq_type nvarchar(max)
,@freq_interval nvarchar(max)
,@freq_subday_type nvarchar(max)
,@freq_subday_interval nvarchar(max)
,@freq_relative_interval nvarchar(max)
,@freq_recurrence_factor nvarchar(max)
,@active_start_date nvarchar(max)
,@active_end_date nvarchar(max)
,@active_start_time nvarchar(max)
,@active_end_time nvarchar(max)
,@schedule_uid nvarchar(max)


DECLARE schedules_cursor CURSOR FOR
select
 name
, enabled
, freq_type
, freq_interval
, freq_subday_type
, freq_subday_interval
, freq_relative_interval
, freq_recurrence_factor
, active_start_date
, active_end_date
, active_start_time
, active_end_time
, schedule_uid
 from msdb.dbo.sysschedules ss left
 join msdb.dbo.sysjobschedules sj on ss.schedule_id = sj.schedule_id
where job_id = @job_id
order by name

OPEN schedules_cursor
FETCH NEXT FROM schedules_cursor INTO
@name
,@schedule_enabled
,@freq_type
,@freq_interval
,@freq_subday_type
,@freq_subday_interval
,@freq_relative_interval
,@freq_recurrence_factor
,@active_start_date
,@active_end_date
,@active_start_time
,@active_end_time
,@schedule_uid

WHILE @@FETCH_STATUS = 0
BEGIN
set @Code = @Code + '
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N''' + @name + ''', 

        @enabled = '+@schedule_enabled+', 
		@freq_type = '+@freq_type+', 
		@freq_interval = '+@freq_interval+', 
		@freq_subday_type = '+@freq_subday_type+', 
		@freq_subday_interval = '+@freq_subday_interval+', 
		@freq_relative_interval = '+@freq_relative_interval+', 
		@freq_recurrence_factor = '+@freq_recurrence_factor+', 
		@active_start_date = '+@active_start_date+', 
		@active_end_date = '+@active_end_date+', 
		@active_start_time = '+@active_start_time+', 
		@active_end_time = '+@active_end_time+', 
		@schedule_uid = N'''+@schedule_uid+'''
IF(@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
'

FETCH NEXT FROM schedules_cursor INTO
@name
, @schedule_enabled
, @freq_type
, @freq_interval
, @freq_subday_type
, @freq_subday_interval
, @freq_relative_interval
, @freq_recurrence_factor
, @active_start_date
, @active_end_date
, @active_start_time
, @active_end_time
, @schedule_uid

END

CLOSE schedules_cursor
DEALLOCATE schedules_cursor


set @Code = @Code + '
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N''(local)''
IF(@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF(@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
    GO'

select @Code
", connection);

                using (SqlDataReader reader = getDefinitionSQL.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            try
                            {
                                ObjectDefinition = reader.GetString(0);
                            }
                            catch
                            {
                                ObjectDefinition = null;
                            }
                        }
                    }
                    reader.Close();
                }


                return ObjectDefinition;
            }
        }

        public  string[] GetObjectsFromContent(string Content)
        {
            if (Content != null)
            {
                Content = Content.Replace("\r", "");
                Content = Content.Replace("п»ї", "");
                Content = Content.Replace("dbo.", "");
                Content = Content.Replace(".StoredProcedure", "");
                Content = Content.Replace(".Table", "");
                Content = Content.Replace(".Job", "");
                Content = Content.Replace(".UserDefinedFunction", "");
                Content = Content.Replace(".Synonym", "");
                Content = Content.Replace(".View", "");
                Content = Content.Replace(".sql", "");
                Content = Content.Replace(" ", "");           
            return ObjectNames = Content.Split('\n',',',';');
            }
            else
            {
                return null;
            };
        }

        public string[] GetObjectsFromSQL(string Content)
        {
            if (Content != null)
            {
                using (SqlConnection connection = new SqlConnection(GetConnectionString()))
                {
                    try
                    {
                        connection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        Console.WriteLine("Нажмите любую клавишу ...");
                        Console.ReadKey();
                        Environment.Exit(1);
                        return null;
                    }
                    SqlCommand getObjectsSQL = new SqlCommand(
                    Content, connection);
                    
                    using (SqlDataReader reader = getObjectsSQL.ExecuteReader())
                    {
                        string ObjectStringNames = "";
                        if (reader.HasRows)
                        {
                            
                            while (reader.Read())
                            {
                                ObjectStringNames+=";"+reader.GetString(0); 
                            }
                        }
                        reader.Close();
                        ObjectNames = ObjectStringNames.Split(';');
                        ObjectNames = ObjectNames.Where(x => x != "").ToArray();
                    }
                }
                return ObjectNames;
            }
            else
            {
                return null;
            };
        }
    }
}
