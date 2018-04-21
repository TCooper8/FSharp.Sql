namespace TCooper8.FSharp.Sql

open Npgsql
open System.Data.Common
open FSharp.AsyncUtil

module private Util =
  open System

  let (|Int|_|) (s:string) =
    match Int32.TryParse s with
    | false, _ -> None
    | true, v -> Some v

  let (|Bool|_|) (s:string) =
    match Boolean.TryParse s with
    | false, _ -> None
    | true, b -> Some b

  let (|SslMode|_|) (s:string) =
    match s.ToLower() with
    | "disable" -> Some SslMode.Disable
    | "prefer" -> Some SslMode.Prefer
    | "require" -> Some SslMode.Require
    | _ -> None

module ConnectionString =
  open System
  open System.Web

  let ofUri (uri:Uri) =
    let userInfo = uri.UserInfo
    let username, password =
      let i = userInfo.IndexOf ':'
      if i = -1 then userInfo.Substring(0, i), userInfo.Substring(i + 1)
      else userInfo, ""

    let queryMap =
      HttpUtility.ParseQueryString uri.Query
      |> fun col ->
        let keys = col.Keys |> Seq.cast<string>
        keys
        |> Seq.map (fun key -> key, col.[key])
      |> Map.ofSeq
    let query key action =
      Map.tryFind key queryMap
      |> Option.iter action

    let builder = Npgsql.NpgsqlConnectionStringBuilder()

    query "applicationName" (fun v -> builder.ApplicationName <- v)
    query "commandTimeout" (function
      | Util.Int i -> builder.CommandTimeout <- i
      | _ -> failwithf "Expected `commandTimeout` to be a valid int"
    )
    query "connectionIdleLifetime" (function
      | Util.Int i -> builder.ConnectionIdleLifetime <- i
      | _ -> failwithf "Expected `connectionIdleLifetime` to be a valid int"
    )
    builder.Database <- (uri.AbsolutePath.Substring 1)
    builder.Host <- uri.Host
    query "maxPoolSize" (function
      | Util.Int i -> builder.MaxPoolSize <- i
      | _ -> failwithf "Expected `maxPoolSize` to be a valid int"
    )
    query "minPoolSize" (function
      | Util.Int i -> builder.MinPoolSize <- i
      | _ -> failwithf "Expected `minPoolSize` to be a valid int"
    )
    builder.Password <- password
    query "pooling" (function
      | Util.Bool b -> builder.Pooling <- b
      | _ -> failwithf "Expected `pooling` to be a valid boolean"
    )
    builder.Port <- uri.Port
    query "sslMode" (function
      | Util.SslMode sslMode -> builder.SslMode <- sslMode
      | _ -> failwithf "Expected `sslMode` to be a valid SslMode of 'prefer', 'require' or 'disable'"
    )
    builder.Username <- username

    builder.ConnectionString

/// <summary> This module is a collection of utilities for using NpgsqlConnection(s). </summary>
module SqlConn =

  /// <summary> Create a new NpgsqlConnection from a connection string or uri. </summary>
  let connect connectionString = async {
    let conn = new NpgsqlConnection(connectionString)
    do! conn.OpenAsync() |> Async.AwaitTask
    return conn
  }

/// <summary> This module is a collection of utilities for an NpgsqlCommand. </summary>
module SqlCmd =
  /// <summary> Construct a new NpgsqlCommand from a SQL query. </summary>
  /// <param name="sql"> The SQL statement. </param>
  let cmd sql =
    new NpgsqlCommand(sql)

  let withParam (key:string) (value:obj) (cmd:NpgsqlCommand) =
    do cmd.Parameters.AddWithValue(key, value) |> ignore
    cmd

  let withParams (pairs: (string * obj) seq) (cmd:NpgsqlCommand) =
    pairs
    |> Seq.fold (fun cmd (key, value) -> withParam key value cmd) cmd

  let withConn conn (cmd:NpgsqlCommand) =
    cmd.Connection <- conn
    cmd

  let thenConnect connectionString (cmd:NpgsqlCommand) = async {
    let! conn = SqlConn.connect connectionString
    return withConn conn cmd
  }

  let asReader (cmd:NpgsqlCommand) =
    cmd.ExecuteReaderAsync()
    |> Async.AwaitTask

  let asScalar (cmd:NpgsqlCommand) =
    cmd.ExecuteScalarAsync()
    |> Async.AwaitTask
    |> fun task -> async {
      let! res = task
      return res :?> 'a
    }

  let exec (cmd:NpgsqlCommand) =
    cmd.ExecuteNonQueryAsync()
    |> Async.AwaitTask

module SqlReader =
  let field (reader:DbDataReader) name = async {
    let ord = reader.GetOrdinal name
    let! isNull = reader.IsDBNullAsync ord |> Async.AwaitTask
    if isNull then return None
    else
      let! value = reader.GetFieldValueAsync ord |> Async.AwaitTask
      return Some value
  }

  let ord (reader:DbDataReader) index =
    reader.GetFieldValueAsync index
    |> Async.AwaitTask

  let read (reader:DbDataReader) = async {
    let! ok = reader.ReadAsync() |> Async.AwaitTask
    if not ok then
      reader.Dispose()
      return None
    else return Some reader
  }

  let record (reader:DbDataReader) (fieldNames:string list): obj option list Async =
    let rec loop acc = function
      | [] -> async { return List.rev acc }
      | fieldName::fieldNames ->
        async {
          let! value = field reader fieldName
          return! loop (value::acc) fieldNames
        }
    loop [] fieldNames

module SqlQuery =
  type SqlQuery =
    | SqlQuery of string
    | SqlPrepared of string * (string * obj) seq

  let exec connectionString = function
    | SqlQuery queryString ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.thenConnect connectionString
      |> Async.bind SqlCmd.exec

    | SqlPrepared (queryString, ps) ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.withParams ps
      |> SqlCmd.thenConnect connectionString
      |> Async.bind SqlCmd.exec

  let asReader connectionString = function
    | SqlQuery queryString ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.thenConnect connectionString
      |> Async.bind SqlCmd.asReader

    | SqlPrepared (queryString, ps) ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.withParams ps
      |> SqlCmd.thenConnect connectionString
      |> Async.bind SqlCmd.asReader

  let asScalar connectionString = function
    | SqlQuery queryString ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.thenConnect connectionString
      |> Async.bind SqlCmd.asScalar

    | SqlPrepared (queryString, ps) ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.withParams ps
      |> SqlCmd.thenConnect connectionString
      |> Async.bind SqlCmd.asScalar

  let transaction connectionString = function
    | SqlQuery queryString ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.thenConnect connectionString
      |> Async.bind (fun cmd -> async {
        let transaction = cmd.Connection.BeginTransaction()
        let! res = cmd |> SqlCmd.exec |> Async.Catch
        match res with
        | Choice2Of2 e ->
          do! transaction.RollbackAsync() |> Async.AwaitTask
          raise e
          return -1
        | Choice1Of2 res ->
          do! transaction.CommitAsync() |> Async.AwaitTask
          return res
      })

    | SqlPrepared (queryString, ps) ->
      queryString
      |> SqlCmd.cmd
      |> SqlCmd.withParams ps
      |> SqlCmd.thenConnect connectionString
      |> Async.bind (fun cmd -> async {
        let transaction = cmd.Connection.BeginTransaction()
        let! res = cmd |> SqlCmd.exec |> Async.Catch
        match res with
        | Choice2Of2 e ->
          do! transaction.RollbackAsync() |> Async.AwaitTask
          raise e
          return -1
        | Choice1Of2 res ->
          do! transaction.CommitAsync() |> Async.AwaitTask
          return res
      })
