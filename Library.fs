namespace TCooper8.FSharp.Sql

open Npgsql
open System.Data.Common
open FSharp.AsyncUtil

module SqlCmd =
  let connect connectionString = async {
    let conn = new NpgsqlConnection(connectionString)
    do! conn.OpenAsync() |> Async.AwaitTask
    return conn
  }

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
    let! conn = connect connectionString
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
