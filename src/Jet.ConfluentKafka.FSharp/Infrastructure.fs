namespace Jet.ConfluentKafka.FSharp

open System
open System.Threading.Tasks

[<AutoOpen>]
module private AsyncHelpers =
    module Option =
        let inline defaultValue defaultValue = function Some v -> v | None -> defaultValue
        let orElse ifNone option = match option with None -> ifNone | Some _ -> option
#if NET461
    module Array =
        let head xs = Seq.head xs
        let last xs = Seq.last xs
#endif
    type Async =
        static member map (f : 't -> 'u) (xa: Async<'t>): Async<'u> = async {
            let! x = xa
            return f x }
        static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task<'T>) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k t.Result
                    else ek(Exception "invalid Task state!"))
                |> ignore

        static member AwaitTaskCorrect (task : Task) : Async<unit> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k ()
                    else ek(Exception "invalid Task state!"))
                |> ignore
