using System;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Research.Naiad.Examples.Experiment
{
    public enum State
    {
        ESTIMATED,
        KNOWN,
        UNDERWAY,
        FINISHING,
        DONE
    };

    public class Nudger
    {
        public readonly string Name;

        private List<Pair<Epoch, State>> Transitions;

        public State State { get => Transitions.Last().Second; }

        public Nudger(Epoch epoch, string Name)
        {
            this.Name = Name;
            Transitions = new List<Pair<Epoch, State>>();
            Transitions.Add(new Pair<Epoch, State>(epoch, State.ESTIMATED));
        }

        public State Nudge(Epoch epoch)
        {
            if (this.State != State.DONE)
                Transitions.Add(new Pair<Epoch, State>(epoch, this.State + 1));
            return State;
        }

        public bool HasStateAt(Epoch epoch)
        {
            // LessThan includes equal
            return this.Transitions.First().First.LessThan(epoch);
        }

        public Nullable<State> StateAt(Epoch epoch)
        {
            if (!HasStateAt(epoch)) return null;

            var last = Transitions.FindLast(pair => pair.First.LessThan(epoch));
            return last.Second;
        }

        public override string ToString()
        {
            return "Nudger[" + Name + ", " + State + ", " + Transitions.Last().First.ToString() + "]";
        }
    }


    //input - a string
    //output - a transitioned nudge
    //internal class NudgerVertex : UnaryVertex<string, Nudger, Epoch>
    //{
    //    private readonly Dictionary<string, Nudger> Nudgers = new Dictionary<string, Nudger>();
    //    private readonly HashSet<string> Changed = new HashSet<string>();

    //    public override void OnReceive(Message<string, Epoch> message)
    //    {
    //        Console.WriteLine("NudgerVertex: OnReceive");
    //        this.NotifyAt(message.time);

    //        for (int i = 0; i < message.length; i++)
    //        {
    //            var data = message.payload[i];
    //            if (!this.Nudgers.ContainsKey(data))
    //                this.Nudgers[data] = new Nudger(message.time, data);
    //            else
    //                this.Nudgers[data].Nudge(message.time);

    //            this.Changed.Add(data);
    //        }
    //    }

    //    public override void OnNotify(Epoch time)
    //    {
    //        var output = this.Output.GetBufferForTime(time);
    //        foreach (var record in this.Changed)
    //            output.Send(Nudgers[record]);

    //        this.Changed.Clear();
    //        Console.WriteLine("NudgerVertex: Received OnNotify for Epoch: " + this.ToString());
    //    }

    //    public NudgerVertex(int index, Stage<Epoch> stage) : base(index, stage) { }

    //}

    internal class NudgerVertex : UnaryVertex<string, NudgerUpdate, Epoch>
    {
        private readonly Dictionary<string, Nudger> Nudgers = new Dictionary<string, Nudger>();

        public override void OnReceive(Message<string, Epoch> message)
        {
            Console.WriteLine("NudgerVertex: OnReceive");
            this.NotifyAt(message.time);

            for (int i = 0; i < message.length; i++)
            {
                var data = message.payload[i];
                var state = State.ESTIMATED;
                if (!this.Nudgers.ContainsKey(data))
                    this.Nudgers[data] = new Nudger(message.time, data);
                else
                    state = this.Nudgers[data].Nudge(message.time);

                var update = new NudgerUpdate(data, state);

                var output = this.Output.GetBufferForTime(message.time);
                Console.WriteLine("Sending update: " + update + ", i = " + i);
                output.Send(update);
            }
        }

        public override void OnNotify(Epoch time)
        {

            Console.WriteLine("NudgerVertex: Received OnNotify for Epoch: " + this.ToString());
        }

        public NudgerVertex(int index, Stage<Epoch> stage) : base(index, stage) { }

    }

    internal struct NudgerUpdate
    {
        public readonly string NudgerName;
        public readonly State NewState;
        public NudgerUpdate(string name, State State)
        {
            NudgerName = name;
            NewState = State;
        }
        public override string ToString()
        {
            return "NudgerUpdate[" + NudgerName + ", " + NewState + "]";
        }
    }

    internal class HyperVertex : BinaryVertex<NudgerUpdate, Epoch, IReadOnlyDictionary<string, State>, Epoch>
    {
        private SortedDictionary<Epoch, IReadOnlyDictionary<string, State>> Indices;

        private Dictionary<Epoch, List<NudgerUpdate>> InFlightUpdates;

        public bool HasStateAt(Epoch epoch)
        {
            return this.Indices.First().Key.LessThan(epoch);
        }

        public IReadOnlyDictionary<string, State> EntriesAt(Epoch epoch)
        {
            if (!HasStateAt(epoch)) return new Dictionary<string, State>();

            var last = Indices.ToList().FindLast(pair => pair.Key.LessThan(epoch));
            return last.Value;
        }

        public HyperVertex(int index, Stage<Epoch> stage) : base(index, stage)
        {
            Indices = new SortedDictionary<Epoch, IReadOnlyDictionary<string, State>>();
            InFlightUpdates = new Dictionary<Epoch, List<NudgerUpdate>>();
        }

        public override void OnReceive1(Message<NudgerUpdate, Epoch> message)
        {
            Console.WriteLine("HyperVertex: OnReceive1");
            this.NotifyAt(message.time);

            if (!InFlightUpdates.ContainsKey(message.time))
                InFlightUpdates[message.time] = new List<NudgerUpdate>();


            InFlightUpdates[message.time].AddRange(message.payload.Take(message.length));
        }

        public override void OnReceive2(Message<Epoch, Epoch> message)
        {
            Console.WriteLine("HyperVertex: OnReceive2");
            this.NotifyAt(message.time);
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                Console.WriteLine("HyperIndex: obtaining for i = " + i + ", payload = " + message.payload[i]);
                output.Send(this.EntriesAt(message.payload[i]));
            }


        }

        public override void OnNotify(Epoch time)
        {
            Console.WriteLine("HyperVertex: Received OnNotify for Epoch: " + time.ToString());
            var newPairs = new Dictionary<string, State>();
            if (Indices.Count > 0)
            {
                var latestSolid = Indices.Last();
                if (!latestSolid.Key.LessThan(time))
                {
                    //erm, that shouldn't happen
                    Console.WriteLine("HyperIndex: times appear out of order: OnNotify: "
                        + time.ToString() + ", latest solid: " + latestSolid.Key.ToString());
                }
                // "clone" the latest entry
                newPairs = latestSolid.Value.ToDictionary(k => k.Key, v => v.Value);
            }
            if (InFlightUpdates.ContainsKey(time))
            {
                // merge updates
                var changes = InFlightUpdates[time];
                Console.WriteLine("HyperVertex: changes:");

                foreach (var change in changes)
                {
                    Console.WriteLine("\t" + change);
                    newPairs[change.NudgerName] = change.NewState;
                }
                // add to soldified state
                Indices[time] = newPairs;

                InFlightUpdates.Remove(time);
            }
            Console.WriteLine("HyperVertex: Finished OnNotify for Epoch: " + time.ToString());
        }
    }


    public class Experiment : Example
    {
        public string Usage => "No args required";

        public string Help => "Does some stuff";

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // let our "processes" be known by strings
                var instructions = new BatchedDataSource<string>();
                // and our queries be made by timestamps of ints
                var queries = new BatchedDataSource<Epoch>();


                var instrStreamInput = computation.NewInput(instructions);
                var queryStreamInput = computation.NewInput(queries);
                var s1 = instrStreamInput.NewUnaryStage((i, s) => new NudgerVertex(i, s), x => x.GetHashCode(), null, "Nudgers");

                var s2 = Foundry.NewBinaryStage(s1, queryStreamInput, (i, s) => new HyperVertex(i, s),
                    x => x.GetHashCode(), y => y.GetHashCode(), z => z.GetHashCode(), "Hypers");

                s2.Subscribe(list =>
                {
                    foreach (var element in list)
                    {
                        foreach(var kv in element)
                            Console.WriteLine(kv.Key + " : " + kv.Value);
                    }
                });
                computation.Activate();

                if (computation.Configuration.ProcessID == 0)
                {
                    // with our dataflow graph defined, we can start soliciting strings from the user.
                    Console.WriteLine("Start entering names of Nudgers that are getting transitioned");
                    Console.WriteLine("Naiad will display the status of changed Nudgers");

                    // read lines of input and hand them to the input, until an empty line appears.
                    for (var line = Console.ReadLine(); line.Length > 0; line = Console.ReadLine())
                    {
                        var split = line.Trim().Split();
                        if (split.Length == 1)
                        {
                            int i = 0;
                            if (int.TryParse(split[0], out i))
                            {
                                queries.OnNext(new Epoch(i));
                                instructions.OnNext();
                            }
                            else
                            {
                                instructions.OnNext(split);
                                queries.OnNext();
                            }
                        }
                        else
                        {
                            instructions.OnNext(split);
                            queries.OnNext();
                        }
                    }


                }

                instructions.OnCompleted();   // signal the end of the input.
                queries.OnCompleted();
                computation.Join();           // waits until the graph has finished executing.
            }
        }
    }
}

