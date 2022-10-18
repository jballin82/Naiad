using System;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Research.Naiad.Examples.Experiment
{
    #region Objects
    public enum State
    {
        ESTIMATED,
        KNOWN,
        UNDERWAY,
        FINISHING,
        DONE
    };

    public enum Colour
    {
        RED,
        GREEN,
        BLUE,
        YELLOW
    };

    public class Nudger
    {
        public readonly string Name;

        public readonly Colour Colour;

        private List<Pair<Epoch, State>> Transitions;

        public State State { get => Transitions.Last().Second; }

        public Nudger(Epoch epoch, string Name)
        {
            this.Name = Name;
            switch (Name.GetHashCode() % 4)
            {
                case 0:
                    Colour = Colour.RED;
                    break;
                case 1:
                    Colour = Colour.BLUE;
                    break;
                case 2:
                    Colour = Colour.GREEN;
                    break;
                default:
                    Colour = Colour.YELLOW;
                    break;

            };
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

    internal struct NudgerUpdate
    {
        public readonly string NudgerName;
        public readonly Colour NudgerColour;
        public readonly State NewState;
        public NudgerUpdate(string name, Colour colour, State state)
        {
            NudgerName = name;
            NudgerColour = colour;
            NewState = state;
        }
        public override string ToString()
        {
            return String.Format("NudgerUpdate[{0}, {1}, {2}]", NudgerName, NudgerColour, NewState);
        }
    }
    #endregion


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

                var colour = this.Nudgers[data].Colour;
                var update = new NudgerUpdate(data, colour, state);

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



    internal class HyperVertex : BinaryVertex<NudgerUpdate, Epoch, Pair<Epoch, IReadOnlyDictionary<string, State>>, Epoch>
    {
        private SortedDictionary<Epoch, IReadOnlyDictionary<string, State>> Indices;

        private Dictionary<Epoch, List<NudgerUpdate>> InFlightUpdates;

        public bool HasStateAt(Epoch epoch)
        {
            return this.Indices.First().Key.LessThan(epoch);
        }

        public IReadOnlyDictionary<string, State> EntriesAt(Epoch epoch)
        {
            Console.WriteLine("HyperVertex {0} state: ", this.VertexId);
            foreach (var item in Indices)
            {
                Console.WriteLine("{0}: {1}: {2}", this.VertexId, item.Key,
                    string.Join(",", item.Value.Select(kv => "[" + kv.Key + ": " + kv.Value + "]")));
            }
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
            Console.WriteLine("HyperVertex {0}: OnReceive1 (nudger updates)", this.VertexId);
            this.NotifyAt(message.time);

            if (!InFlightUpdates.ContainsKey(message.time))
                InFlightUpdates[message.time] = new List<NudgerUpdate>();


            for (int i = 0; i < message.length; i++)
            {
                Console.WriteLine("HyperVertex {0}: {1}: {2}", this.VertexId, i, message.payload[i]);
            }
            InFlightUpdates[message.time].AddRange(message.payload.Take(message.length));
        }

        public override void OnReceive2(Message<Epoch, Epoch> message)
        {
            Console.WriteLine("HyperVertex {0}: OnReceive2 (query)", this.VertexId);
            this.NotifyAt(message.time);
            var output = this.Output.GetBufferForTime(message.time);

            for (int i = 0; i < message.length; i++)
            {
                var queryTime = message.payload[i];
                Console.WriteLine("HyperVertex {0}: reading state for epoch: {1}", this.VertexId, queryTime);
                output.Send(new Pair<Epoch, IReadOnlyDictionary<string, State>>(queryTime, this.EntriesAt(queryTime)));
            }

        }

        public override void OnNotify(Epoch time)
        {
            Console.WriteLine("HyperVertex {0}: Received OnNotify for Epoch: {1}", this.VertexId, time);
            var newPairs = new Dictionary<string, State>();
            if (Indices.Count > 0)
            {
                var latestSolid = Indices.Last();
                if (!latestSolid.Key.LessThan(time))
                {
                    //erm, that shouldn't happen
                    Console.WriteLine("HyperVertex: times appear out of order: OnNotify: "
                        + time.ToString() + ", latest solid: " + latestSolid.Key.ToString());
                }
                // "clone" the latest entry
                newPairs = latestSolid.Value.ToDictionary(k => k.Key, v => v.Value);
            }
            if (InFlightUpdates.ContainsKey(time))
            {
                // merge updates
                var changes = InFlightUpdates[time];
                Console.WriteLine("HyperVertex {0}: changes:", this.VertexId);

                foreach (var change in changes)
                {
                    Console.WriteLine("\t+HyperVertex {0}: {1}", this.VertexId, change);
                    newPairs[change.NudgerName] = change.NewState;
                }
                // add to soldified state
                Indices[time] = newPairs;

                InFlightUpdates.Remove(time);
            }
            Console.WriteLine("HyperVertex {0}: Finished OnNotify for Epoch: {1}", this.VertexId, time);
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
                // and our queries be made by timestamps of ints
                var instructions = new BatchedDataSource<string>();
                var queries = new BatchedDataSource<Epoch>();

                /*
                 * instructions >
                 *  instrStreamInput > s1 (Nudgers)-----\
                 *                                       \
                 *                                        s2 (Hypers) ----> query results
                 *                                       /
                 *  queryStreamInput > -----------------/
                 * queries >
                 * 
                 */

                var instrStreamInput = computation.NewInput(instructions);
                var queryStreamInput = computation.NewInput(queries);

                // Apparently stable partition function:
                // input: nudger name's hash code
                // output: *nudgerupdate* name's hash code
                var s1 = Foundry.NewUnaryStage(instrStreamInput,
                    (i, s) => new NudgerVertex(i, s), x => x.GetHashCode(), x => x.NudgerColour.GetHashCode(), "Nudgers");

                // partition all by 0 => everything comes together
                // In the KeyValueLookup example, the two partition functions are the same: Updating the value associated with a key and querying the value associated with a key get sent to the same vertex
                // but here, Naiad is going to direct the query to just one vertex, which we don't want (if we have multiple vertices)
                // Reasoning that setting the partition function to zero on *both* streams of input implies a singleton
                var s2 = Foundry.NewBinaryStage(s1, queryStreamInput,
                    (i, s) => new HyperVertex(i, s),
                    x => 0, y => 0, z => 0, "Hypers");

                // subscribe to the output of the Hyper vertex
                s2.Subscribe(list =>
                {
                    Console.WriteLine("Receiving hyper outputs:");
                    Console.WriteLine("| Epoch\t| Nudger\t| State\t\t|");
                    foreach (var element in list)
                    {
                        foreach (var kv in element.Second)
                            Console.WriteLine("| {0}\t| {1}\t\t| {2}\t|", element.First, kv.Key, kv.Value);
                    }
                });

                computation.Activate();

                var frontier = 0;
                computation.OnFrontierChange += (c, f) =>
                {
                    var frontiers = f.NewFrontier.Select(k => new Epoch().InitializeFrom(k, 1).epoch).ToList();
                    frontiers.Add(0);
                    frontier = frontiers.Max();
                    Console.WriteLine("New frontier: {0}, Max: {1}", string.Join(", ", f.NewFrontier), frontier);

                };


                if (computation.Configuration.ProcessID == 0)
                {
                    // with our dataflow graph defined, we can start soliciting strings from the user.
                    Console.WriteLine("Enter string names for nudgers to nudge");
                    Console.WriteLine("Enter an int to query an epoch; enter '?' to query the current frontier's epoch");

                    // read lines of input and hand them to the input, until an empty line appears.
                    for (var line = Console.ReadLine(); line.Length > 0; line = Console.ReadLine())
                    {
                        var split = line.Trim().Split();


                        if (split.Length == 1)
                        {
                            if (split[0] == "?")
                            {
                                queries.OnNext(new Epoch(frontier));
                                instructions.OnNext();
                            }
                            else
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

