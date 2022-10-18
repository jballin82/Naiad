using System;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;

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


    internal static class AppState

    {
        public static List<HyperVertex> _hypers = new List<HyperVertex>();
    }

    internal class HyperVertex : UnaryVertex<NudgerUpdate, Epoch, Epoch>
    {
        private SortedDictionary<Epoch, IReadOnlyDictionary<string, State>> Indices;
        internal Epoch LatestEpoch { get; private set; }

        private Dictionary<Epoch, List<NudgerUpdate>> InFlightUpdates;

        public bool HasStateAt(Epoch epoch)
        {
            if (this.Indices.Count == 0)
            {
                //Console.WriteLine("No indices yet!");
                return false;
            }
            return this.Indices.First().Key.LessThan(epoch);
        }

        public IReadOnlyDictionary<string, State> EntriesAt(Epoch epoch)
        {
            //Console.WriteLine("HyperVertex {0} state: ", this.VertexId);
            //foreach (var item in Indices)
            //{
            //    Console.WriteLine("{0}: {1}: {2}", this.VertexId, item.Key,
            //        string.Join(",", item.Value.Select(kv => "[" + kv.Key + ": " + kv.Value + "]")));
            //}
            if (!HasStateAt(epoch))
                return new Dictionary<string, State>();

            var last = Indices.ToList().FindLast(pair => pair.Key.LessThan(epoch));
            return last.Value;
        }

        public HyperVertex(int index, Stage<Epoch> stage) : base(index, stage)
        {
            Indices = new SortedDictionary<Epoch, IReadOnlyDictionary<string, State>>();
            InFlightUpdates = new Dictionary<Epoch, List<NudgerUpdate>>();
            AppState._hypers.Add(this);
        }

        public override void OnReceive(Message<NudgerUpdate, Epoch> message)
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
                Console.WriteLine("HyperVertex {0}: committing changes:", this.VertexId);

                foreach (var change in changes)
                {
                    Console.WriteLine("\t+HyperVertex {0}: {1}", this.VertexId, change);
                    newPairs[change.NudgerName] = change.NewState;
                }
                // add to soldified state
                Indices[time] = newPairs;

                InFlightUpdates.Remove(time);
            }

            var output = this.Output.GetBufferForTime(time);
            output.Send(time);
            LatestEpoch = time;
            Console.WriteLine("HyperVertex {0}: Finished OnNotify for Epoch: {1}", this.VertexId, time);

        }
    }


    public class Experiment : Example
    {
        public string Usage => "No args required";

        public string Help => "Does some stuff";

        private void PrintQuery(int i)
        {

            Console.WriteLine("State for Epoch: {0}", i);
            var r = AppState._hypers.SelectMany(h => h.EntriesAt(new Epoch(i))).ToArray();

            Console.WriteLine("Name\t| State");

            foreach (var elem in r)
                Console.WriteLine("{0}\t| {1}", elem.Key, elem.Value);
            Console.WriteLine("({0} records)", r.Length);
        }

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // let our "processes" be known by strings
                // and our queries be made by timestamps of ints
                var instructions = new BatchedDataSource<string>();


                /*
                 * instructions >
                 *  instrStreamInput > s1 (Nudgers)----- s2 (Hypers) ----> completed Epoch
                 *                                        
                 */

                var instrStreamInput = computation.NewInput(instructions);

                // Apparently stable partition function:
                // input: nudger name's hash code
                // output: *nudgerupdate*'s colour
                var s1 = Foundry.NewUnaryStage(instrStreamInput,
                    (i, s) => new NudgerVertex(i, s), x => x.GetHashCode(), x => x.NudgerColour.GetHashCode(), "Nudgers");

                // partition all by 0 => everything comes together
                // (prior BinaryVertex code) In the KeyValueLookup example, the two partition functions are the same: Updating the value associated
                // with a key and querying the value associated with a key get sent to the same vertex
                // but here, Naiad is going to direct the query to just one vertex, which we don't want (if we have multiple vertices)
                // Reasoning that setting the partition function to zero on *both* streams of input implies a singleton

                // Partition input based on NudgerColour, output has no partitioning
                var s2 = Foundry.NewUnaryStage(s1,
                    (i, s) => new HyperVertex(i, s), x => x.NudgerColour.GetHashCode(), y => 0, "Hypers");

                // the last unary stage emits the latest epoch that has worked its way through; this is the trailing frontier
                var trailingFrontier = -1;
                s2.Subscribe(completions =>
                        {
                            trailingFrontier = completions.Select(i => i.epoch).Max();
                            Console.WriteLine("*** Epoch {0} complete", trailingFrontier);
                        }
                    );

                computation.Activate();

                // AFAICT the frontier is the newest (highest) epoch of all pointstamps in the system
                // (It isn't the lowest complete epoch - that's the trailing frontier above which it output by the computation)
                // although I'm surprised it's not immediately available - it's not the shadowFrontier defined thus:
                var shadowFrontier = 0;
                var leadingFrontier = 0;
                computation.OnFrontierChange += (c, f) =>
                {

                    var frontiers = f.NewFrontier.Select(k => new Epoch().InitializeFrom(k, 1).epoch).ToArray();
                    if (frontiers.Length > 0)

                    {
                        shadowFrontier = frontiers.Min();
                        leadingFrontier = frontiers.Max();
                    }
                    Console.WriteLine("Frontier update: {0}, Min: {1}, Max: {2}", string.Join(", ", f.NewFrontier), shadowFrontier, leadingFrontier);

                };


                if (computation.Configuration.ProcessID == 0)
                {
                    // with our dataflow graph defined, we can start soliciting strings from the user.
                    Console.WriteLine("Enter names for nudgers to nudge");
                    Console.WriteLine("Enter '?' to query the current trailing frontier's epoch; ?n to query the n'th historical epoch");

                    // read lines of input and hand them to the input, until an empty line appears.

                    for (var line = Console.ReadLine(); line.Length > 0; line = Console.ReadLine())
                    {


                        Console.WriteLine("*** Trailing frontier is: {0}", trailingFrontier);

                        var split = line.Trim().Split();

                        if (split[0].StartsWith("?"))
                        {
                            if (split[0] == "?")
                                PrintQuery(trailingFrontier);
                            else
                            {
                                var rest = split[0].Substring(1);
                                int i = 0;
                                if (int.TryParse(rest, out i))
                                    PrintQuery(i);
                                else
                                    Console.WriteLine("Failed to parse input; try again");
                            }
                        }
                        else
                        {
                            instructions.OnNext(split);
                        }
                    }
                }

                instructions.OnCompleted();   // signal the end of the input.

                computation.Join();           // waits until the graph has finished executing.

            }
        }
    }
}

