namespace RequestLimiter;

internal sealed class GateMetrics
{
    private readonly Dictionary<(string Client, string Route), Counters> counters =
        (from client in Policies.All
         from route in new[] { "/work", "/bulk" }
         select (client.Name, route)).ToDictionary(key => key, _ => new Counters());

    public Counters For(Client client, string route) => counters[(client.Name, route)];

    public object Snapshot() => counters.Select(pair =>
    {
        var row = pair.Value;
        var rateRejected = Interlocked.Read(ref row.RateRejected);
        var queueFull = Interlocked.Read(ref row.QueueFull);
        var queueTimeout = Interlocked.Read(ref row.QueueTimeout);
        return new
        {
            client = pair.Key.Client,
            route = pair.Key.Route,
            accepted = Interlocked.Read(ref row.Accepted),
            rateRejected,
            queueFull,
            queueTimeout,
            rejected = rateRejected + queueFull + queueTimeout,
            queued = Interlocked.Read(ref row.Queued),
            inFlight = Interlocked.Read(ref row.InFlight)
        };
    }).ToArray();

    internal sealed class Counters
    {
        public long Accepted;
        public long RateRejected;
        public long QueueFull;
        public long QueueTimeout;
        public long Queued;
        public long InFlight;
    }
}
