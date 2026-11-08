using System.Threading.RateLimiting;

namespace RequestLimiter;

internal sealed class ClientStates(GateMode mode, TimeProvider clock)
{
    private readonly TimeProvider clock = clock;
    private readonly object sync = new();
    private readonly Dictionary<(string Client, string Route), State> states = new();

    public Rental Rent(Client client, string route)
    {
        lock (sync)
        {
            var now = clock.GetTimestamp();
            // Only authenticated, fixed client/route pairs reach this map.
            foreach (var pair in states.ToArray())
            {
                if (pair.Value.Users == 0 && pair.Value.CanForget(now, clock))
                {
                    states.Remove(pair.Key);
                    pair.Value.Dispose();
                }
            }

            var key = (client.Name, route);
            if (!states.TryGetValue(key, out var state))
            {
                state = new State(route == "/work" ? client.Work : client.Bulk, mode, clock);
                states.Add(key, state);
            }

            state.Users++;
            return new Rental(this, state);
        }
    }

    internal sealed class Rental(ClientStates owner, State state) : IDisposable
    {
        private State? rented = state;

        public State State => rented ?? throw new ObjectDisposedException(nameof(Rental));

        public void Dispose()
        {
            var returned = Interlocked.Exchange(ref rented, null);
            if (returned is null)
                return;

            lock (owner.sync)
            {
                if (--returned.Users == 0)
                    returned.IdleSince = owner.clock.GetTimestamp();
            }
        }
    }

    internal sealed class State : IDisposable
    {
        private readonly Quota quota;
        private readonly TokenBucket? custom;
        private readonly TokenBucketRateLimiter? standard;

        public State(Quota quota, GateMode mode, TimeProvider clock)
        {
            this.quota = quota;
            if (mode == GateMode.Custom)
                custom = new TokenBucket(quota, clock);
            else
                standard = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
                {
                    TokenLimit = quota.Burst,
                    TokensPerPeriod = 1,
                    ReplenishmentPeriod = TimeSpan.FromSeconds(1d / quota.TokensPerSecond),
                    AutoReplenishment = false,
                    QueueLimit = 0,
                    QueueProcessingOrder = QueueProcessingOrder.OldestFirst
                });

            Slots = new RequestSlots(quota.Concurrent, quota.Queue);
        }

        public int Users { get; set; }
        public long IdleSince { get; set; }
        public RequestSlots Slots { get; }

        public TimeSpan TakeToken()
        {
            if (custom is not null)
                return custom.Take();

            standard!.TryReplenish();
            using var lease = standard.AttemptAcquire();
            if (lease.IsAcquired)
                return TimeSpan.Zero;

            return lease.TryGetMetadata(MetadataName.RetryAfter, out var delay) && delay > TimeSpan.Zero
                ? delay
                : TimeSpan.FromSeconds(1);
        }

        public bool CanForget(long now, TimeProvider clock)
        {
            if (clock.GetElapsedTime(IdleSince, now) < TimeSpan.FromSeconds(30) + quota.RefillFromEmpty)
                return false;

            if (standard is null)
                return true;

            standard.TryReplenish();
            return standard.GetStatistics()?.CurrentAvailablePermits == quota.Burst;
        }

        public void Dispose() => standard?.Dispose();
    }
}
