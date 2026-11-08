namespace RequestLimiter;

internal sealed class TokenBucket(Quota quota, TimeProvider clock)
{
    private readonly object sync = new();
    private long updatedAt = clock.GetTimestamp();
    private long available = quota.Burst * TimeSpan.TicksPerSecond;

    // A zero delay means a token was consumed.
    public TimeSpan Take()
    {
        lock (sync)
        {
            var now = clock.GetTimestamp();
            // One token is represented by TicksPerSecond units so fractional refill survives each call.
            var capacity = quota.Burst * TimeSpan.TicksPerSecond;
            var elapsed = clock.GetElapsedTime(updatedAt, now).Ticks;
            available = elapsed >= TicksToRefill(capacity)
                ? capacity
                : Math.Min(capacity, available + elapsed * quota.TokensPerSecond);
            updatedAt = now;
            if (available >= TimeSpan.TicksPerSecond)
            {
                available -= TimeSpan.TicksPerSecond;
                return TimeSpan.Zero;
            }

            return TimeSpan.FromTicks(TicksToRefill(TimeSpan.TicksPerSecond - available));
        }
    }

    private long TicksToRefill(long units) => (units + quota.TokensPerSecond - 1) / quota.TokensPerSecond;
}
