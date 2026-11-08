using Microsoft.Extensions.Time.Testing;

namespace RequestLimiter.Tests;

public class LimiterTests
{
    [Fact]
    public void Burst_fractional_refill_and_capacity_use_monotonic_time()
    {
        var clock = new FakeTimeProvider();
        var bucket = new TokenBucket(new Quota(3, 4, 1, 1), clock);
        for (var i = 0; i < 3; i++)
            Assert.Equal(TimeSpan.Zero, bucket.Take());

        Assert.Equal(TimeSpan.FromMilliseconds(250), bucket.Take());
        clock.Advance(TimeSpan.FromMilliseconds(125));
        Assert.Equal(TimeSpan.FromMilliseconds(125), bucket.Take());
        clock.Advance(TimeSpan.FromMilliseconds(124) + TimeSpan.FromTicks(9999));
        Assert.Equal(TimeSpan.FromTicks(1), bucket.Take());
        clock.Advance(TimeSpan.FromTicks(1));
        Assert.Equal(TimeSpan.Zero, bucket.Take());
        Assert.Equal(TimeSpan.FromMilliseconds(250), bucket.Take());

        clock.Advance(TimeSpan.FromDays(30));
        for (var i = 0; i < 3; i++)
            Assert.Equal(TimeSpan.Zero, bucket.Take());

        Assert.Equal(TimeSpan.FromMilliseconds(250), bucket.Take());
    }

    [Fact]
    public async Task Standard_bucket_refills_on_its_own_clock_before_idle_eviction()
    {
        var clock = new FakeTimeProvider();
        var quota = new Quota(2, 1, 1, 1);
        var client = new Client("test", quota, quota);
        var states = new ClientStates(GateMode.Standard, clock);
        using var active = states.Rent(client, "/work");
        var original = active.State;
        Assert.Equal(TimeSpan.Zero, original.TakeToken());
        Assert.Equal(TimeSpan.Zero, original.TakeToken());
        Assert.Equal(TimeSpan.FromSeconds(1), original.TakeToken());

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(6));
        while (original.TakeToken() > TimeSpan.Zero)
            await Task.Delay(25, deadline.Token);

        clock.Advance(TimeSpan.FromMinutes(1));
        using (var concurrent = states.Rent(client, "/work"))
            Assert.Same(original, concurrent.State);
        active.Dispose();

        clock.Advance(TimeSpan.FromMinutes(1));
        using (var depleted = states.Rent(client, "/work"))
            Assert.Same(original, depleted.State);

        // Only the eviction age uses FakeTimeProvider. The framework bucket refills via Stopwatch.
        while (true)
        {
            clock.Advance(TimeSpan.FromMinutes(1));
            using var rental = states.Rent(client, "/work");
            if (!ReferenceEquals(original, rental.State))
            {
                Assert.Equal(TimeSpan.Zero, rental.State.TakeToken());
                Assert.Equal(TimeSpan.Zero, rental.State.TakeToken());
                break;
            }

            await Task.Delay(25, deadline.Token);
        }
    }

    [Fact]
    public async Task Cancelled_waiters_free_queue_space_and_handoff_remains_fifo()
    {
        var slots = new RequestSlots(1, 2);
        using var active = await slots.AcquireAsync(default, out _);
        Assert.NotNull(active);
        using var cancel = new CancellationTokenSource();
        var abandoned = slots.AcquireAsync(cancel.Token, out var queued);
        Assert.True(queued);
        var first = slots.AcquireAsync(default, out _);
        Assert.Null(await slots.AcquireAsync(default, out queued));
        Assert.False(queued);

        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned.AsTask());
        var second = slots.AcquireAsync(default, out queued);
        Assert.True(queued);
        active.Dispose();

        using var firstLease = await first.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(firstLease);
        Assert.False(second.IsCompleted);
        firstLease.Dispose();
        using var secondLease = await second.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(secondLease);
    }

    [Fact]
    public async Task Cancellation_racing_with_release_neither_leaks_nor_duplicates_a_slot()
    {
        var slots = new RequestSlots(1, 1);
        for (var i = 0; i < 100; i++)
        {
            using var active = await slots.AcquireAsync(default, out var queued);
            Assert.False(queued);
            Assert.NotNull(active);
            using var cancel = new CancellationTokenSource();
            var waiting = slots.AcquireAsync(cancel.Token, out queued);
            Assert.True(queued);
            await Task.WhenAll(Task.Run(active.Dispose), Task.Run(cancel.Cancel));
            try
            {
                using var handedOff = await waiting.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                Assert.NotNull(handedOff);
                var probe = slots.AcquireAsync(default, out queued);
                Assert.True(queued);
                handedOff.Dispose();
                using var probeLease = await probe.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                Assert.NotNull(probeLease);
            }
            catch (OperationCanceledException error) when (cancel.IsCancellationRequested)
            {
                Assert.Equal(cancel.Token, error.CancellationToken);
            }
        }

        using var last = await slots.AcquireAsync(default, out var lastQueued);
        Assert.NotNull(last);
        Assert.False(lastQueued);
    }

    [Fact]
    public void Idle_cleanup_and_parallel_rents_do_not_split_a_bucket()
    {
        var clock = new FakeTimeProvider();
        var states = new ClientStates(GateMode.Custom, clock);
        var normal = Policies.All.Single(client => client.Name == "normal");
        using var anchor = states.Rent(normal, "/work");
        var original = anchor.State;
        clock.Advance(TimeSpan.FromMinutes(1));
        Assert.Same(original, CheckConcurrentBurst());

        anchor.Dispose();
        using (var recent = states.Rent(normal, "/work"))
        {
            Assert.Same(original, recent.State);
            Assert.True(recent.State.TakeToken() > TimeSpan.Zero);
        }

        clock.Advance(TimeSpan.FromMinutes(1));
        Assert.NotSame(original, CheckConcurrentBurst());

        ClientStates.State CheckConcurrentBurst()
        {
            var seen = new ClientStates.State[128];
            var accepted = 0;
            Parallel.For(0, seen.Length, i =>
            {
                using var rental = states.Rent(normal, "/work");
                seen[i] = rental.State;
                if (rental.State.TakeToken() == TimeSpan.Zero)
                    Interlocked.Increment(ref accepted);
            });

            Assert.All(seen, state => Assert.Same(seen[0], state));
            Assert.Equal(normal.Work.Burst, accepted);
            return seen[0];
        }
    }
}
