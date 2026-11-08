namespace RequestLimiter;

internal sealed class RequestSlots(int limit, int queueLimit)
{
    private readonly object sync = new();
    private readonly LinkedList<TaskCompletionSource> queue = new();
    private int occupied;

    public ValueTask<Lease?> AcquireAsync(CancellationToken cancellationToken, out bool queued)
    {
        queued = false;
        cancellationToken.ThrowIfCancellationRequested();

        lock (sync)
        {
            if (occupied < limit)
            {
                occupied++;
                return ValueTask.FromResult<Lease?>(new Lease(this));
            }

            if (queue.Count == queueLimit)
                return ValueTask.FromResult<Lease?>(null);

            queued = true;
            var waiter = queue.AddLast(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            return WaitAsync(waiter, cancellationToken);
        }
    }

    private async ValueTask<Lease?> WaitAsync(LinkedListNode<TaskCompletionSource> waiter, CancellationToken cancellationToken)
    {
        try
        {
            await waiter.Value.Task.WaitAsync(cancellationToken);
            return new Lease(this);
        }
        catch (OperationCanceledException)
        {
            lock (sync)
            {
                if (waiter.List is not null)
                    queue.Remove(waiter);
                else
                    Release(); // The handoff won the queue lock, but cancellation won WaitAsync.
            }

            throw;
        }
    }

    private void Release()
    {
        if (queue.First is { } next)
        {
            queue.RemoveFirst();
            next.Value.SetResult();
        }
        else
        {
            occupied--;
        }
    }

    internal sealed class Lease(RequestSlots owner) : IDisposable
    {
        private RequestSlots? slots = owner;

        public void Dispose()
        {
            var returned = Interlocked.Exchange(ref slots, null);
            if (returned is null)
                return;

            lock (returned.sync)
                returned.Release();
        }
    }
}
