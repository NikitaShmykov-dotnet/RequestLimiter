namespace RequestLimiter;

internal enum GateMode { Custom, Standard, Off }

internal sealed record Quota(int Burst, int TokensPerSecond, int Concurrent, int Queue)
{
    public TimeSpan RefillFromEmpty => TimeSpan.FromSeconds((double)Burst / TokensPerSecond);
}

internal sealed record Client(string Name, Quota Work, Quota Bulk);

internal static class Policies
{
    private static readonly Dictionary<string, Client> Clients = new(StringComparer.Ordinal)
    {
        ["demo-normal"] = new("normal", new(12, 12, 2, 2), new(4, 4, 1, 1)),
        ["demo-aggressive"] = new("aggressive", new(8, 16, 2, 3), new(2, 4, 1, 1))
    };

    public static Client? Identify(IHeaderDictionary headers) =>
        headers.TryGetValue("X-Api-Key", out var values) && values.Count == 1 &&
        values[0] is { Length: <= 64 } key && Clients.TryGetValue(key, out var client)
            ? client
            : null;

    public static IEnumerable<Client> All => Clients.Values;
}
