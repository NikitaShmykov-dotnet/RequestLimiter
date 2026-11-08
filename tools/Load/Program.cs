using System.Diagnostics;
using System.Globalization;

if (args.Length is < 2 or > 4 || !Uri.TryCreate(args[0], UriKind.Absolute, out var gateway) ||
    gateway.Scheme != Uri.UriSchemeHttp || gateway.Host != "127.0.0.1" ||
    !int.TryParse(args[1], out var seconds) || seconds is < 1 or > 120 ||
    (args.Length >= 3 && args[2] is not ("/work" or "/bulk")))
{
    Console.Error.WriteLine("Usage: dotnet run --project tools/Load -c Release -- http://127.0.0.1:5070 12 [/work|/bulk] [samples.csv]");
    return 2;
}

CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
var route = args.Length >= 3 ? args[2] : "/work";
var (normalInterval, aggressiveInterval) = route == "/work" ? (125, 10) : (320, 25);
using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false })
{
    BaseAddress = gateway,
    Timeout = TimeSpan.FromSeconds(3)
};

using (var warmup = new HttpRequestMessage(HttpMethod.Get, route + "?delayMs=0"))
{
    warmup.Headers.Add("X-Api-Key", "demo-normal");
    using var response = await client.SendAsync(warmup);
    response.EnsureSuccessStatusCode();
}
await Task.Delay(TimeSpan.FromSeconds(1));

var duration = TimeSpan.FromSeconds(seconds);
var run = Stopwatch.StartNew();
var results = await Task.WhenAll(
    DriveAsync("normal", "demo-normal", TimeSpan.FromMilliseconds(normalInterval)),
    DriveAsync("aggressive", "demo-aggressive", TimeSpan.FromMilliseconds(aggressiveInterval)));
run.Stop();

Console.WriteLine($"route: {route}; intervals: {normalInterval}/{aggressiveInterval}ms; offered window: {seconds}s; elapsed with drain: {run.Elapsed.TotalSeconds:F2}s; backend delay: 80ms");
Console.WriteLine("client       sent    ok   rejected   refused%    ok/s   all p50/p95/p99 ms        ok p50/p95/p99 ms");
foreach (var (name, samples) in results)
{
    var successes = samples.Where(sample => sample.Status is >= 200 and < 300).ToArray();
    var rejected = samples.Length - successes.Length;
    Console.WriteLine($"{name,-11} {samples.Length,5} {successes.Length,5} {rejected,10} " +
        $"{100d * rejected / samples.Length,9:F1} {successes.Length / run.Elapsed.TotalSeconds,7:F1}   " +
        $"{Percentiles(samples),-24}  {Percentiles(successes)}");
    Console.WriteLine("  " + string.Join(", ", samples.GroupBy(sample => sample.Status).OrderBy(group => group.Key)
        .Select(group => $"{(group.Key == 0 ? "transport error" : group.Key.ToString())}={group.Count()}")));
}

if (args.Length == 4)
{
    await using var csv = File.CreateText(args[3]);
    await csv.WriteLineAsync("client,request,status,milliseconds");
    foreach (var (name, samples) in results)
    {
        for (var i = 0; i < samples.Length; i++)
            await csv.WriteLineAsync($"{name},{i + 1},{samples[i].Status},{samples[i].Milliseconds:F3}");
    }
}

return 0;

async Task<(string Name, Sample[] Samples)> DriveAsync(string name, string key, TimeSpan interval)
{
    using var timer = new PeriodicTimer(interval);
    var requests = new List<Task<Sample>>();
    while (run.Elapsed < duration)
    {
        requests.Add(SendAsync(key));
        await timer.WaitForNextTickAsync();
    }

    return (name, await Task.WhenAll(requests));
}

async Task<Sample> SendAsync(string key)
{
    using var request = new HttpRequestMessage(HttpMethod.Get, route + "?delayMs=80");
    request.Headers.Add("X-Api-Key", key);
    var started = Stopwatch.GetTimestamp();
    try
    {
        using var response = await client.SendAsync(request);
        return new Sample((int)response.StatusCode, Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    }
    catch (Exception error) when (error is HttpRequestException or OperationCanceledException)
    {
        return new Sample(0, Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    }
}

static string Percentiles(IEnumerable<Sample> samples)
{
    var values = samples.Select(sample => sample.Milliseconds).Order().ToArray();
    if (values.Length == 0)
        return "—";

    return string.Join("/", new[] { 0.50, 0.95, 0.99 }
        .Select(p => values[(int)Math.Ceiling(p * values.Length) - 1].ToString("F1")));
}

internal readonly record struct Sample(int Status, double Milliseconds);
