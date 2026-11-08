using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;

namespace RequestLimiter.Tests;

public class GatewayTests
{
    [Theory]
    [InlineData("custom")]
    [InlineData("standard")]
    public async Task Backend_concurrency_and_queue_are_bounded_and_timeouts_release_capacity(string mode)
    {
        await using var demo = await Scenario.StartAsync(mode);
        var first = demo.Send();
        var firstBackend = await demo.Arrival();
        var second = demo.Send();
        var secondBackend = await demo.Arrival();
        var third = demo.Send();
        await demo.WaitForMetric("queued", 1);
        var fourth = demo.Send();
        await demo.WaitForMetric("queued", 2);

        await ExpectProblem(demo.Send(), HttpStatusCode.ServiceUnavailable, "Queue is full");
        Assert.False(demo.HasArrival);
        demo.Clock.Advance(TimeSpan.FromMilliseconds(200));
        await ExpectProblem(third, HttpStatusCode.ServiceUnavailable, "Queue wait timed out");
        await ExpectProblem(fourth, HttpStatusCode.ServiceUnavailable, "Queue wait timed out");
        Assert.False(demo.HasArrival);
        Assert.Equal(2, await demo.Metric("inFlight"));

        firstBackend.Release.TrySetResult();
        secondBackend.Release.TrySetResult();
        using var firstResponse = await first;
        using var secondResponse = await second;
        Assert.Equal(HttpStatusCode.OK, firstResponse.StatusCode);
        Assert.Equal(HttpStatusCode.OK, secondResponse.StatusCode);

        var slow = demo.Send();
        var slowBackend = await demo.Arrival();
        demo.Clock.Advance(TimeSpan.FromMilliseconds(500));
        await ExpectProblem(slow, HttpStatusCode.GatewayTimeout, "Backend timed out");
        await slowBackend.Disconnected.Task.WaitAsync(Scenario.SafetyTimeout);
        Assert.Equal(0, await demo.Metric("inFlight"));
    }

    [Theory]
    [InlineData("custom")]
    [InlineData("standard")]
    public async Task Disconnect_reclaims_waiting_and_running_capacity(string mode)
    {
        await using var demo = await Scenario.StartAsync(mode);
        using var runningCancel = new CancellationTokenSource();
        using var queuedCancel = new CancellationTokenSource();
        var running = demo.Send(cancellationToken: runningCancel.Token);
        var runningBackend = await demo.Arrival();
        var held = demo.Send();
        var heldBackend = await demo.Arrival();
        var abandoned = demo.Send(cancellationToken: queuedCancel.Token);
        await demo.WaitForMetric("queued", 1);
        queuedCancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);

        var next = demo.Send();
        await demo.WaitForMetric("queued", 2);
        runningCancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        await runningBackend.Disconnected.Task.WaitAsync(Scenario.SafetyTimeout);
        var nextBackend = await demo.Arrival();
        nextBackend.Release.TrySetResult();
        heldBackend.Release.TrySetResult();

        using var nextResponse = await next;
        using var heldResponse = await held;
        Assert.Equal(HttpStatusCode.OK, nextResponse.StatusCode);
        Assert.Equal(HttpStatusCode.OK, heldResponse.StatusCode);
        Assert.Equal(3, await demo.Metric("accepted"));
        Assert.Equal(0, await demo.Metric("inFlight"));
    }

    [Theory]
    [InlineData("custom")]
    [InlineData("standard")]
    public async Task Rate_limit_is_partitioned_and_identity_cannot_be_overridden(string mode)
    {
        await using var demo = await Scenario.StartAsync(mode);
        using var bogus = new HttpRequestMessage(HttpMethod.Get, "/work");
        bogus.Headers.Add("X-Client-Id", "normal");
        await ExpectProblem(demo.Client.SendAsync(bogus), HttpStatusCode.Unauthorized, "A valid API key is required");

        // Exhaust the bucket with a concurrent burst while holding backend requests.
        // In standard mode, refill can run during the burst, so the exact rejection count varies.
        using var attack = new CancellationTokenSource();
        var burst = Enumerable.Range(0, 32)
            .Select(_ => demo.Send("demo-aggressive", cancellationToken: attack.Token)).ToArray();
        try
        {
            await demo.Arrival();
            await demo.Arrival();
            using var timeout = new CancellationTokenSource(Scenario.SafetyTimeout);

            var pending = burst.ToList();
            while (true)
            {
                var completed = await Task.WhenAny(pending).WaitAsync(timeout.Token);
                pending.Remove(completed);
                var response = await completed;
                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    Assert.Equal(TimeSpan.FromSeconds(1), response.Headers.RetryAfter?.Delta);
                    break;
                }
            }

            Assert.True(await demo.Metric("rateRejected", "aggressive") > 0);
            await Complete(demo.Send("demo-aggressive", "/bulk"));
            await Complete(demo.Send());
            Assert.Equal(1, await demo.Metric("accepted"));
        }
        finally
        {
            attack.Cancel();
            foreach (var request in burst)
            {
                try
                {
                    (await request).Dispose();
                }
                catch (OperationCanceledException error)
                {
                    Assert.Equal(attack.Token, error.CancellationToken);
                }
            }
        }

        async Task Complete(Task<HttpResponseMessage> response)
        {
            var arrival = await demo.Arrival();
            Assert.False(arrival.SensitiveHeadersForwarded);
            arrival.Release.TrySetResult();
            using var completed = await response;
            Assert.Equal(HttpStatusCode.OK, completed.StatusCode);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Oversized_body_never_reaches_backend(bool chunked)
    {
        await using var demo = await Scenario.StartAsync();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/work")
        {
            Content = new ByteArrayContent(new byte[Proxy.MaxBodyBytes + 1])
        };
        request.Headers.Add("X-Api-Key", "demo-normal");
        if (chunked)
            request.Headers.TransferEncodingChunked = true;

        await ExpectProblem(demo.Client.SendAsync(request), HttpStatusCode.RequestEntityTooLarge, "Request body exceeds 64 KiB");
        Assert.False(demo.HasArrival);
        Assert.Equal(0, await demo.Metric("inFlight"));
    }

    [Fact]
    public async Task Backend_connection_failure_releases_capacity()
    {
        await using var demo = await Scenario.StartAsync();
        await demo.StopBackend();
        await ExpectProblem(demo.Send(), HttpStatusCode.BadGateway, "Backend connection failed");
        Assert.Equal(0, await demo.Metric("inFlight"));
    }

    [Fact]
    public async Task Backend_body_deadline_releases_capacity()
    {
        var headersSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var demo = await Scenario.StartAsync(startResponse: async context =>
        {
            context.Response.ContentLength = 1;
            await context.Response.Body.FlushAsync(context.RequestAborted);
            headersSent.SetResult();
        });

        var response = demo.Send(route: "/work?delayMs=1");
        var upstream = await demo.Arrival();
        await headersSent.Task.WaitAsync(Scenario.SafetyTimeout);
        demo.Clock.Advance(TimeSpan.FromMilliseconds(500));
        await ExpectProblem(response, HttpStatusCode.GatewayTimeout, "Backend timed out");
        await upstream.Disconnected.Task.WaitAsync(Scenario.SafetyTimeout);
        await AssertCanForward(demo);
    }

    [Fact]
    public async Task Oversized_backend_body_releases_capacity()
    {
        await using var demo = await Scenario.StartAsync(startResponse: async context =>
        {
            await context.Response.Body.WriteAsync(new byte[Proxy.MaxBodyBytes + 1], context.RequestAborted);
        });

        var response = demo.Send(route: "/work?delayMs=1");
        var upstream = await demo.Arrival();
        upstream.Release.TrySetResult();
        await ExpectProblem(response, HttpStatusCode.BadGateway, "Backend response exceeds 64 KiB");
        await AssertCanForward(demo);
    }

    [Fact]
    public async Task Slow_upload_times_out_without_contacting_backend()
    {
        await using var demo = await Scenario.StartAsync();
        using var timeout = new CancellationTokenSource(Scenario.SafetyTimeout);
        using var socket = new TcpClient();
        await socket.ConnectAsync(IPAddress.Loopback, demo.Client.BaseAddress!.Port, timeout.Token);
        await using var stream = socket.GetStream();
        await stream.WriteAsync(Encoding.ASCII.GetBytes(
            "POST /work HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: demo-normal\r\n" +
            "Content-Length: 10\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"), timeout.Token);
        using var reader = new StreamReader(stream);
        Assert.Equal("HTTP/1.1 100 Continue", await reader.ReadLineAsync(timeout.Token));
        Assert.Equal("", await reader.ReadLineAsync(timeout.Token));
        await stream.WriteAsync("a"u8.ToArray(), timeout.Token);

        demo.Clock.Advance(TimeSpan.FromSeconds(2));
        Assert.StartsWith("HTTP/1.1 408 ", await reader.ReadLineAsync(timeout.Token));
        Assert.False(demo.HasArrival);
        await AssertCanForward(demo);
    }

    private static async Task AssertCanForward(Scenario demo)
    {
        Assert.Equal(0, await demo.Metric("inFlight"));

        // Both permits must be reusable; a successful request alone could hide a leaked permit.
        var requests = new[] { demo.Send(), demo.Send() };
        var first = await demo.Arrival();
        var second = await demo.Arrival();
        first.Release.TrySetResult();
        second.Release.TrySetResult();
        foreach (var request in requests)
        {
            using var response = await request;
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        Assert.Equal(0, await demo.Metric("inFlight"));
    }

    private static async Task ExpectProblem(Task<HttpResponseMessage> responseTask, HttpStatusCode status, string title)
    {
        using var response = await responseTask;
        Assert.Equal(status, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(title, problem.GetProperty("title").GetString());
    }

    private sealed class BackendCall(bool sensitiveHeadersForwarded)
    {
        public bool SensitiveHeadersForwarded { get; } = sensitiveHeadersForwarded;
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Disconnected { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class Scenario(WebApplication gateway, WebApplication backend, Channel<BackendCall> calls,
        FakeTimeProvider clock) : IAsyncDisposable
    {
        public static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(5);
        public FakeTimeProvider Clock => clock;
        public HttpClient Client { get; } = new() { BaseAddress = new Uri(gateway.Urls.Single()), Timeout = SafetyTimeout };
        public bool HasArrival => calls.Reader.TryPeek(out _);

        public static async Task<Scenario> StartAsync(string mode = "custom", Func<HttpContext, Task>? startResponse = null)
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Logging.ClearProviders();
            var backend = builder.Build();
            var calls = Channel.CreateUnbounded<BackendCall>();
            backend.MapMethods("/{route}", ["GET", "POST"], async (HttpContext context) =>
            {
                var call = new BackendCall(context.Request.Headers.ContainsKey("X-Api-Key") ||
                    context.Request.Headers.ContainsKey("Authorization") || context.Request.Headers.ContainsKey("Cookie"));
                await calls.Writer.WriteAsync(call, context.RequestAborted);
                try
                {
                    if (startResponse is not null && context.Request.Query["delayMs"] == "1")
                    {
                        await startResponse(context);
                        await call.Release.Task.WaitAsync(context.RequestAborted);
                        return Results.Empty;
                    }

                    await call.Release.Task.WaitAsync(context.RequestAborted);
                    return Results.Ok();
                }
                catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
                {
                    call.Disconnected.TrySetResult();
                    throw;
                }
            });
            await backend.StartAsync();

            var clock = new FakeTimeProvider();
            var gateway = Gateway.Build(
                ["--urls=http://127.0.0.1:0", "--Gate:Mode=" + mode, "--Gate:Backend=" + backend.Urls.Single(),
                    "--Logging:LogLevel:Default=Warning"], clock);
            await gateway.StartAsync();
            return new Scenario(gateway, backend, calls, clock);
        }

        public Task<HttpResponseMessage> Send(string key = "demo-normal", string route = "/work",
            CancellationToken cancellationToken = default) => SendCoreAsync(key, route, cancellationToken);

        private async Task<HttpResponseMessage> SendCoreAsync(string key, string route, CancellationToken cancellationToken)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, route);
            request.Headers.Add("X-Api-Key", key);
            request.Headers.Add("X-Client-Id", "normal");
            request.Headers.Add("Authorization", "Bearer demo-only");
            request.Headers.Add("Cookie", "demo=session");
            return await Client.SendAsync(request, cancellationToken);
        }

        public async Task<BackendCall> Arrival()
        {
            using var timeout = new CancellationTokenSource(SafetyTimeout);
            return await calls.Reader.ReadAsync(timeout.Token);
        }

        public async Task<long> Metric(string name, string client = "normal")
        {
            var rows = await Client.GetFromJsonAsync<JsonElement>("/metrics");
            return rows.EnumerateArray().Single(row =>
                row.GetProperty("client").GetString() == client && row.GetProperty("route").GetString() == "/work")
                .GetProperty(name).GetInt64();
        }

        public async Task WaitForMetric(string name, long value)
        {
            using var timeout = new CancellationTokenSource(SafetyTimeout);
            while (await Metric(name) != value)
                await Task.Delay(10, timeout.Token);
        }

        public Task StopBackend() => backend.StopAsync();

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            using var timeout = new CancellationTokenSource(SafetyTimeout);
            await gateway.StopAsync(timeout.Token);
            await backend.StopAsync(timeout.Token);
            await gateway.DisposeAsync();
            await backend.DisposeAsync();
        }
    }
}
