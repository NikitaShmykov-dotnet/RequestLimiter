using RequestLimiter;

var app = Gateway.Build(args);
await app.RunAsync();

namespace RequestLimiter
{
    internal static class Gateway
    {
        public static WebApplication Build(string[] args, TimeProvider? clock = null)
        {
            var builder = WebApplication.CreateBuilder(args);
            builder.WebHost.UseUrls(builder.Configuration["urls"] ?? "http://127.0.0.1:5070");
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Limits.MaxRequestBodySize = Proxy.MaxBodyBytes;
                options.Limits.MaxRequestHeadersTotalSize = 16 * 1024;
            });

            var mode = builder.Configuration["Gate:Mode"]?.ToLowerInvariant() switch
            {
                null or "custom" => GateMode.Custom,
                "standard" => GateMode.Standard,
                "off" => GateMode.Off,
                _ => throw new InvalidOperationException("Gate:Mode must be custom, standard or off.")
            };
            var address = builder.Configuration["Gate:Backend"] ?? "http://127.0.0.1:5071/";
            if (!Uri.TryCreate(address, UriKind.Absolute, out var backend) ||
                backend.Scheme != Uri.UriSchemeHttp || backend.Host != "127.0.0.1" ||
                backend.PathAndQuery != "/" || backend.Fragment.Length != 0 || backend.UserInfo.Length != 0)
            {
                throw new InvalidOperationException("Gate:Backend must be a loopback HTTP origin at 127.0.0.1.");
            }

            clock ??= TimeProvider.System;
            var states = new ClientStates(mode, clock);
            var metrics = new GateMetrics();
            builder.Services.AddProblemDetails();
            builder.Services.AddOpenApi();
            builder.Services.AddHealthChecks();
            builder.Services.AddHttpClient<Proxy>(client =>
            {
                client.BaseAddress = backend;
                client.Timeout = Timeout.InfiniteTimeSpan;
            }).RedactLoggedHeaders(_ => true)
                .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
                {
                    AllowAutoRedirect = false,
                    UseCookies = false,
                    UseProxy = false
                });

            var app = builder.Build();
            app.UseExceptionHandler();
            app.MapOpenApi();
            app.MapHealthChecks("/health");
            app.MapGet("/metrics", () => Results.Ok(metrics.Snapshot()));

            foreach (var route in new[] { "/work", "/bulk" })
            {
                app.MapMethods(route, ["GET", "POST"], (HttpContext context, Proxy proxy) =>
                    HandleAsync(context, route, proxy, mode, states, metrics, clock))
                    .WithDescription("Requires X-Api-Key. The optional delayMs query parameter accepts 0–2000.")
                    .Produces(StatusCodes.Status200OK, contentType: "application/json")
                    .ProducesProblem(StatusCodes.Status400BadRequest)
                    .ProducesProblem(StatusCodes.Status401Unauthorized)
                    .ProducesProblem(StatusCodes.Status408RequestTimeout)
                    .ProducesProblem(StatusCodes.Status413PayloadTooLarge)
                    .ProducesProblem(StatusCodes.Status429TooManyRequests)
                    .ProducesProblem(StatusCodes.Status502BadGateway)
                    .ProducesProblem(StatusCodes.Status503ServiceUnavailable)
                    .ProducesProblem(StatusCodes.Status504GatewayTimeout);
            }

            app.Logger.LogInformation("RequestLimiter started in {Mode} mode", mode);
            return app;
        }

        private static async Task<IResult> HandleAsync(
            HttpContext context, string route, Proxy proxy, GateMode mode,
            ClientStates states, GateMetrics metrics, TimeProvider clock)
        {
            var client = Policies.Identify(context.Request.Headers);
            if (client is null)
                return Results.Problem(statusCode: 401, title: "A valid API key is required");

            var delay = 80;
            if (context.Request.Query.TryGetValue("delayMs", out var values) &&
                (values.Count != 1 || !int.TryParse(values[0], out delay) || delay is < 0 or > 2000))
                return Results.Problem(statusCode: 400, title: "delayMs must be between 0 and 2000");

            if (context.Request.ContentLength > Proxy.MaxBodyBytes)
                return Results.Problem(statusCode: 413, title: "Request body exceeds 64 KiB");

            var counters = metrics.For(client, route);
            using var rental = mode == GateMode.Off ? null : states.Rent(client, route);
            var disconnected = context.RequestAborted;

            if (rental is null)
                return await ForwardAsync();

            var retryAfter = rental.State.TakeToken();
            if (retryAfter > TimeSpan.Zero)
            {
                Interlocked.Increment(ref counters.RateRejected);
                context.Response.Headers.RetryAfter = Math.Max(1, (int)Math.Ceiling(retryAfter.TotalSeconds))
                    .ToString(System.Globalization.CultureInfo.InvariantCulture);
                return Results.Problem(statusCode: 429, title: "Rate limit exceeded");
            }

            RequestSlots.Lease? slot;
            using (var queueDeadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(200), clock))
            using (var queueCancellation = CancellationTokenSource.CreateLinkedTokenSource(disconnected, queueDeadline.Token))
            {
                try
                {
                    var waiting = rental.State.Slots.AcquireAsync(queueCancellation.Token, out var queued);
                    if (queued)
                        Interlocked.Increment(ref counters.Queued);

                    slot = await waiting;
                }
                catch (OperationCanceledException) when (queueDeadline.IsCancellationRequested && !disconnected.IsCancellationRequested)
                {
                    Interlocked.Increment(ref counters.QueueTimeout);
                    return Results.Problem(statusCode: 503, title: "Queue wait timed out");
                }
            }

            using (slot)
            {
                if (slot is null)
                {
                    Interlocked.Increment(ref counters.QueueFull);
                    return Results.Problem(statusCode: 503, title: "Queue is full");
                }

                return await ForwardAsync();
            }

            async Task<IResult> ForwardAsync()
            {
                disconnected.ThrowIfCancellationRequested();
                Interlocked.Increment(ref counters.Accepted);
                Interlocked.Increment(ref counters.InFlight);
                try
                {
                    return await proxy.ForwardAsync(context.Request, route, delay, clock, disconnected);
                }
                finally
                {
                    Interlocked.Decrement(ref counters.InFlight);
                }
            }
        }
    }
}
