using System.Buffers;
using System.Globalization;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Http.Features;

namespace RequestLimiter;

internal sealed class Proxy(HttpClient backend)
{
    public const int MaxBodyBytes = 64 * 1024;

    public async Task<IResult> ForwardAsync(
        HttpRequest request, string route, int delay, TimeProvider clock, CancellationToken disconnected)
    {
        using var outgoing = new HttpRequestMessage(new HttpMethod(request.Method),
            route + "?delayMs=" + delay.ToString(CultureInfo.InvariantCulture));

        if (request.HttpContext.Features.Get<IHttpRequestBodyDetectionFeature>()?.CanHaveBody == true)
        {
            using var uploadDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(2), clock);
            using var upload = CancellationTokenSource.CreateLinkedTokenSource(disconnected, uploadDeadline.Token);
            try
            {
                var bytes = await ReadBoundedAsync(request.Body, upload.Token);
                if (bytes is null)
                    return Results.Problem(statusCode: 413, title: "Request body exceeds 64 KiB");

                outgoing.Content = new ByteArrayContent(bytes);
                if (MediaTypeHeaderValue.TryParse(request.ContentType, out var mediaType))
                    outgoing.Content.Headers.ContentType = mediaType;
            }
            catch (BadHttpRequestException error)
            {
                return Results.Problem(statusCode: error.StatusCode,
                    title: error.StatusCode == 413 ? "Request body exceeds 64 KiB" : "Invalid request body");
            }
            catch (OperationCanceledException) when (uploadDeadline.IsCancellationRequested && !disconnected.IsCancellationRequested)
            {
                return Results.Problem(statusCode: 408, title: "Request body timed out");
            }
        }

        using var upstreamDeadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(500), clock);
        using var upstream = CancellationTokenSource.CreateLinkedTokenSource(disconnected, upstreamDeadline.Token);
        try
        {
            using var response = await backend.SendAsync(outgoing, HttpCompletionOption.ResponseHeadersRead, upstream.Token);
            await using var stream = await response.Content.ReadAsStreamAsync(upstream.Token);
            var bytes = await ReadBoundedAsync(stream, upstream.Token);
            if (bytes is null)
                return Results.Problem(statusCode: 502, title: "Backend response exceeds 64 KiB");

            return new BackendResponse((int)response.StatusCode, response.Content.Headers.ContentType?.ToString(), bytes);
        }
        catch (OperationCanceledException) when (upstreamDeadline.IsCancellationRequested && !disconnected.IsCancellationRequested)
        {
            return Results.Problem(statusCode: 504, title: "Backend timed out");
        }
        catch (Exception error) when (error is HttpRequestException or IOException)
        {
            disconnected.ThrowIfCancellationRequested();
            return Results.Problem(statusCode: 502, title: "Backend connection failed");
        }
    }

    private static async Task<byte[]?> ReadBoundedAsync(Stream stream, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(8192);
        try
        {
            using var body = new MemoryStream();
            while (true)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(0, 8192), cancellationToken);
                if (read == 0)
                    return body.ToArray();

                if (body.Length + read > MaxBodyBytes)
                    return null;

                body.Write(buffer, 0, read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private sealed record BackendResponse(int Status, string? ContentType, byte[] Body) : IResult
    {
        public Task ExecuteAsync(HttpContext context)
        {
            context.Response.StatusCode = Status;
            context.Response.ContentType = ContentType;
            context.Response.ContentLength = Body.Length;
            return context.Response.Body.WriteAsync(Body, context.RequestAborted).AsTask();
        }
    }
}
