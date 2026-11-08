var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(builder.Configuration["urls"] ?? "http://127.0.0.1:5071");
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 64 * 1024);
builder.Services.AddHealthChecks();
var app = builder.Build();
using var capacity = new SemaphoreSlim(4);
app.MapHealthChecks("/health");
app.MapMethods("/{kind:regex(^(work|bulk)$)}", ["GET", "POST"], async (HttpContext context, string kind, int delayMs = 80) =>
{
    if (delayMs is < 0 or > 2000)
        return Results.BadRequest();

    await capacity.WaitAsync(context.RequestAborted);
    try
    {
        await context.Request.Body.CopyToAsync(Stream.Null, context.RequestAborted);
        await Task.Delay(delayMs, context.RequestAborted);
        return Results.Ok(new { kind, delayMs, bytes = context.Request.ContentLength ?? 0 });
    }
    finally
    {
        capacity.Release();
    }
});

await app.RunAsync();
