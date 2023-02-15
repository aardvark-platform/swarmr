﻿using System.Text.Json;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public static class SwarmUtils
{
    public static string ToJsonString(this object self, JsonSerializerOptions? jsonSerializerOptions = null)
        => JsonSerializer.Serialize(self, jsonSerializerOptions ?? JsonOptions);

    public static T Deserialize<T>(object self) => self switch
    {
        T x => x,

        JsonElement e =>
            JsonSerializer.Deserialize<T>(e, JsonOptions)
            ?? throw new Exception(
                "Error 81a683b2-8fbc-4a3f-b967-37013883b05e."
                ),

        string s =>
            JsonSerializer.Deserialize<T>(s, JsonOptions)
            ?? throw new Exception(
                $"Failed to deserialize JSON \"{s}\". " +
                $"Error a6a602f7-277a-4f76-9d80-8cbd2ecd78c1."
                ),

        null => throw new Exception(
            "Missing request. " +
            "Error b96a7af3-7f30-4aa9-a713-f36568e179a7."
            ),

        _ => throw new Exception(
            $"Unknown request object \"{self.GetType().FullName}\". " +
            $"Error d6f9dfbd-ed17-4ba8-94c7-afa7e1663cd3."
            )
    };

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        AllowTrailingCommas = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals | JsonNumberHandling.AllowReadingFromString,
        Converters =
        {
            new JsonStringEnumConverter(),
        },
    };
}

public class TruncateStream : Stream
{
    private readonly Stream _innerStream;
    private readonly long _maxLength;
    private readonly Action<long>? _progress;
    private long _position;

    public TruncateStream(Stream innerStream, long maxLength, Action<long>? progress = default)
    {
        _innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));
        _maxLength = maxLength;
        _progress = progress;
        _position = 0;
    }

    public override bool CanRead => _innerStream.CanRead;
    public override bool CanSeek => _innerStream.CanSeek;
    public override bool CanWrite => false;
    public override long Length => Math.Min(_innerStream.Length, _maxLength);
    public override long Position { get => _position; set => throw new NotSupportedException(); }
    public override void Flush() => _innerStream.Flush();

    public override int Read(byte[] buffer, int offset, int count)
    {
        long remaining = _maxLength - _position;
        if (remaining <= 0) return 0;
        int readCount = (int)Math.Min(remaining, count);
        int bytesRead = _innerStream.Read(buffer, offset, readCount);
        _position += bytesRead;
        _progress?.Invoke(_position);
        return bytesRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }
}
