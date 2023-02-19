namespace Swarmr.Base;

public class TruncatedStream : Stream
{
    private readonly Stream _innerStream;
    private readonly long _maxLength;
    private readonly Action<long>? _progress;
    private long _position;

    public TruncatedStream(Stream innerStream, long maxLength, Action<long>? progress = default)
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
