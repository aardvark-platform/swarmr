using System.Security.Cryptography;

namespace Swarmr.Base;

public record SwarmFile(
    string Name,
    DateTimeOffset Created,
    string Hash,
    string FileName
    )
{
    public static async Task<string> ComputeHashAsync(FileInfo file, Action<long>? progress = null)
    {
        var maxLength = Math.Min(file.Length, 128 * 1024 * 1024);
        var filestream = file.OpenRead();
        var hashstream = new TruncateStream(filestream, maxLength: maxLength, progress);
        var sha256 = await SHA256.Create().ComputeHashAsync(hashstream);
        var hash = Convert.ToHexString(sha256).ToLowerInvariant();
        hashstream.Close();
        filestream.Close();
        return hash;
    }
}
