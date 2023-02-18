using Spectre.Console;
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


public class LocalSwarmFiles
{
    private DirectoryInfo _basedir;

    public LocalSwarmFiles(string basedir)
    {
        _basedir = new(basedir);
        if (!_basedir.Exists) _basedir.Create();
    }

    private DirectoryInfo GetOrCreateDir(string logicalName)
    {
        var dir = new DirectoryInfo(Path.Combine(_basedir.FullName, logicalName));
        if (!dir.Exists) dir.Create();
        return dir;
    }

    private FileInfo GetMetadataFile(string logicalName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, "file.json"));

    public FileInfo GetContentFile(string logicalName, string fileName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, fileName));

    public FileInfo GetContentFile(SwarmFile swarmfile)
        => new(Path.Combine(GetOrCreateDir(swarmfile.Name).FullName, swarmfile.FileName));

    public async Task<SwarmFile?> TryReadAsync(string name)
    {
        var file = GetMetadataFile(name);

        if (file.Exists)
        {
            var s = await File.ReadAllTextAsync(file.FullName);
            return SwarmUtils.Deserialize<SwarmFile>(s);
        }
        else
        {
            return null;
        }
    }

    public async Task WriteAsync(SwarmFile f)
    {
        var file = GetMetadataFile(f.Name);
        await File.WriteAllTextAsync(file.FullName, f.ToJsonString());

        // delete old content files (with different name)
        foreach (var info in file.Directory!.EnumerateFileSystemInfos())
        {
            if (info.Name == "file.json") continue;
            if (info.Name == f.FileName) continue;
            info.Delete();
            AnsiConsole.WriteLine($"[LocalSwarmFiles] deleted {info.FullName}");
        }
    }
}