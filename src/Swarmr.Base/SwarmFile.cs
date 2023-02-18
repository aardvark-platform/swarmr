using Spectre.Console;
using System.Security.Cryptography;

namespace Swarmr.Base;

public record SwarmFile(
    DateTimeOffset Created,
    string LogicalName,
    string FileName,
    string? Hash
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

    public bool Exists(string logicalName)
        => GetMetadataFile(logicalName).Exists;

    public SwarmFile Create(string logicalName)
    {
        if (Exists(logicalName)) throw new Exception(
            $"SwarmFile \"{logicalName}\" already exists. " +
            $"Error 68629f78-4d96-4202-905b-0e76b6fd49ed."
            );
        return new SwarmFile(
            Created: DateTimeOffset.Now,
            LogicalName: logicalName,
            FileName: Path.GetFileName(logicalName),
            Hash: null!
            );
    }

    private FileInfo GetMetadataFile(string logicalName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, "file.json"));

    public FileInfo GetContentFile(string logicalName, string fileName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, fileName));

    public FileInfo GetContentFile(SwarmFile swarmfile)
        => new(Path.Combine(GetOrCreateDir(swarmfile.LogicalName).FullName, swarmfile.FileName));

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
        var file = GetMetadataFile(f.LogicalName);
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

    public async Task SetHashFromContentFile(SwarmFile f, Action<long>? progress = null)
    {
        if (f.Hash != null) throw new Exception(
            "Can only set hash if undefined. " +
            "Error d9480da7-e263-4b64-b2db-b9b316cb88dd."
            );

        var h = await SwarmFile.ComputeHashAsync(GetContentFile(f), progress);
        f = f with { Hash = h };
        await WriteAsync(f);
    }
}