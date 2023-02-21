using Spectre.Console;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace Swarmr.Base;

public record SwarmFile(
    DateTimeOffset Created,
    string LogicalName,
    string FileName,
    string? Hash
    )
{
    public const string METAFILE_NAME = "___swarmfile.json";

    public static async Task<string> ComputeHashAsync(FileInfo file, Action<long>? progress = null)
    {
        var maxLength = Math.Min(file.Length, 128 * 1024 * 1024);
        var filestream = file.OpenRead();
        var hashstream = new TruncatedStream(filestream, maxLength: maxLength, progress);
        var sha256 = await SHA256.Create().ComputeHashAsync(hashstream);
        var hash = Convert.ToHexString(sha256).ToLowerInvariant();
        hashstream.Close();
        filestream.Close();
        return hash;
    }
}


public class LocalSwarmFiles
{
    private readonly DirectoryInfo _basedir;

    public IEnumerable<SwarmFile> Files => List();

    public LocalSwarmFiles(string basedir)
    {
        _basedir = new(basedir);
        if (!_basedir.Exists) _basedir.Create();
    }

    public DirectoryInfo GetOrCreateDir(string logicalName)
    {
        var dir = new DirectoryInfo(Path.Combine(_basedir.FullName, logicalName));
        if (!dir.Exists) dir.Create();
        return dir;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DirectoryInfo GetDir(SwarmFile swarmfile)
        => new(Path.Combine(_basedir.FullName, swarmfile.LogicalName));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Exists(string logicalName)
        => GetMetadataFile(logicalName).Exists;

    public void Delete(SwarmFile swarmfile)
        => GetDir(swarmfile).Delete(recursive: true);

    public SwarmFile Create(string logicalName, string? fileName = null, bool force = false)
    {
        fileName ??= Path.GetFileName(logicalName);
        var result = new SwarmFile(
            Created: DateTimeOffset.Now,
            LogicalName: logicalName,
            FileName: fileName,
            Hash: null!
            );

        if (Exists(logicalName))
        {
            if (force)
            {
                //GetMetadataFile(result).Delete();
            }
            else
            {
                throw new Exception(
                    $"SwarmFile \"{logicalName}\" already exists. " +
                    $"Error 68629f78-4d96-4202-905b-0e76b6fd49ed."
                    );
            }
        }

        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public FileInfo GetMetadataFile(string logicalName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, SwarmFile.METAFILE_NAME));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public FileInfo GetMetadataFile(SwarmFile swarmfile)
        => new(Path.Combine(GetOrCreateDir(swarmfile.LogicalName).FullName, SwarmFile.METAFILE_NAME));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public FileInfo GetContentFile(string logicalName, string fileName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, fileName));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            if (info.Name == SwarmFile.METAFILE_NAME) continue;
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

    public IEnumerable<SwarmFile> List()
    {
        var xs = _basedir
            .EnumerateFiles(SwarmFile.METAFILE_NAME, SearchOption.AllDirectories)
            .ToList();

        var result = xs
            .Select(f => 
            {
                try
                {
                    return SwarmUtils.TryDeserialize<SwarmFile>(File.ReadAllText(f.FullName));
                }
                catch (Exception e)
                {
                    AnsiConsole.MarkupLine(
                        $"[red][[ERROR]]Corrupt swarm file metadata. " +
                        $"Deleting {f.FullName.EscapeMarkup()}.\n" +
                        $"{e.Message.EscapeMarkup()}[/]"
                        );
                    f.Delete();
                    return null;
                }
            })
            .Where(x => x != null)
            ;

        return (IEnumerable<SwarmFile>)result;
    }
}