using Spectre.Console;
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

    public static SwarmFile Create(string logicalName, string? fileName = null)
        => new(
            Created: DateTimeOffset.Now,
            LogicalName: logicalName,
            FileName: fileName ?? Path.GetFileName(logicalName),
            Hash: null!
            );
}

public static class SwarmFileExtensions
{
    public static (string urlContent, string urlMetadata) GetDownloadLinks(this SwarmFile file, Node fromNode)
    {
        var prefix = $"{fromNode.ConnectUrl}/static/files/{file.LogicalName}";
        return (
            urlContent: $"{prefix}/{file.FileName}",
            urlMetadata: $"{prefix}/{SwarmFile.METAFILE_NAME}"
        );
    }

    public static async Task<SwarmFile> DownloadToLocalAsync(this SwarmFile file, Node fromNode, LocalSwarmFiles toLocal)
    {
        var (urlContent, urlMetadata) = file.GetDownloadLinks(fromNode);
        using var http = new HttpClient();

        var fileContent = toLocal.GetContentFileInfo(file);
        var fileMetadata = toLocal.GetMetadataFileInfo(file);

        await using (var @lock = await toLocal.GetLockAsync(file))
        {
            fileMetadata.Delete();
            await http.DownloadToFile(urlContent, fileContent);
            await http.DownloadToFile(urlMetadata, fileMetadata);
        }

        return file;
    }
}

public class LocalSwarmFiles
{
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

    // LocalSwarmFiles

    private readonly DirectoryInfo _basedir;

    public LocalSwarmFiles(string basedir)
    {
        _basedir = new(basedir);
        if (!_basedir.Exists) _basedir.Create();
    }

    public DirectoryInfo BaseDir => _basedir;

    // create

    public SwarmFile Create(string logicalName, string? fileName = null)
    {
        if (Exists(logicalName)) throw new Exception(
            $"SwarmFile \"{logicalName}\" already exists in {_basedir}. " +
            $"Error 68629f78-4d96-4202-905b-0e76b6fd49ed."
            );

        return new SwarmFile(
            Created: DateTimeOffset.Now,
            LogicalName: logicalName,
            FileName: fileName ?? Path.GetFileName(logicalName),
            Hash: null!
            );
    }

    public async Task<SwarmFile> SetHashFromContentFile(SwarmFile f, Action<long>? progress = null)
    {
        if (f.Hash != null) throw new Exception(
            "Can only set hash if undefined. " +
            "Error d9480da7-e263-4b64-b2db-b9b316cb88dd."
            );

        var h = await ComputeHashAsync(GetContentFileInfo(f), progress);
        f = f with { Hash = h };
        return await WriteAsync(f);
    }

    // list

    public IEnumerable<SwarmFile> Files => List();

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

    // exists

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Exists(string logicalName)
        => GetMetadataFileInfo(logicalName).Exists;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Exists(SwarmFile file)
        => GetMetadataFileInfo(file).Exists;

    // read/write

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public FileInfo GetMetadataFileInfo(SwarmFile swarmfile)
        => new(Path.Combine(GetOrCreateDir(swarmfile.LogicalName).FullName, SwarmFile.METAFILE_NAME));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public FileInfo GetContentFileInfo(string logicalName, string fileName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, fileName));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public FileInfo GetContentFileInfo(SwarmFile swarmfile)
        => new(Path.Combine(GetOrCreateDir(swarmfile.LogicalName).FullName, swarmfile.FileName));

    public async Task<SwarmFile?> TryReadAsync(string logicalName)
    {
        var file = GetMetadataFileInfo(logicalName);

        await using var @lock = await GetLockAsync(logicalName);

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

    public async Task<SwarmFile> WriteAsync(SwarmFile f)
    {
        await using var @lock = await GetLockAsync(f);

        var file = GetMetadataFileInfo(f.LogicalName);
        await File.WriteAllTextAsync(file.FullName, f.ToJsonString());

        // delete old content files (with different name)
        foreach (var info in file.Directory!.EnumerateFileSystemInfos())
        {
            if (info.Name == SwarmFile.METAFILE_NAME) continue;
            if (info.Name == f.FileName) continue;
            info.Delete();
            AnsiConsole.WriteLine($"[LocalSwarmFiles] deleted {info.FullName}");
        }

        return f;
    }

    // delete

    public async Task Delete(SwarmFile f)
    {
        await using var @lock = await GetLockAsync(f);
        GetDirectoryInfo(f).Delete(recursive: true);
    }

    #region locks

    public sealed class AsyncDisposable : IDisposable, IAsyncDisposable
    {
        private Func<Task>? _disposeAction;

        public AsyncDisposable(Func<Task> disposeAction)
        {
            _disposeAction = disposeAction;
        }

        public async ValueTask DisposeAsync()
        {
            await (_disposeAction?.Invoke() ?? Task.CompletedTask);
            GC.SuppressFinalize(this);
        }

        public void Dispose()
        {
            _disposeAction?.Invoke();
            _disposeAction = null;
        }
    }

    private Dictionary<string, SemaphoreSlim> _locks = new();

    public Task<IAsyncDisposable> GetLockAsync(SwarmFile file, CancellationToken ct = default)
        => GetLockAsync(file.LogicalName, ct);

    public async Task<IAsyncDisposable> GetLockAsync(string logicalName, CancellationToken ct = default)
    {
        SemaphoreSlim? sem;
        lock (_locks)
        {
            if (!_locks.TryGetValue(logicalName, out sem))
            {
                _locks[logicalName] = sem = new SemaphoreSlim(1);
            }
        }

        await sem.WaitAsync(ct);

        return new AsyncDisposable(() =>
        {
            sem.Release();
            return Task.CompletedTask;
        });
    }

    #endregion

    #region Internal


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private DirectoryInfo GetOrCreateDir(string logicalName)
    {
        var dir = new DirectoryInfo(Path.Combine(_basedir.FullName, logicalName));
        if (!dir.Exists) dir.Create();
        return dir;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private DirectoryInfo GetDirectoryInfo(SwarmFile swarmfile)
        => new(Path.Combine(_basedir.FullName, swarmfile.LogicalName));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private FileInfo GetMetadataFileInfo(string logicalName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, SwarmFile.METAFILE_NAME));

    #endregion
}