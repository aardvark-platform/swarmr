using Spectre.Console;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace Swarmr.Base;

public interface ISwarmFileEntry 
{
    DateTimeOffset Created { get; }
    string LogicalName { get; }
}

public record SwarmFileDir(
    DateTimeOffset Created,
    string LogicalName
    ) : ISwarmFileEntry;

public record SwarmFile(
    DateTimeOffset Created,
    string LogicalName,
    string FileName,
    string? Hash
    ) : ISwarmFileEntry 
{
    public const string METAFILE_NAME = "___swarmfile.json";
    public const string LOCKFILE_NAME = "___lock";

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

        await using (var @lock = await toLocal.GetLockAsync(file, label: "DownloadToLocalAsync"))
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
    private readonly string _nodeId;
    private readonly byte[] _nodeIdUtf8;

    public LocalSwarmFiles(string basedir, string nodeId)
    {
        _basedir = new(basedir);
        if (!_basedir.Exists) _basedir.Create();

        _nodeId = nodeId;
        _nodeIdUtf8 = Encoding.UTF8.GetBytes(_nodeId);
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

    public IEnumerable<SwarmFile> Files => List(recursive: true).Cast<SwarmFile>();

    public IEnumerable<ISwarmFileEntry> List(string? path = null, bool recursive = false)
    {
        var dir = path != null ? new DirectoryInfo(Path.Combine(_basedir.FullName, path)) : _basedir;

        var result = new List<ISwarmFileEntry>();

        var xs = recursive
            ? dir.EnumerateFiles(SwarmFile.METAFILE_NAME, SearchOption.AllDirectories)
            : dir.EnumerateFiles(SwarmFile.METAFILE_NAME, SearchOption.TopDirectoryOnly)
            ;

        foreach (var x in xs) {
            try {
                var f = SwarmUtils.TryDeserialize<SwarmFile>(File.ReadAllText(x.FullName));
                if (f != null) result.Add(f);
            }
            catch (Exception e) {
                AnsiConsole.MarkupLine(
                    $"[red][[ERROR]]Corrupt swarm file metadata. " +
                    $"Deleting {x.FullName.EscapeMarkup()}.\n" +
                    $"{e.Message.EscapeMarkup()}[/]"
                    );
                x.Delete();
            }
        }

        if (!recursive) 
        {
            var ys = dir.EnumerateDirectories();
            foreach (var y in ys) {
                var m = new FileInfo(Path.Combine(y.FullName, SwarmFile.METAFILE_NAME));
                if (m.Exists) {
                    try {
                        var f = SwarmUtils.TryDeserialize<SwarmFile>(File.ReadAllText(m.FullName));
                        if (f != null) result.Add(f);
                    }
                    catch (Exception e) {
                        AnsiConsole.MarkupLine(
                            $"[red][[ERROR]]Corrupt swarm file metadata. " +
                            $"Deleting {m.FullName.EscapeMarkup()}.\n" +
                            $"{e.Message.EscapeMarkup()}[/]"
                            );
                        m.Delete();
                    }
                }
                else {
                    result.Add(new SwarmFileDir(Created: y.CreationTimeUtc, LogicalName: y.Name));
                }
            }
        }

        return result;
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

        await using var @lock = await GetLockAsync(logicalName, label: "TryReadAsync");

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
        await using var @lock = await GetLockAsync(f, label: "WriteAsync");

        var file = GetMetadataFileInfo(f.LogicalName);
        await File.WriteAllTextAsync(file.FullName, f.ToJsonString());

        // delete old content files (with different name)
        foreach (var info in file.Directory!.EnumerateFileSystemInfos())
        {
            if (info.Name == SwarmFile.METAFILE_NAME) continue;
            if (info.Name == SwarmFile.LOCKFILE_NAME) continue;
            if (info.Name == f.FileName) continue;
            info.Delete();
            AnsiConsole.WriteLine($"[LocalSwarmFiles] deleted {info.FullName}");
        }

        return f;
    }

    // delete

    public async Task Delete(SwarmFile f)
    {
        await using var @lock = await GetLockAsync(f, label: "Delete");
        GetDirectoryInfo(f).Delete(recursive: true);
    }

    public void DeleteDir(string logicalName) 
    {
        GetDirectoryInfo(logicalName).Delete(recursive: true);
    }

    #region locks

    private Dictionary<string, FileStream> _locks = new();

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

    public Task<IAsyncDisposable> GetLockAsync(SwarmFile file, string label, CancellationToken ct = default)
        => GetLockAsync(file.LogicalName, label, ct);

    public async Task<IAsyncDisposable> GetLockAsync(string logicalName, string label, CancellationToken ct = default)
    {
        FileInfo? lockFileInfo = null;
        FileStream? f = null;

        lock (_locks) _locks.TryGetValue(logicalName, out f);

        if (f == null)
        {
            GetDirectoryInfo(logicalName).Create();
            lockFileInfo = GetLockFileInfo(logicalName);
            var waitMilliseconds = 42;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    f = lockFileInfo.Open(FileMode.Create, access: FileAccess.Write, share: FileShare.Read);
                    f.Write(_nodeIdUtf8);
                    f.Flush();
                    lock (_locks) { _locks.Add(logicalName, f); }
                    //AnsiConsole.MarkupLine($"[lime][[LOCK]][[{label}]] {logicalName} ACQUIRED[/]");
                    break;
                }
                catch
                {
                    //AnsiConsole.MarkupLine($"[lime][[LOCK]][[{label}]] {logicalName} RETRY IN {waitMilliseconds} ms[/]");
                    await Task.Delay(waitMilliseconds, ct);
                    waitMilliseconds = Math.Min(waitMilliseconds * 2, 1000);
                }
            }
        }
        else
        {
            //AnsiConsole.MarkupLine($"[lime][[LOCK]][[{label}]] {logicalName} ACQUIRED (CACHED)[/]");
        }

        return new AsyncDisposable(() =>
        {
            f.Close();
            lock (_locks) _locks.Remove(logicalName);
            try 
            {
                lockFileInfo!.Delete();
            } 
            catch
            {
                //AnsiConsole.MarkupLine($"[lime][[LOCK]][[{label}]] {logicalName} LOCKFILE DELETE FAILED[/]");
            }
            //AnsiConsole.MarkupLine($"[lime][[LOCK]][[{label}]] {logicalName} RELEASED[/]");
            return Task.CompletedTask;
        });
    }

    #endregion

    #region Internal

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private FileInfo GetLockFileInfo(string logicalName)
        => new(Path.Combine(GetDirectoryInfo(logicalName).FullName, SwarmFile.LOCKFILE_NAME));

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
    private DirectoryInfo GetDirectoryInfo(string logicalName)
        => new(Path.Combine(_basedir.FullName, logicalName));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private FileInfo GetMetadataFileInfo(string logicalName)
        => new(Path.Combine(GetOrCreateDir(logicalName).FullName, SwarmFile.METAFILE_NAME));

    #endregion
}