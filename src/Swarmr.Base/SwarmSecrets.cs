using Aardvark.Base.Cryptography;
using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace Swarmr.Base;

public record SwarmSecrets(string Id, long Revision, DateTimeOffset Created, ImmutableDictionary<string, string> Map) {

    public static readonly SwarmSecrets Empty = new(
        Id: "",
        Revision: 0L,
        Created: DateTimeOffset.MinValue,
        Map: ImmutableDictionary<string, string>.Empty
        );

    [JsonIgnore]
    private FileInfo File { get; set; } = null!;

    public async Task<SwarmSecrets> SaveAsync() {
        await Secrets.EncryptStringToFileAsync(
            source: this.ToJsonString(),
            targetFile: File.FullName,
            password: ""
            );
        return this;
    }

    public static async Task<SwarmSecrets> CreateAsync(FileInfo file) {

        if (file.Exists) {
            return SwarmUtils.Deserialize<SwarmSecrets>(
                await Secrets.DecryptFileToStringAsync(sourceFile: file.FullName, password: "")
                ) with { File = file };
        }
        else {
            return new(
                Id: Guid.NewGuid().ToString(),
                Revision: 0L,
                Created: DateTimeOffset.UtcNow,
                Map: ImmutableDictionary<string, string>.Empty
                ) {
                File = file
            };
        }
    }

    public SwarmSecrets Set(string key, string value) => this with {
        Revision = Revision + 1,
        Map = Map.SetItem(key, value)
    };

    public SwarmSecrets Remove(string key) => this with {
        Revision = Revision + 1,
        Map = Map.Remove(key)
    };

    public async Task<string> EncodeAsync() {
        var buffer = await Secrets.EncryptStringToBufferAsync(source: this.ToJsonString(), password: "");
        var s = Convert.ToHexString(buffer);
        return s;
    }

    public static async Task<SwarmSecrets> DecodeAsync(string s, FileInfo file) 
    {
        var buffer = Convert.FromHexString(s);
        var json = await Secrets.DecryptBufferToStringAsync(source: buffer, password: "");
        var secrets = SwarmUtils.Deserialize<SwarmSecrets>(json) with { File = file };
        return secrets;
    }
}
