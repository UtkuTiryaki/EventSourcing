using System.Text.Json;
using System.Text.Json.Serialization;
using Dapper;
using Microsoft.Data.SqlClient;

namespace EventSourcing;

public interface IRepository 
{
    Task SaveAsync<T>(T readmodel) where T : IReadmodel;
    Task<T?> LoadAsync<T>(Guid aggregateRootId) where T : IReadmodel;
    Task DeleteAsync(Guid aggregateRootId);
}

public record ReadmodelRepository(string ConnectionString) : IRepository
{
    private static readonly JsonSerializerOptions _serializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public async Task DeleteAsync(Guid aggregateRootId)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        
        await connection.ExecuteAsync("""
            DELETE FROM dbo.Readmodels WHERE AggregateId = @AggregateId
        """, new { AggregateId = aggregateRootId }, transaction);
        
        await transaction.CommitAsync();
    }

    public async Task<T?> LoadAsync<T>(Guid aggregateRootId) where T : IReadmodel
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        var json = await connection.QuerySingleOrDefaultAsync<string>(@"
            SELECT ReadmodelData FROM dbo.Readmodels WHERE AggregateId = @AggregateId
        ", new { AggregateId = aggregateRootId });
            
        if (json is null)
            return default;
            
        return JsonSerializer.Deserialize<T>(json, _serializerOptions);
    }

    public async Task SaveAsync<T>(T readmodel) where T : IReadmodel
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        
        var readmodelData = JsonSerializer.Serialize(readmodel, readmodel.GetType(), _serializerOptions);
        var readmodelType = readmodel.GetType().AssemblyQualifiedName;
        
        await connection.ExecuteAsync("""
        MERGE dbo.Readmodels AS target
        USING (VALUES (@AggregateId, @ReadmodelType, @ReadmodelData)) 
            AS source (AggregateId, ReadmodelType, ReadmodelData)
        ON target.AggregateId = source.AggregateId
        WHEN MATCHED THEN
            UPDATE SET ReadmodelType = source.ReadmodelType, 
                    ReadmodelData = source.ReadmodelData
        WHEN NOT MATCHED THEN
            INSERT (AggregateId, ReadmodelType, ReadmodelData)
            VALUES (source.AggregateId, source.ReadmodelType, source.ReadmodelData);
        """, new { AggregateId = readmodel.Id, ReadmodelType = readmodelType, ReadmodelData = readmodelData }, transaction);
        
        await transaction.CommitAsync();
    }
}