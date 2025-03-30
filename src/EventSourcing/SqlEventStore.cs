using System.Text.Json;
using System.Text.Json.Serialization;
using Dapper;
using MediatR;
using Microsoft.Data.SqlClient;

namespace EventSourcing;

public interface IEventStore 
{
    Task SaveStreamAsync(EventStream eventStream);
    Task<EventStream> LoadStreamAsync(Guid aggregateId);
}

internal record SqlEventStore(string ConnectionString, IPublisher Publisher) : IEventStore
{
    private static readonly JsonSerializerOptions _serializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
    
    public async Task SaveStreamAsync(EventStream eventStream) 
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        
        foreach (var @event in eventStream.Events) 
        {
            var eventData = JsonSerializer.Serialize(@event, @event.GetType(), _serializerOptions);
            var eventType = @event.GetType().AssemblyQualifiedName;
            await connection.ExecuteAsync("""
                INSERT INTO dbo.Events (AggregateId, EventType, EventData, OccurredOn)
                VALUES (@AggregateId, @EventType, @EventData, @OccurredOn)
            """, new { AggregateId = eventStream.AggregateId, EventType = eventType, EventData = eventData, OccurredOn = DateTime.Now }, transaction);
        }
        
        await transaction.CommitAsync();
        foreach (var @event in eventStream.Events) 
        {
            await Publisher.Publish(@event);
        }
    }
    
    public async Task<EventStream> LoadStreamAsync(Guid aggregateId) 
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        var storedEvents = await connection.QueryAsync("""
            SELECT EventType, EventData FROM dbo.Events WHERE AggregateId = @AggregateId ORDER BY OccurredOn ASC
        """, new { AggregateId = aggregateId });
        
        List<DomainEvent> events = [];
        foreach (var @record in storedEvents) 
        {
            var type = Type.GetType(@record.EventType);
            if (type is null) continue;
            var domainEvent = JsonSerializer.Deserialize(@record.EventData, type, _serializerOptions);
            if (domainEvent is null) continue;
            
            events.Add((DomainEvent)domainEvent);
        }
        
        return new EventStream(aggregateId, events);
    }
}