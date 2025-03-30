using Testcontainers.MsSql;

namespace EventSourcing.Tests;

public class EventSourcingIntegrationTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlContainer;
    private IServiceProvider _serviceProvider = null!;
    
    public EventSourcingIntegrationTests()
    {
        _sqlContainer = new MsSqlBuilder()
            .WithCleanUp(true)
            .Build();
    }
    
    public async Task InitializeAsync()  
    {
        await _sqlContainer.StartAsync();
        await _sqlContainer.ExecScriptAsync("""
        IF OBJECT_ID('dbo.Events', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.Events (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                AggregateId UNIQUEIDENTIFIER NOT NULL,
                EventType NVARCHAR(500) NOT NULL,
                EventData NVARCHAR(MAX) NOT NULL,
                OccurredOn DATETIME NOT NULL
            );
        END
        """);
        
        var services = new ServiceCollection();
        services.AddMediatR(o => o.RegisterServicesFromAssemblyContaining<EventSourcingIntegrationTests>());
        services.AddEventSourcing(_sqlContainer.GetConnectionString());
        _serviceProvider = services.BuildServiceProvider();
    }
    
    public Task DisposeAsync() => _sqlContainer.DisposeAsync().AsTask();
    
    [Fact]
    [Trait("DockerPlatform", "Linux")]
    public async Task Can_Create_And_Load_ExampleAggregate() 
    {
        var eventStore = _serviceProvider.GetRequiredService<IEventStore>();
        
        Guid aggregateId = Guid.NewGuid();
        
        var created = ExampleAggregate.Create(aggregateId, "Initial Name");        
        var stream = EventStream.Create(aggregateId)
            .Append(created.UncommittedEvents);    
        await eventStore.SaveStreamAsync(stream);
        
        var loadedStream = await eventStore.LoadStreamAsync(aggregateId);
        var loadedAggregate = loadedStream.Replay<ExampleAggregate>();
        
        Assert.NotNull(loadedAggregate);
        Assert.Equal("Initial Name", loadedAggregate.Name);
        Assert.Equal(aggregateId, loadedAggregate.Id);
        
        var updatedAggregate = loadedAggregate.ChangeName("Updated Name");
        var updatedStream = loadedStream.Append(updatedAggregate.UncommittedEvents);
        await eventStore.SaveStreamAsync(updatedStream);
        
        var finalStream = await eventStore.LoadStreamAsync(aggregateId);
        var finalAggregate = finalStream.Replay<ExampleAggregate>();
        
        Assert.NotNull(finalAggregate);
        Assert.Equal("Updated Name", finalAggregate.Name);
        Assert.Equal(aggregateId, finalAggregate.Id);
    }
}

public record ExampleCreatedEvent(Guid AggregateId, string Name) : DomainEvent;

public record ExampleNameChangedEvent(Guid AggregateId, string NewName) : DomainEvent;

public record ExampleAggregate : AggregateRoot
{    
    public string Name { get; init; }

    public ExampleAggregate(Guid id, string name)
    {
        Id = id;
        Name = name;
    }
    
    public static ExampleAggregate Create(Guid id, string name)
    {
        var aggregate = new ExampleAggregate(id, name);
        return aggregate.AddDomainEvent<ExampleAggregate>(new ExampleCreatedEvent(id, name));
    }

    public ExampleAggregate ChangeName(string newName)
    {
        return this.AddDomainEvent<ExampleAggregate>(new ExampleNameChangedEvent(Id, newName));
    }

    public override AggregateRoot Apply(DomainEvent domainEvent) =>
        domainEvent switch
        {
            ExampleCreatedEvent e => this with { Id = e.AggregateId, Name = e.Name },
            ExampleNameChangedEvent e => this with { Name = e.NewName },
            _ => this
        };
}