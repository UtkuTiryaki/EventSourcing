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
        
        await _sqlContainer.ExecScriptAsync(@"
            IF OBJECT_ID('dbo.Readmodels', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Readmodels (
                    AggregateId UNIQUEIDENTIFIER PRIMARY KEY,
                    ReadmodelType NVARCHAR(500) NOT NULL,
                    ReadmodelData NVARCHAR(MAX) NOT NULL
                );
            END
        ");
        
        var services = new ServiceCollection();
        services.AddMediatR(o => o.RegisterServicesFromAssemblyContaining<EventSourcingIntegrationTests>());
        services.AddEventSourcing(_sqlContainer.GetConnectionString());
        services.AddCqrs();
        _serviceProvider = services.BuildServiceProvider();
    }
    
    public Task DisposeAsync() => _sqlContainer.DisposeAsync().AsTask();
    
    [Fact]
    public async Task SendAsync_Command_WithResponse_ReturnsCorrectValue()
    {
        // Arrange
        var bus = _serviceProvider.GetRequiredService<IBus>();
        var command = new TestCommand("Test");

        // Act
        var result = await bus.SendAsync(command);

        // Assert
        Assert.Equal("Test", result);
    }
    
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
    
    [Fact]
    public async Task SaveLoadDelete_ReadmodelRepository_Works()
    {
        var repository = _serviceProvider.GetRequiredService<IRepository>();
        var readmodel = new ExampleReadmodel(Guid.NewGuid(), "Test Name");

        await repository.SaveAsync(readmodel);

        var loaded = await repository.LoadAsync<ExampleReadmodel>(readmodel.Id);
        Assert.NotNull(loaded);
        Assert.Equal(readmodel.Id, loaded!.Id);
        Assert.Equal("Test Name", loaded!.Name);

        var updatedReadmodel = new ExampleReadmodel(readmodel.Id, "Updated Name");
        await repository.SaveAsync(updatedReadmodel);

        var loadedUpdated = await repository.LoadAsync<ExampleReadmodel>(readmodel.Id);
        Assert.NotNull(loadedUpdated);
        Assert.Equal("Updated Name", loadedUpdated!.Name);

        await repository.DeleteAsync(readmodel.Id);
        var afterDelete = await repository.LoadAsync<ExampleReadmodel>(readmodel.Id);
        Assert.Null(afterDelete);
    }
    
    [Fact]
    public void ExampleProjection_Projects_Correctly()
    {
        var aggregateId = Guid.NewGuid();
        var events = new List<DomainEvent>
        {
            new ExampleCreatedEvent(aggregateId, "Initial Name"),
            new ExampleNameChangedEvent(aggregateId, "Updated Name")
        };
        var eventStream = new EventStream(aggregateId, events);
        var projection = new ExampleProjection(eventStream);

        var readmodel = projection.Project(aggregateId) as ExampleReadmodel;

        Assert.NotNull(readmodel);
        Assert.Equal(aggregateId, readmodel!.Id);
        Assert.Equal("Updated Name", readmodel!.Name);
    }
}

public record TestCommand(string Name) : ICommand<string>;
public record TestCommandHandler : ICommandHandler<TestCommand, string>
{
    public Task<string> HandleAsync(TestCommand command, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(command.Name);
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

public record ExampleProjection(EventStream EventStream) : Projection(EventStream)
{
    public override IReadmodel? Project(Guid aggregateRootId)
    {
        ExampleReadmodel? readmodel = null; 
        foreach (var @event in EventStream.Events) 
        {
            readmodel = @event switch
            {
                ExampleCreatedEvent e => new ExampleReadmodel(e.AggregateId, e.Name),
                ExampleNameChangedEvent e when readmodel is not null => readmodel with { Name = e.NewName },
                _ => readmodel
            };
        }
        
        return readmodel;
    }
}

public record ExampleReadmodel(Guid Id, string Name) : IReadmodel;