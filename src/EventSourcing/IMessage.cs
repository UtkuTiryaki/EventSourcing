using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing;


public static class EventSourcingServiceCollectionExtensions
{
    public static IServiceCollection AddCqrs(this IServiceCollection services, params Assembly[] assemblies)
    {
        if (assemblies == null || assemblies.Length == 0)
        {
            assemblies = [Assembly.GetCallingAssembly()];
        }

        services.AddScoped<IBus, Bus>();
        services.AddScoped<ICommandBus>(sp => sp.GetRequiredService<IBus>());
        services.AddScoped<IQueryBus>(sp => sp.GetRequiredService<IBus>());
        services.AddScoped<IEventBus>(sp => sp.GetRequiredService<IBus>());

        RegisterHandlers(services, assemblies, typeof(ICommandHandler<>), typeof(ICommand));
        RegisterHandlersWithResponse(services, assemblies, typeof(ICommandHandler<,>), typeof(ICommand<>));
        RegisterHandlersWithResponse(services, assemblies, typeof(IQueryHandler<,>), typeof(IQuery<>));
        RegisterHandlers(services, assemblies, typeof(IEventHandler<>), typeof(IEvent));

        return services;
    }

    private static void RegisterHandlers(IServiceCollection services, Assembly[] assemblies, Type handlerInterfaceType, Type messageType)
    {
        var handlerTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => !t.IsAbstract && !t.IsInterface)
            .Where(t => t.GetInterfaces().Any(i => IsHandlerInterface(i, handlerInterfaceType)))
            .ToList();

        foreach (var handlerType in handlerTypes)
        {
            var interfaces = handlerType.GetInterfaces()
                .Where(i => IsHandlerInterface(i, handlerInterfaceType))
                .ToList();

            foreach (var interfaceType in interfaces)
            {
                var messageTypeParam = interfaceType.GetGenericArguments()[0];
                if (messageType.IsAssignableFrom(messageTypeParam))
                {
                    services.AddTransient(interfaceType, handlerType);
                }
            }
        }
    }

    private static void RegisterHandlersWithResponse(IServiceCollection services, Assembly[] assemblies, Type handlerInterfaceType, Type messageType)
    {
        var handlerTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => !t.IsAbstract && !t.IsInterface)
            .Where(t => t.GetInterfaces().Any(i => IsHandlerInterfaceWithResponse(i, handlerInterfaceType)))
            .ToList();

        foreach (var handlerType in handlerTypes)
        {
            var interfaces = handlerType.GetInterfaces()
                .Where(i => IsHandlerInterfaceWithResponse(i, handlerInterfaceType))
                .ToList();

            foreach (var interfaceType in interfaces)
            {
                var messageTypeParam = interfaceType.GetGenericArguments()[0];
                var isMessageHandlerInterface = messageType.IsGenericTypeDefinition
                    ? messageTypeParam.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == messageType)
                    : messageType.IsAssignableFrom(messageTypeParam);

                if (isMessageHandlerInterface)
                {
                    services.AddTransient(interfaceType, handlerType);
                }
            }
        }
    }

    private static bool IsHandlerInterface(Type type, Type handlerInterfaceType)
    {
        return type.IsGenericType && type.GetGenericTypeDefinition() == handlerInterfaceType;
    }

    private static bool IsHandlerInterfaceWithResponse(Type type, Type handlerInterfaceType)
    {
        return type.IsGenericType && type.GetGenericTypeDefinition() == handlerInterfaceType;
    }
}

public sealed record Bus(IServiceProvider ServiceProvider) : IBus
{
    public async Task PublishAsync(IEvent @event, CancellationToken cancellationToken = default)
    {
        var requestType = @event.GetType();
        var handlerType = typeof(IEventHandler<>).MakeGenericType(requestType);
        
        var handlers = ServiceProvider.GetServices(handlerType);
        if (handlers is null || !handlers.Any())
        {
            throw new InvalidOperationException($"No handler found for event type {requestType}.");
        }
        
        var method = handlerType.GetMethod("HandleAsync") ?? 
            throw new InvalidOperationException($"No HandleAsync method found for handler type {handlerType}.");
        
        var tasks = handlers.Select(handler => 
            (Task)method.Invoke(handler, [@event, cancellationToken])!);
        
        await Task.WhenAll(tasks);
    }

    public async Task<TResponse> SendAsync<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default)
    {
        var requestType = command.GetType();
        var handlerType = typeof(ICommandHandler<,>).MakeGenericType(requestType, typeof(TResponse));
        
        var handler = ServiceProvider.GetService(handlerType) ?? 
            throw new InvalidOperationException($"No handler found for command type {requestType}.");
        
        var method = handlerType.GetMethod("HandleAsync") ?? 
            throw new InvalidOperationException($"No HandleAsync method found for handler type {handlerType}.");
        
        var result = (Task<TResponse>)method.Invoke(handler, [command, cancellationToken])!;
        
        return await result;
    }

    public async Task SendAsync(ICommand command, CancellationToken cancellationToken = default)
    {
        var requestType = command.GetType();
        var handlerType = typeof(ICommandHandler<>).MakeGenericType(requestType);
        
        var handler = ServiceProvider.GetService(handlerType) ?? 
            throw new InvalidOperationException($"No handler found for command type {requestType}.");
        
        var method = handlerType.GetMethod("HandleAsync") ?? 
            throw new InvalidOperationException($"No HandleAsync method found for handler type {handlerType}.");
        
        var task = (Task)method.Invoke(handler, [command, cancellationToken])!;
        
        await task;
    }

    public async Task<TResponse> SendAsync<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default)
    {
        var requestType = query.GetType();
        var handlerType = typeof(IQueryHandler<,>).MakeGenericType(requestType, typeof(TResponse));
        
        var handler = ServiceProvider.GetService(handlerType) ?? 
            throw new InvalidOperationException($"No handler found for query type {requestType}.");
        
        var method = handlerType.GetMethod("HandleAsync") ?? 
            throw new InvalidOperationException($"No HandleAsync method found for handler type {handlerType}.");
        
        var result = (Task<TResponse>)method.Invoke(handler, [query, cancellationToken])!;
        
        return await result;
    }
}

public interface IMessage 
{    
}

public interface ICommand<out TResponse> : IMessage { }
public interface ICommandHandler<in TRequest, TResponse>
    where TRequest : ICommand<TResponse>
{
    Task<TResponse> HandleAsync(TRequest command, CancellationToken cancellationToken);
}

public interface ICommand : IMessage { }
public interface ICommandHandler<in TRequest> where TRequest : ICommand
{
    Task HandleAsync(TRequest command, CancellationToken cancellationToken);
}

public interface IQuery<out TResponse> : IMessage { }
public interface IQueryHandler<in TRequest, TResponse>
    where TRequest : IQuery<TResponse>
{
    Task<TResponse> HandleAsync(TRequest query, CancellationToken cancellationToken);
}

public interface IEvent : IMessage { }
public interface IEventHandler<in TEvent> where TEvent : IEvent
{
    Task HandleAsync(TEvent @event, CancellationToken cancellationToken);
}

public interface ICommandBus 
{
    Task<TResponse> SendAsync<TResponse>(ICommand<TResponse> command, CancellationToken cancellationToken = default);
    Task SendAsync(ICommand command, CancellationToken cancellationToken = default);
}

public interface IQueryBus
{
    Task<TResponse> SendAsync<TResponse>(IQuery<TResponse> query, CancellationToken cancellationToken = default);
}

public interface IEventBus 
{
    Task PublishAsync(IEvent @event, CancellationToken cancellationToken = default);
}

public interface IBus : ICommandBus, IEventBus, IQueryBus;