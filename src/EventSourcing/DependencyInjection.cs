using MediatR;
using Microsoft.Extensions.DependencyInjection;

namespace EventSourcing;

public static class DependencyInjection 
{
    public static IServiceCollection AddEventSourcing(this IServiceCollection services, string connectionString) 
    {
        services.AddScoped<IEventStore>(sp => 
        {
            var publisher = sp.GetRequiredService<IPublisher>();
            return new SqlEventStore(connectionString, publisher);
        });
        
        services.AddScoped<IRepository>(sp => 
        {
            return new ReadmodelRepository(connectionString);
        });
        
        return services;
    }
}