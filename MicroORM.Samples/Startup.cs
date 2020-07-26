using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MicroORM.Internal;
using MicroORM.Middleware;
using MicroORM.Samples.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.PlatformAbstractions;

namespace MicroORM.Samples
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers().AddNewtonsoftJson(o => o.SerializerSettings.ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore);

            services.AddMicroORM(Configuration["DefaultConnString"]);

            services.AddSwaggerGen(o =>
            {
                o.SwaggerDoc("v1", new Microsoft.OpenApi.Models.OpenApiInfo()
                {
                    Title = "Demonstration API for MicroORM",
                    Version = "v1",
                    Contact = new Microsoft.OpenApi.Models.OpenApiContact()
                    {
                        Email = "andre@arabesaraiva.com",
                        Name = "André Árabe Saraiva",
                        Url = new Uri("https://github.com/arabesaraiva")
                    }
                });

                o.IncludeXmlComments(System.IO.Path.Combine(PlatformServices.Default.Application.ApplicationBasePath, $"{PlatformServices.Default.Application.ApplicationName}.xml"));
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });

            app.UseSwagger();
            app.UseSwaggerUI(o=>
            {
                o.SwaggerEndpoint("/swagger/v1/swagger.json", "Demonstration API for MicroORM");
            });


        }
    }
}
