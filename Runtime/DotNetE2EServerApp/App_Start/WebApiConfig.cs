// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Web.Http;
using AutoMapper;
using Microsoft.WindowsAzure.Mobile.Service;
using Microsoft.WindowsAzure.Mobile.Service.Config;
using Microsoft.WindowsAzure.Mobile.Service.Security;
using Newtonsoft.Json;
using ZumoE2EServerApp.DataObjects;
using ZumoE2EServerApp.Models;
using ZumoE2EServerApp.Utils;

namespace ZumoE2EServerApp
{
    public static class WebApiConfig
    {
        public static void Register()
        {
            ConfigOptions options = new ConfigOptions
            {
                PushAuthorization = AuthorizationLevel.Application,
                DiagnosticsAuthorization = AuthorizationLevel.Anonymous,
            };

            HttpConfiguration config = ServiceConfig.Initialize(new ConfigBuilder(options));

            // Now add any missing connection strings and app settings from the environment.
            // Any envrionment variables found with names that match existing connection
            // string and app setting names will be used to replace the value.
            // This allows the Web.config (which typically would contain secrets) to be
            // checked in, but requires people running the tests to config their environment.
            IServiceSettingsProvider settingsProvider = config.DependencyResolver.GetServiceSettingsProvider();
            ServiceSettingsDictionary settings = settingsProvider.GetServiceSettings();
            IDictionary environmentVariables = Environment.GetEnvironmentVariables();
            foreach (var conKey in settings.Connections.Keys.ToArray())
            {
                var envKey = environmentVariables.Keys.OfType<string>().FirstOrDefault(p => p == conKey);
                if (!string.IsNullOrEmpty(envKey))
                {
                    settings.Connections[conKey].ConnectionString = (string)environmentVariables[envKey];
                }
            }

            foreach (var setKey in settings.Keys.ToArray())
            {
                var envKey = environmentVariables.Keys.OfType<string>().FirstOrDefault(p => p == setKey);
                if (!string.IsNullOrEmpty(envKey))
                {
                    settings[setKey] = (string)environmentVariables[envKey];
                }
            }

            // Emulate the auth behavior of the server: default is application unless explicitly set.
            config.Properties["MS_IsHosted"] = true;

            config.Formatters.JsonFormatter.SerializerSettings.DateFormatHandling = DateFormatHandling.IsoDateFormat;

            Mapper.Initialize(cfg =>
            {
                cfg.CreateMap<IntIdRoundTripTableItem, IntIdRoundTripTableItemDto>()
                   .ForMember(dto => dto.Id, map => map.MapFrom(db => MySqlFuncs.LTRIM(MySqlFuncs.StringConvert(db.Id))));
                cfg.CreateMap<IntIdRoundTripTableItemDto, IntIdRoundTripTableItem>()
                   .ForMember(db => db.Id, map => map.MapFrom(dto => MySqlFuncs.LongParse(dto.Id)));

                cfg.CreateMap<IntIdMovie, IntIdMovieDto>()
                   .ForMember(dto => dto.Id, map => map.MapFrom(db => MySqlFuncs.LTRIM(MySqlFuncs.StringConvert(db.Id))));
                cfg.CreateMap<IntIdMovieDto, IntIdMovie>()
                   .ForMember(db => db.Id, map => map.MapFrom(dto => MySqlFuncs.LongParse(dto.Id)));

            });

            Database.SetInitializer(new DbInitializer());
        }

        class DbInitializer : ClearDatabaseSchemaAlways<SDKClientTestContext>
        {
            protected override void Seed(SDKClientTestContext context)
            {
                // to enable some better testing scenarios for offline, we'll insert 50 records,
                // then bulk insert 50 so they have the same UpdatedAt, then continue with the rest
                var movies = TestMovies.GetTestMovies();
                foreach (var movie in movies.Take(50))
                {
                    context.Set<Movie>().Add(movie);
                }
                context.SaveChangesAsync().Wait();
                this.BulkInsertMovies(context.Database.Connection.ConnectionString, movies.Skip(50).Take(50));
                foreach (var movie in movies.Skip(100))
                {
                    context.Set<Movie>().Add(movie);
                }

                foreach (var movie in TestMovies.TestIntIdMovies)
                {
                    context.Set<IntIdMovie>().Add(movie);
                }

                base.Seed(context);
            }

            private void BulkInsertMovies(string connStr, IEnumerable<Movie> movies)
            {
                SqlBulkCopy bcp = new SqlBulkCopy(connStr, SqlBulkCopyOptions.FireTriggers);
                bcp.DestinationTableName = "[ZumoE2EServerApp].Movies";

                var table = new DataTable();
                // these columns need to be declared in the same order as declared in SQL
                table.Columns.Add("Id");
                table.Columns.Add("Title");
                table.Columns.Add("Duration");
                table.Columns.Add("MpaaRating");
                table.Columns.Add("ReleaseDate");
                table.Columns.Add("BestPictureWinner");
                table.Columns.Add("Year");
                table.Columns.Add("Version");
                table.Columns.Add("CreatedAt");
                table.Columns.Add("UpdatedAt");
                table.Columns.Add("Deleted");

                movies.Each(m =>
                {
                    var row = table.NewRow();
                    row["Id"] = m.Id;
                    row["Title"] = m.Title;
                    row["MpaaRating"] = m.MpaaRating;
                    row["Year"] = m.Year;
                    row["ReleaseDate"] = m.ReleaseDate;
                    row["Duration"] = m.Duration;
                    row["BestPictureWinner"] = m.BestPictureWinner;
                    row["Deleted"] = m.Deleted;
                    row["UpdatedAt"] = m.UpdatedAt;
                    row["Version"] = m.Version;
                    row["CreatedAt"] = DateTimeOffset.UtcNow;
                    table.Rows.Add(row);
                });

                bcp.WriteToServer(table);
            }
        }


    }
}
