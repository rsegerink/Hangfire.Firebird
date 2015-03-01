using Hangfire;
using Hangfire.Dashboard;
using Hangfire.Firebird;
using Hangfire.Firebird.Msmq;
using Microsoft.Owin;
using Owin;

[assembly: OwinStartup(typeof(NancySample.Startup))]

namespace NancySample
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            app.UseHangfire(config =>
            {
                //use Firebird embedded with MSMQ
                config
                    .UseFirebirdStorage(@"User=SYSDBA;Password=masterkey;Database=S:\Source\Hangfire.Firebird\HANGFIRE_SAMPLE.FDB;Packet Size=8192;DataSource=localhost;Port=3050;Dialect=3;Charset=NONE;ServerType=1;ClientLibrary=S:\Source\Hangfire.Firebird\Firebird\fbembed.dll;")
                    .UseMsmqQueues(@".\private$\hangfire-{0}");
                config.UseServer();
            });

            app.UseNancy();

            RecurringJob.AddOrUpdate(
                () => TextBuffer.WriteLine("Recurring Job completed successfully!"),
                Cron.Minutely);
        }
    }
}