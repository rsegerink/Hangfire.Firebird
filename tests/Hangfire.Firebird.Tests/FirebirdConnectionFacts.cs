// This file is part of Hangfire.Firebird

// Copyright © 2015 Rob Segerink <https://github.com/rsegerink/Hangfire.Firebird>.
// 
// Hangfire.Firebird is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.Firebird is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.Firebird. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using FirebirdSql.Data.FirebirdClient;
using Xunit;

namespace Hangfire.Firebird.Tests
{
    public class FirebirdConnectionFacts
    {
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly PersistentJobQueueProviderCollection _providers;
        private readonly FirebirdStorageOptions _options;

        public FirebirdConnectionFacts()
        {
            _queue = new Mock<IPersistentJobQueue>();

            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue(It.IsNotNull<IDbConnection>()))
                .Returns(_queue.Object);

            _providers = new PersistentJobQueueProviderCollection(provider.Object);

            _options = new FirebirdStorageOptions()
            {
                
            };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdConnection(null, _providers, _options));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdConnection(ConnectionUtils.CreateConnection(), null, _options));

            Assert.Equal("queueProviders", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdConnection(ConnectionUtils.CreateConnection(), _providers, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dispose_DisposesTheConnection_IfOwned()
        {
            using (var sqlConnection = ConnectionUtils.CreateConnection())
            {
                var connection = new FirebirdConnection(sqlConnection, _providers, _options);

                connection.Dispose();

                Assert.Equal(ConnectionState.Closed, sqlConnection.State);
            }
        }

        [Fact, CleanDatabase]
        public void Dispose_DoesNotDisposeTheConnection_IfNotOwned()
        {
            using (var sqlConnection = ConnectionUtils.CreateConnection())
            {
                var connection = new FirebirdConnection(sqlConnection, _providers, ownsConnection: false, options: _options);

                connection.Dispose();

                Assert.Equal(ConnectionState.Open, sqlConnection.State);
            }
        }


        [Fact, CleanDatabase]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            UseConnection(connection =>
            {
                var token = new CancellationToken();
                var queues = new[] { "default" };

                connection.FetchNextJob(queues, token);

                _queue.Verify(x => x.Dequeue(queues, token));
            });
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_Throws_IfMultipleProvidersResolved()
        {
            UseConnection(connection =>
            {
                var token = new CancellationToken();
                var anotherProvider = new Mock<IPersistentJobQueueProvider>();
                _providers.Add(anotherProvider.Object, new[] { "critical" });

                Assert.Throws<InvalidOperationException>(
                    () => connection.FetchNextJob(new[] { "critical", "default" }, token));
            });
        }

        [Fact, CleanDatabase]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            UseConnection(connection =>
            {
                var transaction = connection.CreateWriteTransaction();
                Assert.NotNull(transaction);
            });
        }

        [Fact, CleanDatabase]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            UseConnection(connection =>
            {
                var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
                Assert.NotNull(@lock);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        null,
                        new Dictionary<string, string>(),
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("job", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        Job.FromExpression(() => SampleMethod("hello")),
                        null,
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("parameters", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            UseConnections((sql, connection) =>
            {
                var createdAt = new DateTime(2012, 12, 12);
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethod("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);
                Assert.NotEmpty(jobId);

                var sqlJob = sql.Query(string.Format(@"select * from ""{0}.JOB""", _options.Prefix)).Single();
                Assert.Equal(jobId, sqlJob.ID.ToString());
                Assert.Equal(createdAt, sqlJob.CREATEDAT);
                Assert.Equal(null, (int?)sqlJob.STATEID);
                Assert.Equal(null, (string)sqlJob.STATENAME);

                var invocationData = JobHelper.FromJson<InvocationData>((string)sqlJob.INVOCATIONDATA);
                invocationData.Arguments = sqlJob.ARGUMENTS;

                var job = invocationData.Deserialize();
                Assert.Equal(typeof(FirebirdConnectionFacts), job.Type);
                Assert.Equal("SampleMethod", job.Method.Name);
                Assert.Equal("\"Hello\"", job.Arguments[0]);

                Assert.True(createdAt.AddDays(1).AddMinutes(-1) < sqlJob.EXPIREAT);
                Assert.True(sqlJob.EXPIREAT < createdAt.AddDays(1).AddMinutes(1));

                var parameters = sql.Query(
                    string.Format(@"select * from ""{0}.JOBPARAMETER"" where jobid = @id", _options.Prefix),
                    new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) })
                    .ToDictionary(x => (string)x.NAME, x => (string)x.VALUE);

                Assert.Equal("Value1", parameters["Key1"]);
                Assert.Equal("Value2", parameters["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobData(null)));
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseConnection(connection =>
            {
                var result = connection.GetJobData("1");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                insert into ""{0}.JOB"" (invocationdata, arguments, statename, createdat)
                values (@invocationData, @arguments, @stateName, DATEADD(minute, -{1:N5}*60, current_timestamp)) returning id;", _options.Prefix, _options.UtcOffset);

            UseConnections((sql, connection) =>
            {
                var job = Job.FromExpression(() => SampleMethod("wrong"));

                var jobId = sql.ExecuteScalar<int>(
                    arrangeSql,
                    new
                    {
                        invocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                        stateName = "Succeeded",
                        arguments = "['Arguments']"
                    }).ToString();

                var result = connection.GetJobData(jobId);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Succeeded", result.State);
                Assert.Equal("Arguments", result.Job.Arguments[0]);
                Assert.Null(result.LoadException);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                connection => Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null)));
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            UseConnection(connection =>
            {
                var result = connection.GetStateData("1");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsCorrectData()
        {
            string createJobSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, statename, createdat)
                VALUES ('', '', '', DATEADD(minute, -{1:N5}*60, current_timestamp)) RETURNING id;", _options.Prefix, _options.UtcOffset);

            string createState1Sql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.STATE"" (jobid, name, createdat)
                VALUES(@jobId, 'old-state', DATEADD(minute, -{1:N5}*60, current_timestamp));", _options.Prefix, _options.UtcOffset);

            string createState2Sql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.STATE"" (jobid, name, reason, data, createdat)
                VALUES(@jobId, @name, @reason, @data, DATEADD(minute, -{1:N5}*60, current_timestamp))
                RETURNING id;", _options.Prefix, _options.UtcOffset);

            string updateJobStateSql = string.Format(@"
                UPDATE ""{0}.JOB""
                SET stateid = @stateId
                WHERE id = @jobId;", _options.Prefix);

            UseConnections((sql, connection) =>
            {
                var data = new Dictionary<string, string>
                {
                    { "Key", "Value" }
                };

                var jobId = sql.ExecuteScalar<int>(createJobSql);

                sql.ExecuteScalar(
                    createState1Sql,
                    new { jobId = jobId });

                var stateId = sql.ExecuteScalar<int>(
                    createState2Sql,
                    new { jobId = jobId, name = "Name", reason = "Reason", @data = JobHelper.ToJson(data) }); 

                sql.Execute(updateJobStateSql, new { jobId = jobId, stateId = stateId });

                var result = connection.GetStateData(jobId.ToString(CultureInfo.InvariantCulture));
                Assert.NotNull(result);

                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, statename, createdat)
                VALUES (@invocationData, @arguments, @stateName, DATEADD(minute, -{1:N5}*60, current_timestamp)) RETURNING id", _options.Prefix, _options.UtcOffset);

            UseConnections((sql, connection) =>
            {
                var jobId = sql.ExecuteScalar<int>(
                    arrangeSql,
                    new
                    {
                        invocationData = JobHelper.ToJson(new InvocationData(null, null, null, null)),
                        stateName = "Succeeded",
                        arguments = "['Arguments']"
                    });

                var result = connection.GetJobData(jobId.ToString());

                Assert.NotNull(result.LoadException);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("1", null, "value"));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat)
                VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp)) RETURNING id", _options.Prefix, _options.UtcOffset);

            UseConnections((sql, connection) =>
            {
                var job = sql.ExecuteScalar<int>(arrangeSql);
                string jobId = job.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");

                var parameter = sql.Query(string.Format(@"
                    SELECT * FROM ""{0}.JOBPARAMETER"" WHERE jobid = @id AND name = @name;", _options.Prefix),
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal("Value", parameter.VALUE);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat)
                VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp)) RETURNING id", _options.Prefix, _options.UtcOffset);

            UseConnections((sql, connection) =>
            {
                var job = sql.ExecuteScalar<int>(arrangeSql);
                string jobId = job.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                var parameter = sql.Query(string.Format(@"
                    SELECT * FROM ""{0}.JOBPARAMETER"" WHERE jobid = @id AND name = @name;", _options.Prefix),
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal("AnotherValue", parameter.VALUE);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat)
                VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp)) RETURNING id", _options.Prefix, _options.UtcOffset);

            UseConnections((sql, connection) =>
            {
                var job = sql.ExecuteScalar<int>(arrangeSql);
                string jobId = job.ToString();

                connection.SetJobParameter(jobId, "Name", null);

                var parameter = sql.Query(string.Format(@"
                    SELECT * FROM ""{0}.JOBPARAMETER"" WHERE jobid = @id AND name = @name;", _options.Prefix),
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal((string)null, parameter.value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter(null, "hello"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter("1", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            UseConnection(connection =>
            {
                var value = connection.GetJobParameter("1", "hello");
                Assert.Null(value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                EXECUTE BLOCK (name varchar(40) = @name, ""value"" BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @value)
                RETURNS (id int)
                AS
                BEGIN
                   INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat)
                   VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp))
                   RETURNING id INTO :id;

                   INSERT INTO ""{0}.JOBPARAMETER"" (jobid, name, ""VALUE"")
                   VALUES (:id, :name, :""value"");

                   SUSPEND;
                END", _options.Prefix, _options.UtcOffset);
                
            UseConnections((sql, connection) =>
            {
                var id = sql.Query<int>(
                    arrangeSql,
                    new { name = "name", value = "value" }).Single();

                var value = connection.GetJobParameter(Convert.ToString(id, CultureInfo.InvariantCulture), "name");

                Assert.Equal("value", value);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.GetFirstByLowestScoreFromSet("key", 0, -1)));
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetFirstByLowestScoreFromSet(
                    "key", 0, 1);

                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            string arrangeSql = string.Format(@"
                EXECUTE BLOCK AS
                BEGIN
                    INSERT INTO ""{0}.SET"" (""KEY"", score, ""VALUE"") VALUES ('key', 1.0, '1.0');
                    INSERT INTO ""{0}.SET"" (""KEY"", score, ""VALUE"") VALUES ('key', -1.0, '-1.0');
                    INSERT INTO ""{0}.SET"" (""KEY"", score, ""VALUE"") VALUES ('key', -5.0, '-5.0');
                    INSERT INTO ""{0}.SET"" (""KEY"", score, ""VALUE"") VALUES ('another-key', -2.0, '-2.0');
                    SUSPEND;
                END", _options.Prefix);                

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

                Assert.Equal("-1.0", result);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer(null, new ServerContext()));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer("server", null));

                Assert.Equal("context", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            UseConnections((sql, connection) =>
            {
                var context1 = new ServerContext
                {
                    Queues = new[] { "critical", "default" },
                    WorkerCount = 4
                };
                connection.AnnounceServer("server", context1);

                var server = sql.Query(string.Format(@"SELECT * FROM ""{0}.SERVER""", _options.Prefix)).Single();
                Assert.Equal("server", server.ID);
                Assert.True(((string)server.DATA).StartsWith(
                    "{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":"),
                    server.DATA);
                Assert.NotNull(server.LASTHEARTBEAT);

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);
                var sameServer = sql.Query(string.Format(@"SELECT * FROM ""{0}.SERVER""", _options.Prefix)).Single();
                Assert.Equal("server", sameServer.ID);
                Assert.Contains("1000", sameServer.DATA);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.RemoveServer(null)));
        }

        [Fact, CleanDatabase]
        public void RemoveServer_RemovesAServerRecord()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                EXECUTE BLOCK AS
                BEGIN
                    INSERT INTO ""{0}.SERVER"" (id, data, lastheartbeat) VALUES ('Server1', '', DATEADD(minute, -{1:N5}*60, current_timestamp));
                    INSERT INTO ""{0}.SERVER"" (id, data, lastheartbeat) VALUES ('Server2', '', DATEADD(minute, -{1:N5}*60, current_timestamp));
                    SUSPEND;
                END", _options.Prefix, _options.UtcOffset);   

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.RemoveServer("Server1");

                var server = sql.Query(string.Format(@"SELECT * FROM ""{0}.SERVER""", _options.Prefix)).Single();
                Assert.NotEqual("Server1", server.ID, StringComparer.OrdinalIgnoreCase);
            });
        }

        [Fact, CleanDatabase]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.Heartbeat(null)));
        }

        [Fact, CleanDatabase]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            string arrangeSql = string.Format(@"
                EXECUTE BLOCK AS
                BEGIN
                    INSERT INTO ""{0}.SERVER"" (id, data, lastheartbeat) VALUES ('server1', '', '2012-12-12 12:12:12');
                    INSERT INTO ""{0}.SERVER"" (id, data, lastheartbeat) VALUES ('server2', '', '2012-12-12 12:12:12');
                    SUSPEND;
                END", _options.Prefix);   

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.Heartbeat("server1");

                var servers = sql.Query(string.Format(@"SELECT * FROM ""{0}.SERVER""", _options.Prefix))
                    .ToDictionary(x => (string)x.ID, x => (DateTime)x.LASTHEARTBEAT);

                Assert.NotEqual(2012, servers["server1"].Year);
                Assert.Equal(2012, servers["server2"].Year);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5))));
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            string arrangeSql = string.Format(@"
                INSERT INTO ""{0}.SERVER"" (id, data, lastheartbeat)
                VALUES (@id, '', @heartbeat)", _options.Prefix);

            UseConnections((sql, connection) =>
            {
                sql.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { id = "server1", heartbeat = DateTime.UtcNow.AddDays(-1) },
                        new { id = "server2", heartbeat = DateTime.UtcNow.AddHours(-12) }
                    });

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                var liveServer = sql.Query(string.Format(@"SELECT * FROM ""{0}.SERVER""", _options.Prefix)).Single();
                Assert.Equal("server2", liveServer.ID);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Equal(0, result.Count);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            string arrangeSql = string.Format(@"
                INSERT INTO ""{0}.SET"" (""KEY"", score, ""VALUE"")
                VALUES (@key, 0.0, @value)", _options.Prefix);

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "some-set", value = "1" },
                    new { key = "some-set", value = "2" },
                    new { key = "another-set", value = "3" }
                });

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnections((sql, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var result = sql.Query(string.Format(@"SELECT * FROM ""{0}.HASH"" WHERE ""KEY"" = @key;", _options.Prefix), 
                    new { key = "some-hash" })
                    .ToDictionary(x => (string)x.FIELD, x => (string)x.VALUE);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            string arrangeSql = string.Format(@"
                INSERT INTO ""{0}.HASH"" (""KEY"", field, ""VALUE"")
                VALUES (@key, @field, @value);", _options.Prefix);

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "some-hash", field = "Key1", value = "Value1" },
                    new { key = "some-hash", field = "Key2", value = "Value2" },
                    new { key = "another-hash", field = "Key3", value = "Value3" }
                });

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        private void UseConnections(Action<FbConnection, FirebirdConnection> action)
        {
            using (var sqlConnection = ConnectionUtils.CreateConnection())
            using (var connection = new FirebirdConnection(sqlConnection, _providers, _options))
            {
                action(sqlConnection, connection);
            }
        }

        private void UseConnection(Action<FirebirdConnection> action)
        {
            using (var connection = new FirebirdConnection(
                ConnectionUtils.CreateConnection(),
                _providers,
                _options))
            {
                action(connection);
            }
        }

        public static void SampleMethod(string arg) { }
    }
}
