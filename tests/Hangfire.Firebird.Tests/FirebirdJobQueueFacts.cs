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

﻿using System;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Moq;
using FirebirdSql.Data.FirebirdClient;
using Xunit;

namespace Hangfire.Firebird.Tests
{
    public class FirebirdJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };
        private readonly FirebirdStorageOptions _options;

        public FirebirdJobQueueFacts()
        {
            _options = new FirebirdStorageOptions()
            {

            };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdJobQueue(null, new FirebirdStorageOptions()));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdJobQueue(new Mock<IDbConnection>().Object, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        private void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            string arrangeSql = string.Format(@"
                INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue)
                VALUES (@jobId, @queue) RETURNING id;", _options.Prefix);

            // Arrange
            UseConnection(connection =>
            {
                var id = connection.ExecuteScalar<int>(
                    arrangeSql,
                    new { jobId = 1, queue = "default" });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = (FirebirdFetchedJob)queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal(id, payload.Id);
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                EXECUTE BLOCK (invocationData BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @invocationData,
                    arguments BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @arguments,
                    queue VARCHAR(20) CHARACTER SET UNICODE_FSS = @queue)
                AS
                DECLARE new_id int;
                BEGIN
                    INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat) 
                    VALUES (:invocationData, :arguments, DATEADD(minute, -{1:N5}*60, current_timestamp))
                    RETURNING id INTO :new_id;
                    
                    INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue) 
                    VALUES (:new_id, :queue);
                    
                    SUSPEND;
                END", _options.Prefix, _options.UtcOffset);     

            // Arrange
            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new { invocationData = "", arguments = "", queue = "default" });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(payload);

                var fetchedAt = connection.Query<DateTime?>(
                    string.Format(@"SELECT fetchedat FROM ""{0}.JOBQUEUE"" WHERE jobid = @id", _options.Prefix),
                    new { id = Convert.ToInt32(payload.JobId, CultureInfo.InvariantCulture) }).Single();

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                EXECUTE BLOCK (invocationData BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @invocationData,
                    arguments BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @arguments,
                    queue VARCHAR(20) CHARACTER SET UNICODE_FSS = @queue,
                    fetchedAt TIMESTAMP = @fetchedAt)
                AS
                DECLARE new_id int;
                BEGIN
                    INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat) 
                    VALUES (:invocationData, :arguments, DATEADD(minute, -{1:N5}*60, current_timestamp))
                    RETURNING id INTO :new_id;
                    
                    INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue, fetchedat) 
                    VALUES (:new_id, :queue, :fetchedAt);
                    
                    SUSPEND;
                END", _options.Prefix, _options.UtcOffset);    


            // Arrange
            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new
                    {
                        queue = "default",
                        fetchedAt = DateTime.UtcNow.AddDays(-1),
                        invocationData = "",
                        arguments = ""
                    });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                EXECUTE BLOCK (invocationData BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @invocationData,
                    arguments BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @arguments,
                    queue VARCHAR(20) CHARACTER SET UNICODE_FSS = @queue)
                AS
                DECLARE new_id int;
                BEGIN
                    INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat) 
                    VALUES (:invocationData, :arguments, DATEADD(minute, -{1:N5}*60, current_timestamp))
                    RETURNING id INTO :new_id;
                    
                    INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue) 
                    VALUES (:new_id, :queue);
                    
                    SUSPEND;
                END", _options.Prefix, _options.UtcOffset);    

            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new {queue = "default", invocationData = "", arguments = ""},
                        new {queue = "default", invocationData = "", arguments = ""}
                    });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = connection.Query<DateTime?>(
                    string.Format(@"SELECT fetchedat FROM ""{0}.JOBQUEUE"" WHERE jobid != @id", _options.Prefix),
                    new { id = Convert.ToInt32(payload.JobId, CultureInfo.InvariantCulture) }).Single();

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                EXECUTE BLOCK (invocationData BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @invocationData,
                    arguments BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @arguments,
                    queue VARCHAR(20) CHARACTER SET UNICODE_FSS = @queue)
                AS
                DECLARE new_id int;
                BEGIN
                    INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat) 
                    VALUES (:invocationData, :arguments, DATEADD(minute, -{1:N5}*60, current_timestamp))
                    RETURNING id INTO :new_id;
                    
                    INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue) 
                    VALUES (:new_id, :queue);
                    
                    SUSPEND;
                END", _options.Prefix, _options.UtcOffset);  

            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                connection.Execute(
                    arrangeSql,
                    new { queue = "critical", invocationData = "", arguments = "" });

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueues()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                EXECUTE BLOCK (invocationData BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @invocationData,
                    arguments BLOB SUB_TYPE 1 SEGMENT SIZE 80 CHARACTER SET UNICODE_FSS = @arguments,
                    queue VARCHAR(20) CHARACTER SET UNICODE_FSS = @queue)
                AS
                DECLARE new_id int;
                BEGIN
                    INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat) 
                    VALUES (:invocationData, :arguments, DATEADD(minute, -{1:N5}*60, current_timestamp))
                    RETURNING id INTO :new_id;
                    
                    INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue) 
                    VALUES (:new_id, :queue);
                    
                    SUSPEND;
                END", _options.Prefix, _options.UtcOffset);  

            var queueNames = new[] { "default", "critical" };

            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { queue = queueNames.First(), invocationData = "", arguments = "" },
                        new { queue = queueNames.Last(), invocationData = "", arguments = "" }
                    });

                var queue = CreateJobQueue(connection);

                var queueFirst = (FirebirdFetchedJob)queue.Dequeue(
                    queueNames,
                    CreateTimingOutCancellationToken());

                Assert.NotNull(queueFirst.JobId);
                Assert.Contains(queueFirst.Queue, queueNames);

                var queueLast = (FirebirdFetchedJob)queue.Dequeue(
                    queueNames,
                    CreateTimingOutCancellationToken());

                Assert.NotNull(queueLast.JobId);
                Assert.Contains(queueLast.Queue, queueNames);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                IDbTransaction trans = connection.BeginTransaction();
                queue.Enqueue(trans, "default", "1");

                var record = connection.Query(string.Format(@"SELECT * FROM ""{0}.JOBQUEUE""", _options.Prefix), null, trans).Single();
                trans.Commit();
                Assert.Equal("1", record.JOBID.ToString());
                Assert.Equal("default", record.QUEUE);
                Assert.Null(record.FETCHEDAT);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        public static void Sample(string arg1, string arg2) { }

        private static FirebirdJobQueue CreateJobQueue(IDbConnection connection)
        {
            return new FirebirdJobQueue(connection, new FirebirdStorageOptions());
        }

        private static void UseConnection(Action<FbConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
