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
    public class FirebirdFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        private readonly Mock<IDbConnection> _connection;
        private readonly FirebirdStorageOptions _options;

        public FirebirdFetchedJobFacts()
        {
            _connection = new Mock<IDbConnection>();
            _options = new FirebirdStorageOptions();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdFetchedJob(null, _options, 1, JobId, Queue));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdFetchedJob(_connection.Object, null, 1, JobId, Queue));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdFetchedJob(_connection.Object, _options, 1, null, Queue));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdFetchedJob(_connection.Object, _options, 1, JobId, null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var fetchedJob = new FirebirdFetchedJob(_connection.Object, _options, 1, JobId, Queue);

            Assert.Equal(1, fetchedJob.Id);
            Assert.Equal(JobId, fetchedJob.JobId);
            Assert.Equal(Queue, fetchedJob.Queue);
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, _options, "1", "default");
                var processingJob = new FirebirdFetchedJob(connection, _options, id, "1", "default");

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = connection.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.JOBQUEUE""", _options.Prefix)).Single();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseConnection(connection =>
            {
                // Arrange
                CreateJobQueueRecord(connection, _options, "1", "default");
                CreateJobQueueRecord(connection, _options, "1", "critical");
                CreateJobQueueRecord(connection, _options, "2", "default");

                var fetchedJob = new FirebirdFetchedJob(connection, _options, 999, "1", "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = connection.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.JOBQUEUE""", _options.Prefix)).Single();
                Assert.Equal(3, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, _options, "1", "default");
                var processingJob = new FirebirdFetchedJob(connection, _options, id, "1", "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.Query(string.Format(@"SELECT * FROM ""{0}.JOBQUEUE""", _options.Prefix)).Single();
                Assert.Null(record.FETCHEDAT);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, _options, "1", "default");
                var processingJob = new FirebirdFetchedJob(connection, _options, id, "1", "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.Query(string.Format(@"SELECT * FROM ""{0}.JOBQUEUE""", _options.Prefix)).Single();
                Assert.Null(record.FETCHEDAT);
            });
        }

        private static int CreateJobQueueRecord(IDbConnection connection, FirebirdStorageOptions options, string jobId, string queue)
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue, fetchedat)
                VALUES (@id, @queue, DATEADD(minute, -{1:N5}*60, current_timestamp)) RETURNING id", options.Prefix, options.UtcOffset);

            return connection.ExecuteScalar<int>(arrangeSql, new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue });
        }

        private static void UseConnection(Action<IDbConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
