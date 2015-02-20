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
﻿
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.States;
using Moq;
using FirebirdSql.Data.FirebirdClient;
using Xunit;

namespace Hangfire.Firebird.Tests
{
    public class FirebirdWriteOnlyTransactionFacts
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly FirebirdStorageOptions _options;

        public FirebirdWriteOnlyTransactionFacts()
        {
            var defaultProvider = new Mock<IPersistentJobQueueProvider>();
            defaultProvider.Setup(x => x.GetJobQueue(It.IsNotNull<IDbConnection>()))
                .Returns(new Mock<IPersistentJobQueue>().Object);

            _queueProviders = new PersistentJobQueueProviderCollection(defaultProvider.Object);
            _options = new FirebirdStorageOptions();
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdWriteOnlyTransaction(null, _options, _queueProviders));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdWriteOnlyTransaction(ConnectionUtils.CreateConnection(), null, _queueProviders));

            Assert.Equal("options", exception.ParamName);
        }


        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FirebirdWriteOnlyTransaction(ConnectionUtils.CreateConnection(), _options, null));

            Assert.Equal("queueProviders", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void ExpireJob_SetsJobExpirationData()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat)
                VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp)) 
                RETURNING id;", _options.Prefix, _options.UtcOffset);

            UseConnection(sql =>
            {
                var jobId = sql.ExecuteScalar<int>(arrangeSql).ToString();
                var anotherJobId = sql.ExecuteScalar<int>(arrangeSql).ToString(); 

                Commit(sql, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

                var job = GetTestJob(sql, _options, jobId);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < job.EXPIREAT && job.EXPIREAT <= DateTime.UtcNow.AddDays(1));

                var anotherJob = GetTestJob(sql, _options, anotherJobId);
                Assert.Null(anotherJob.EXPIREAT);
            });
        }

        [Fact, CleanDatabase]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat, expireat)
                VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp), DATEADD(minute, -{1:N5}*60, current_timestamp)) 
                RETURNING id;", _options.Prefix, _options.UtcOffset);

            UseConnection(sql =>
            {
                var jobId = sql.ExecuteScalar<int>(arrangeSql).ToString();
                var anotherJobId = sql.ExecuteScalar<int>(arrangeSql).ToString(); 

                Commit(sql, x => x.PersistJob(jobId));

                var job = GetTestJob(sql, _options, jobId);
                Assert.Null(job.EXPIREAT);

                var anotherJob = GetTestJob(sql, _options, anotherJobId);
                Assert.NotNull(anotherJob.EXPIREAT);
            });
        }

        [Fact, CleanDatabase]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat)
                VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp)) 
                RETURNING id;", _options.Prefix, _options.UtcOffset);

            UseConnection(sql =>
            {
                var jobId = sql.ExecuteScalar<int>(arrangeSql).ToString();
                var anotherJobId = sql.ExecuteScalar<int>(arrangeSql).ToString(); 

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(sql, x => x.SetJobState(jobId, state.Object));

                var job = GetTestJob(sql, _options, jobId);
                Assert.Equal("State", job.STATENAME);
                Assert.NotNull(job.STATEID);

                var anotherJob = GetTestJob(sql, _options, anotherJobId);
                Assert.Null(anotherJob.STATENAME);
                Assert.Null(anotherJob.STATEID);

                var jobState = sql.Query(string.Format(@"SELECT * FROM ""{0}.STATE""", _options.Prefix)).Single();
                Assert.Equal((string)jobId, jobState.JOBID.ToString());
                Assert.Equal("State", jobState.NAME);
                Assert.Equal("Reason", jobState.REASON);
                Assert.NotNull(jobState.CREATEDAT);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.DATA);
            });
        }

        [Fact, CleanDatabase]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            string arrangeSql = string.Format(CultureInfo.InvariantCulture, @"
                INSERT INTO ""{0}.JOB"" (invocationdata, arguments, createdat)
                VALUES ('', '', DATEADD(minute, -{1:N5}*60, current_timestamp)) 
                RETURNING id;", _options.Prefix, _options.UtcOffset);

            UseConnection(sql =>
            {
                var jobId = sql.ExecuteScalar<int>(arrangeSql).ToString(CultureInfo.InvariantCulture);

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(sql, x => x.AddJobState(jobId, state.Object));

                var job = GetTestJob(sql, _options, jobId);
                Assert.Null(job.STATENAME);
                Assert.Null(job.STATEID);

                var jobState = sql.Query(string.Format(@"SELECT * FROM ""{0}.STATE""", _options.Prefix)).Single();
                Assert.Equal((string)jobId, jobState.JOBID.ToString(CultureInfo.InvariantCulture));
                Assert.Equal("State", jobState.NAME);
                Assert.Equal("Reason", jobState.REASON);
                Assert.NotNull(jobState.CREATEDAT);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.DATA);
            });
        }

        /*[Fact, CleanDatabase]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            UseConnection(sql =>
            {
                var correctJobQueue = new Mock<IPersistentJobQueue>();
                var correctProvider = new Mock<IPersistentJobQueueProvider>();
                correctProvider.Setup(x => x.GetJobQueue(It.IsNotNull<IDbConnection>()))
                    .Returns(correctJobQueue.Object);

                _queueProviders.Add(correctProvider.Object, new[] { "default" });

                Commit(sql, x => x.AddToQueue("default", "1"));

                IDbTransaction trans = sql.BeginTransaction(); 
                correctJobQueue.Verify(x => x.Enqueue(trans, "default", "1"));
                trans.Commit();
            });
        }*/

        private static dynamic GetTestJob(IDbConnection connection, FirebirdStorageOptions options, string jobId)
        {
            return connection
                .Query(string.Format(@"SELECT * FROM ""{0}.JOB"" WHERE id = @id", options.Prefix), new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) })
                .Single();
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.IncrementCounter("my-key"));

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.COUNTER""", _options.Prefix)).Single();

                Assert.Equal("my-key", record.KEY);
                Assert.Equal(1, record.VALUE);
                Assert.Equal((DateTime?)null, record.EXPIREAT);
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.COUNTER""", _options.Prefix)).Single();

                Assert.Equal("my-key", record.KEY);
                Assert.Equal(1, record.VALUE);
                Assert.NotNull(record.EXPIREAT);

                var expireAt = (DateTime)record.EXPIREAT;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.COUNTER""", _options.Prefix)).Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.DecrementCounter("my-key"));

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.COUNTER""", _options.Prefix)).Single();

                Assert.Equal("my-key", record.KEY);
                Assert.Equal(-1, record.VALUE);
                Assert.Equal((DateTime?)null, record.EXPIREAT);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.COUNTER""", _options.Prefix)).Single();

                Assert.Equal("my-key", record.KEY);
                Assert.Equal(-1, record.VALUE);
                Assert.NotNull(record.EXPIREAT);

                var expireAt = (DateTime)record.EXPIREAT;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.DecrementCounter("my-key");
                    x.DecrementCounter("my-key");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.COUNTER""", _options.Prefix)).Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.AddToSet("my-key", "my-value"));

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal("my-key", record.KEY);
                Assert.Equal("my-value", record.VALUE);
                Assert.Equal(0.0, record.SCORE, 2);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.AddToSet("my-key", "my-value", 3.2));

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal("my-key", record.KEY);
                Assert.Equal("my-value", record.VALUE);
                Assert.Equal(3.2, record.SCORE, 3);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal(3.2, record.SCORE, 3);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.SET""", _options.Prefix)).Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.InsertToList("my-key", "my-value"));

                var record = sql.Query(string.Format(@"SELECT * FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal("my-key", record.KEY);
                Assert.Equal("my-value", record.VALUE);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_TrimsAList_ToASpecifiedRange()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                var records = sql.Query(string.Format(@"SELECT * FROM ""{0}.LIST""", _options.Prefix)).ToArray();

                Assert.Equal(2, records.Length);
                Assert.Equal("1", records[0].VALUE);
                Assert.Equal("2", records[1].VALUE);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.LIST""", _options.Prefix)).Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.SetRangeInHash("some-hash", null)));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                var result = sql.Query(string.Format(@"
                    SELECT * FROM ""{0}.HASH"" WHERE ""KEY"" = @key;", _options.Prefix),
                    new { key = "some-hash" })
                    .ToDictionary(x => (string)x.FIELD, x => (string)x.VALUE);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_RemovesAllHashRecords()
        {
            UseConnection(sql =>
            {
                // Arrange
                Commit(sql, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                // Act
                Commit(sql, x => x.RemoveHash("some-hash"));

                // Assert
                var count = sql.Query<long>(string.Format(@"SELECT COUNT(*) FROM ""{0}.HASH""", _options.Prefix)).Single();
                Assert.Equal(0, count);
            });
        }

        private void UseConnection(Action<FbConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }

        private void Commit(
            FbConnection connection,
            Action<FirebirdWriteOnlyTransaction> action)
        {
            using (var transaction = new FirebirdWriteOnlyTransaction(connection, _options, _queueProviders))
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
}
