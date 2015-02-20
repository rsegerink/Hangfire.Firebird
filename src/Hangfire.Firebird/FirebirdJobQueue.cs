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
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Firebird.Annotations;
using Hangfire.Storage;
using FirebirdSql.Data.FirebirdClient;

namespace Hangfire.Firebird
{
    internal class FirebirdJobQueue : IPersistentJobQueue
    {
        private readonly FirebirdStorageOptions _options;
        private readonly IDbConnection _connection;

        public FirebirdJobQueue(IDbConnection connection, FirebirdStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException("options");
            if (connection == null) throw new ArgumentNullException("connection");

            _options = options;
            _connection = connection;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            long timeoutSeconds = (long)_options.InvisibilityTimeout.Negate().TotalSeconds;
            FetchedJob fetchedJob;

            string fetchJobSqlTemplate = @"
                EXECUTE BLOCK
                RETURNS (""Id"" int, ""JobId"" int, ""Queue"" VARCHAR(20) CHARACTER SET UNICODE_FSS, ""FetchedAt"" TIMESTAMP)
                AS
                BEGIN
                    UPDATE """ + _options.Prefix + @".JOBQUEUE"" 
                    SET fetchedat = " + string.Format(CultureInfo.InvariantCulture, @"DATEADD(minute, -{0:N5}*60, current_timestamp)", _options.UtcOffset) + @"
                    WHERE id IN (
                        SELECT id 
                        FROM """ + _options.Prefix + @".JOBQUEUE""  
                        WHERE queue IN ('" + string.Join("','", queues) + @"') 
                        AND fetchedat {0} 
                        ORDER BY fetchedat, jobid
                        ROWS 1
                    )
                    RETURNING id, jobid, queue, fetchedat into :""Id"", :""JobId"", :""Queue"", :""FetchedAt"";
                    SUSPEND;
                END";

            var fetchConditions = new[] { "IS NULL", string.Format(CultureInfo.InvariantCulture, "< DATEADD(second, {0}, DATEADD(minute, -{1:N5}*60, current_timestamp))", _options.InvisibilityTimeout./*Negate().*/TotalSeconds, _options.UtcOffset) };
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                string fetchJobSql = string.Format(fetchJobSqlTemplate, fetchConditions[currentQueryIndex]);

                Utils.TryExecute(() =>
                {
                    using (var trx = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        var jobToFetch = _connection.Query<FetchedJob>(
                            fetchJobSql,
                            new { queues = queues.ToList() }, trx)
                            .SingleOrDefault();

                        trx.Commit();

                        return jobToFetch;
                    }
                },
                    out fetchedJob,
                    ex => ex is FbException && (ex as FbException).SQLSTATE == "40001"); //serialization failure, deadlock

                if (fetchedJob.Id == -1)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            } while (fetchedJob.Id == -1);

            return new FirebirdFetchedJob(
                _connection,
                _options,
                fetchedJob.Id,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }

        public void Enqueue(IDbTransaction transaction, string queue, string jobId)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");

            string enqueueJobSql = string.Format(@"
                INSERT INTO ""{0}.JOBQUEUE"" (jobid, queue) 
                VALUES (@jobId, @queue);", _options.Prefix);

            _connection.Execute(enqueueJobSql, new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue }, transaction);
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private class FetchedJob
        {
            public FetchedJob()
            {
                Id = -1;
            }

            public int Id { get; set; }
            public int JobId { get; set; }
            public string Queue { get; set; }
            public DateTime? FetchedAt { get; set; }
        }
    }
}
