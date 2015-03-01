using System;
using System.Data;
using System.Messaging;

namespace Hangfire.Msmq.Tests
{
    internal class MsmqUtils
    {
        public static void EnqueueJobId(IDbTransaction transaction, string queue, string jobId)
        {
            using (var messageQueue = CleanMsmqQueueAttribute.GetMessageQueue(queue))
            using (var message = new Message { Body = jobId, Label = jobId, Formatter = new BinaryMessageFormatter() })
            using (var msqTransaction = new MessageQueueTransaction())
            {
                msqTransaction.Begin();
                messageQueue.Send(message, msqTransaction);
                msqTransaction.Commit();
            }
        }

        public static string DequeueJobId(string queue, TimeSpan timeout)
        {
            using (var messageQueue = CleanMsmqQueueAttribute.GetMessageQueue(queue))
            using (var transaction = new MessageQueueTransaction())
            {
                transaction.Begin();

                using (var message = messageQueue.Receive(timeout, transaction))
                {
                    message.Formatter = new BinaryMessageFormatter();
                    transaction.Commit();

                    return (string)message.Body;
                }
            }
        }
    }
}
