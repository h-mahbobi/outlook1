using Confluent.Kafka;
using Microsoft.VisualBasic;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Producer
{
    /// <summary>
    /// Package :
    ///	  <PackageReference Include="Confluent.Kafka" Version="2.2.0" />
    /// </summary>
    public class KafkaUtils
    {
        public string Hostname { get; }
        public string Topic { get; }
        public string GroupId { get; }
        private ConsumerConfig consumerConfig { get; }
        public KafkaUtils(string hostname = "localhost:29092", string groupid = "group1", string topic = "users")
        {
            Hostname = hostname;
            GroupId=groupid;
            Topic = topic; 
        }


        /// <summary>
        /// usage :
        ///  new RabbitUtils().Recive(f => { Console.WriteLine(f); return ""; });        
        /// </summary>
        /// <param name="RecivedAct"></param>
        public void Recive(Func<string, string> RecivedAct)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = Hostname,
                GroupId = GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            consumer.Subscribe(Topic);
            try
            {
                while (true)
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (result == null)
                    {
                        continue;
                    }
                    RecivedAct(result.TopicPartitionOffset + " : "+result.Message.Value);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }



        public void Send(string msg)
        {
            var config = new ProducerConfig { BootstrapServers = Hostname};

            using var producer = new ProducerBuilder<Null, string>(config).Build();
            producer.Produce(Topic, new Message<Null, string> { Value = msg });
            producer.Flush(TimeSpan.FromSeconds(10));
           
        }
    }
}
