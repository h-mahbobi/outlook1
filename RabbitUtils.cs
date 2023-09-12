using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Producer
{
    public  class RabbitUtils
    {
        public string Hostname { get; }
        public string ExchangeName { get; }
        public string RouteKey { get; }
        private ConnectionFactory cnsfactory { get; }
        public RabbitUtils(string hostname = "localhost", string exchangeName = "ex", string routeKey = "key")
        {
            Hostname = hostname;
            ExchangeName = exchangeName;
            RouteKey = routeKey;

            cnsfactory = new ConnectionFactory()
            {
                HostName = hostname,
            };
        }


        /// <summary>
        /// usage :
        ///  new RabbitUtils().Recive(f => { Console.WriteLine(f); return ""; });        
    /// </summary>
    /// <param name="RecivedAct"></param>
    public  void  Recive(Func<string,string> RecivedAct)
        {
            //string Hostname = "localhost";
            //string ExchangeName = "ex";
            //string RouteKey = "key";

            //var cnsfactory = new ConnectionFactory()
            //{
            //    HostName = Hostname,
            //};

            using var cns = cnsfactory.CreateConnection();
            using var chanell = cns.CreateModel();

            chanell.ExchangeDeclare(ExchangeName, ExchangeType.Topic, false, false, null);

            var queueName = chanell.QueueDeclare().QueueName;
            chanell.QueueBind(queueName, ExchangeName, RouteKey, null);


            var consumer = new EventingBasicConsumer(chanell);
            consumer.Received += Consumer_Received;
            chanell.BasicConsume(queueName,true, consumer); // can true auto ack
            while (true)
            {
                Thread.Sleep(1000);
            }
            void Consumer_Received(object? sender, BasicDeliverEventArgs e)
            {

                var body = e.Body.ToArray();
                var stringmsg = System.Text.Encoding.UTF8.GetString(body);
                //chanell.BasicAck(e.DeliveryTag, false); // not need to set ack if auto ack true;
                RecivedAct(stringmsg);
            }

        }


        /// <summary>
        /// usage :
        /// RabbitUtils r = new RabbitUtils();
        ///  r.Send("hello world");
        /// </summary>
        /// <param name="msg"></param>
        public void Send(string msg)
        {
            var cnsfactory = new ConnectionFactory()
            {
                HostName = this.Hostname,
            };

            using var cns = cnsfactory.CreateConnection();
            using var chanell = cns.CreateModel();

            chanell.ExchangeDeclare(this.ExchangeName, ExchangeType.Topic, false, false, null);

            var messageByte = System.Text.Encoding.UTF8.GetBytes(msg).ToArray();
            chanell.BasicPublish(this.ExchangeName, this.RouteKey, null, messageByte);

        }
    }
}
