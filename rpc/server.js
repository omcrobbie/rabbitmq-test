var amqp = require('amqplib/callback_api');

function fibonacci(n) {
    if (n === 0 || n === 1)
        return n;
    else
        return fibonacci(n - 1) + fibonacci(n - 2);

}
amqp.connect('amqp://localhost', function (err, conn) {
    conn.createChannel(function (err, ch) {
        var q = 'rpc';

        ch.assertQueue(q, { durable: false });
        ch.prefetch(1);
        console.log(' [x] Awaiting RPC requests');
        ch.consume(q.queue, function reply(msg) {
            const n = parseInt(msg.content.toString());
            console.log(' [.] fib(%d)', n)
            setTimeout(() => {
                var f = fibonacci(n);
                console.log('DONE: ', f)
                ch.sendToQueue(
                    msg.properties.replyTo,
                    Buffer.from(f.toString()),
                    { correlationId: msg.properties.correlationId });
                ch.ack(msg);
            }, n * 1000);

        });
    });
});