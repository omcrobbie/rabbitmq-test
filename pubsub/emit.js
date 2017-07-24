const amqp = require('amqplib/callback_api');
const readline = require('readline')
    .createInterface({
        input: process.stdin,
        output: process.stdout
    });

amqp.connect('amqp://localhost', (err, conn) => {
    conn.createChannel((e, ch) => {
        const ex = 'logs';
        ch.assertExchange(ex, 'fanout', {durable:false});
        prompt();
        function prompt() {
            readline.question('New task:', input => {
                ch.publish(ex, '', Buffer.from(input));
                prompt();
            });
        }
        // readline.on('line', ()=> prompt());
    });
});