const amqp = require('amqplib/callback_api');
const readline = require('readline')
    .createInterface({
        input: process.stdin,
        output: process.stdout
    });

amqp.connect('amqp://localhost', (err, conn) => {
    conn.createChannel((e, ch) => {
        const q = 'task_queue';
        ch.assertQueue(q, { durable: true });
        prompt();
        function prompt() {
            readline.question('New task:', input => {
                ch.sendToQueue(q, Buffer.from(input), { persistent: true });
                prompt();
            });
        }
        // readline.on('line', ()=> prompt());
    });
});