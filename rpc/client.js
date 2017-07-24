const amqp = require('amqplib/callback_api');
const readline = require('readline')
    .createInterface({
        input: process.stdin,
        output: process.stdout
    });

amqp.connect('amqp://localhost', (err, conn) => {
    let ids = [];
    conn.createChannel((e, ch) => {
        prompt();
        function prompt() {
            readline.question('New task:', input => {
                if (input === '')
                    prompt();
                const id = genuuid();
                const n = input;
                ids.push(id); 
                ch.assertQueue('', { exclusive: true }, (ee, q) => {
                    ch.consume(q.queue, msg => {
                        const idx = ids.indexOf(msg.properties.correlationId);
                        if ( idx > -1){
                            console.log(`JOB: ${msg.properties.correlationId} DONE`)
                            ids.splice(idx,1);
                            console.log('remaining:', ids.length);
                            if (!ids.length)
                                console.log('** All done');
                            prompt();
                        }
                    }, {noAck: true});
                    ch.sendToQueue(
                    'rpc', 
                    Buffer.from(input), 
                    { correlationId: id, replyTo: q.queue });
                });
                
                prompt();
            });
        }
        // readline.on('line', ()=> prompt());
    });
});
function genuuid() {
    return Math.random().toString() +
        Math.random().toString() +
        Math.random().toString();

}