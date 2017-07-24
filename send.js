const amqp = require('amqplib/callback_api');

amqp.connect('amqp://localhost', (err,conn)=>{
    conn.createChannel((e, ch)=>{
        const q = 'hello';
        const msg = 'Hello World';
        ch.assertQueue(q, {durable: false});
        ch.sendToQueue(q, Buffer.from(msg));
        console.log('Sent:', msg);
    });
    setTimeout(()=>{
        conn.close();
        process.exit(0);
    }, 500);
});