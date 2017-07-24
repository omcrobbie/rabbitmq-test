const amqp = require('amqplib/callback_api');

amqp.connect('amqp://localhost', (err,conn)=>{
    conn.createChannel((e,ch)=>{
        const q = 'hello';
        ch.assertQueue(q, {durable:false});
        ch.consume(q, (msg)=>{
            console.log('Received:', msg.content.toString());
        }, {noAck:true});
    });
});