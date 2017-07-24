const amqp = require('amqplib/callback_api');

amqp.connect('amqp://localhost', (err,conn)=>{
    conn.createChannel((e,ch)=>{
        const q = 'task_queue';
        ch.assertQueue(q, {durable:true});
        ch.prefetch(1);
        ch.consume(q, (msg)=>{
            let delay = 1;
            const parts = msg.content.toString().split('.');
            if (parts.length) {
                delay = Number(parts[parts.length - 1]);
            }
            console.log('Received:', msg.content.toString());
            setTimeout(()=> {
                console.log('Done');
                ch.ack(msg);
            }, delay * 1000);
        }, {noAck:false});
    });
});