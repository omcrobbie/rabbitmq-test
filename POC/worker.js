const amqp = require('amqplib');

let rows = 0;
function parseRow(input) {
    let {line, map, schema } = input;
    let row = {};
    const data = line.split(',');
    schema.forEach( (col) => {
        row[map[col]] = data[schema.indexOf(col)];
    });
    //console.log(row);
    return row;
}

(async function() {
    const conn = await amqp.connect('amqp://localhost');
    const ch = await conn.createChannel();
    const qName = 'rpc';
    const q = await ch.assertQueue(qName, {durable: false});
    ch.prefetch(1);
    console.log('Ready and waiting...');
    ch.consume(q.queue, function reply (msg){
        console.log(`Finished ${rows++} rows`);
        const data = parseRow(JSON.parse(msg.content.toString()));
        ch.sendToQueue(
            msg.properties.replyTo,
            Buffer.from(JSON.stringify(data))
        );
        ch.ack(msg);
    });
    
})();