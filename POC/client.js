const amqp = require('amqplib');
const readline = require('readline');
const fs = require('fs');
const LinkedList = require('dbly-linked-list');

let [doc, map] = process.argv.slice(2);
let result = new LinkedList();
let rowCount = 0;
let populated = 0;
const fileName = './result.json';
const processTimer = 'DONE processing...';
const writeTimer = 'DONE writing...';
const preProcessTimer = 'DONE preprocessing...';
console.time(processTimer);
map = require('./' + map);

(async function connect() {
    try {
        //create the connection
        const conn = await amqp.connect('amqp://localhost');
        //create the channel
        const ch = await conn.createChannel();
        //create the callback queue
        const q = await ch.assertQueue('reply');
        //listen to the queue for responses from the workers
        ch.consume(q.queue, msg => {
            appendResult(msg);
            if (populated === rowCount - 1) {
                console.timeEnd(processTimer);
                doWrite();
                ch.close();
            }
        }, { noAck: true });
        ch.on('error', err => console.log('CHANNEL ERR:', err));
        //read the input file
        doRead(ch, q);
    } catch (e) {
        console.log('ERROR: ', e);
    }
})();
function doRead(ch, q) {
    const reader = readline.createInterface({
        input: fs.createReadStream(doc)
    });
    let schema;
    reader.on('line', line => {
        if (!rowCount) {
            // first row contains header names
            schema = line.toString().split(',');
            reader.resume();
        }
        else if (rowCount > 0) {
            //send the line to be processed
            ch.sendToQueue(
                'rpc',
                Buffer.from(JSON.stringify({ line, map, schema })),
                { replyTo: q.queue, correlationId: rowCount.toString() });
        }
        rowCount++;
    });
    reader.on('close', () => {
        console.log(`SENT: ${rowCount - 1} lines`);
    });
}

function doWrite() {
    // write the results to a file
    console.log('Writing...');
    console.time(writeTimer);
    if (fs.existsSync(fileName)) {
        fs.unlinkSync(fileName);
    }
    fs.writeFileSync(fileName, '{"rows":[', 'utf-8');
    result.forEach(r => {
        fs.appendFileSync(fileName, r.getData().data + ',', 'utf-8');
    })
    fs.appendFileSync(fileName, ']}', 'utf-8');
    console.timeEnd(writeTimer);
    process.exit(0);
}

function appendResult(msg) {
    //resemble the document sorted by row identifer
    const data = msg.content.toString();
    const id = Number(JSON.parse(data).row);
    const min = result.isEmpty() ? 0 : result.getHeadNode().getData().id;
    const max = result.isEmpty() ? 0 : result.getTailNode().getData().id;
    const node = { id, data };
    
    if (!min) {
        //console.log('FIRST:', id);
        result.insert(node);
    }
    else if (id > min && id < max) {
        //conditionally start at the tail or the head, whichever is more efficient
        if ((max - id) >= (id - min)) {
            let n = result.getHeadNode();
            while (n && id > n.getData().id) {
                n = n.next;
            }
            result.insertBefore(n.getData(), node);
        } else {
            let n = result.getTailNode();
            while (n && id < n.getData().id) {
                n = n.prev;
            }
            result.insertAfter(n.getData(), node);
        }
        //console.log('INSERTED:', id, 'after:', n.getData().id);
    }
    else if (id < min && id < max) {
        result.insertFirst(node);
        //console.log('LESS THAN', id);
    }
    else if (id > min && id > max) {
        result.insert(node);
        //console.log('GREATER THAN', id);
    }
    populated++;
}

