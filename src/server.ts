import { connect } from 'amqplib';
import * as express from 'express';
import { v4 as uuidV4 } from 'uuid';
import { MongoClient } from 'mongodb';



const server = express();

(async () => {
    let mongodbclient;
    try {
        server.use(express.json());


        mongodbclient = await MongoClient.connect('mongodb+srv://coding_lamda_db:mongodbCoding@cluster0.hmcxe.mongodb.net/CQRS?retryWrites=true&w=majority');

        const cqrsDb = mongodbclient.db('CQRS');


        const commandsDb = cqrsDb.collection('commands');

        const queryResultsDb = cqrsDb.collection('queryResults');


        //'amqp://localhost:5672'
        const rabbitMqConnection = await connect({ hostname: "127.0.0.1", port: 5672, username: "test", password: "test", vhost: '/' });

        const channel = await rabbitMqConnection.createChannel();

        // const assertQueueReply = await channel.assertQueue('', {
        //     exclusive: true
        // });


        // channel.consume(assertQueueReply.queue, function (msg) {
        //     if (msg!.properties.correlationId == correlationId) {
        //         console.log(' [.] Got %s', msg!.content.toString());
        //         setTimeout(function () {
        //             // connection.close();
        //             // process.exit(0)
        //         }, 500);
        //     }
        // }, {
        //     noAck: true
        // });



        server.post('/register-command', async (request, response) => {
            try {


                const correlationId = uuidV4(); // Generate a corelation id everytime a new http request is generated

                const requestType = request.body.requestType; // 'BOOK','CANCEL'
                const userId = request.body.userId; // can be 
                const roomId = request.body.roomId; // can be 1,2,3

                const commandToInsert = {
                    requestType,
                    userId,
                    roomId
                }
                const commandInsertionResult = await commandsDb.insertOne(commandToInsert);

                const commandId = commandInsertionResult.insertedId.toString();

                channel.sendToQueue('rpc_queue',
                    Buffer.from(commandId), {
                    correlationId: correlationId,

                });

                response.json({
                    status: "SUCCESS",
                    correlationId: correlationId
                });
            } catch (error: any) {
                response.json({
                    status: "FAILED",
                    message: error.toString()
                });
            }

        });



        server.post('/check-command-status', async (request, response) => {
            try {
                const corelationId = request.body.corelationId;

                const commandResult = await queryResultsDb.findOne({ corelationId });

                if (commandResult) {
                    response.json({
                        status: "SUCCESS",
                        commandResult
                    });

                } else {
                    throw "No result found";
                }

            } catch (error: any) {
                response.json({
                    status: "FAILED",
                    message: error.toString()
                });
            }
        });

        server.listen(3000, function () {
            console.log("Listening on 3000");
        })

    } catch (error) {
        console.log(error);
        mongodbclient?.close();
    }

})();



