import { connect } from "amqplib";
import { MongoClient, ObjectID, } from 'mongodb';

(async () => {

    let mongodbclient: any;
    try {

        const rabbitMqConnection = await connect({ hostname: "127.0.0.1", port: 5672, username: "test", password: "test", vhost: '/' });

        mongodbclient = await MongoClient.connect('mongodb+srv://coding_lamda_db:mongodbCoding@cluster0.hmcxe.mongodb.net/CQRS?retryWrites=true&w=majority');

        const cqrsDb = mongodbclient.db('CQRS');
        const cqrsStoreDb = mongodbclient.db('CQRS_STORE');

        const commandsDb = cqrsDb.collection('commands');
        const queryResultsDb = cqrsDb.collection('queryResults');

        const roomsCollection = cqrsStoreDb.collection('rooms');

        const channel = await rabbitMqConnection.createChannel();

        var queue = 'rpc_queue';
        // const assertQueueReply = await channel.assertQueue('', {
        //     durable: false,
        // });

        await channel.prefetch(1);

        console.log(' [x] Awaiting RPC requests');

        channel.consume(queue, async (msg) => {
            let session;
            try {

                session = mongodbclient.startSession();
                let responseToSendToQueue = {};
                const mongodbCommandId = msg!.content.toString();

                const command = await commandsDb.findOne({
                    _id: new ObjectID(mongodbCommandId)
                });

                if (command) {
                    channel.ack(msg!); // If we found a matching command, Acknowledge to the queue that we found a matching command
                }

                // ---------- Handle processing of command here --------------- //

                if (command?.requestType === "BOOK") {
                    // Check whether the status is booked for the given roomId
                    // if yes, then to the replyQueue send message that the room is already booked.
                    // if no, then mark the room as booked with the given user details
                    // In the response queue, Send message with the room as booked

                    const roomId = command.roomId;

                    const listOfRooms = await roomsCollection.find({
                        roomId: roomId,
                    }, { session: session }).toArray();

                    if (listOfRooms.length === 0) {
                        responseToSendToQueue = {
                            status: "NO_ROOM_FOUND",
                            corelationId: msg!.properties.correlationId
                        }
                    } else {
                        const room = listOfRooms[0];

                        if (room.status === "booked") {
                            responseToSendToQueue = {
                                status: "ROOM_ALREADY_BOOKED",
                                corelationId: msg!.properties.correlationId
                            }
                        } else {
                            await roomsCollection.findOneAndUpdate({
                                roomId: roomId,
                            }, {
                                status: "booked",
                                bookedBy: command.userId
                            }, { session: session });

                            responseToSendToQueue = {
                                status: "ROOM_BOOKED_SUCCESSFULLY",
                                corelationId: msg!.properties.correlationId
                            }
                        }

                    }


                } else if (command?.requestType === "CANCEL") {
                    // Check whether the room is booked by the given user
                    // if yes then mark the room booking as canceled and in the response queue, Send message with the success status of the query.
                    // if no then in the response queue, send message with the status of failed to cancel the booking

                    const roomId = command.roomId;

                    const listOfRooms = await roomsCollection.find({
                        roomId: roomId,
                    }).toArray();

                    if (listOfRooms.length === 0) {
                        responseToSendToQueue = {
                            status: "NO ROOM FOUND",
                            corelationId: msg!.properties.correlationId
                        }
                    } else {
                        const room = listOfRooms[0];

                        if (room.status === "booked" && room.bookedBy === command.userId) {
                            await roomsCollection.findOneAndUpdate({
                                roomId: roomId,
                            }, {
                                status: "canceled",
                                bookedBy: command.userId
                            }, { session: session });

                            responseToSendToQueue = {
                                status: "BOOKING_CANCELED_SUCCESSFULLY",
                                corelationId: msg!.properties.correlationId
                            }

                        } else {
                            responseToSendToQueue = {
                                status: "FAILED_TO_CANCEL_BOOKING",
                                corelationId: msg!.properties.correlationId
                            }
                        }
                    }
                }

                await queryResultsDb.insertOne(responseToSendToQueue);

                // channel.sendToQueue(msg!.properties.replyTo,
                //     Buffer.from(JSON.stringify(responseToSendToQueue)), {
                //     correlationId: msg!.properties.correlationId
                // });


            } catch (error) {
                session?.abortTransaction();
                console.log(error);
            } finally {
                session?.endSession();
            }

        });

    } catch (error) {
        mongodbclient?.close();
        console.log(error);
    }
})();


