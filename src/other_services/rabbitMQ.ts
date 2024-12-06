import amqp, { Channel, Connection } from "amqplib";

const RABBITMQ_URL = "amqp://localhost"; // Update if RabbitMQ is hosted remotely
let connection: Connection | null = null;
let channel: Channel | null = null;

/**
 * Create and return a RabbitMQ channel
 */
export async function createChannel(): Promise<{ channel: Channel; connection: Connection }> {
    if (!connection) {
        connection = await amqp.connect(RABBITMQ_URL);
        console.log("RabbitMQ connection established.");
    }
    if (!channel) {
        channel = await connection.createChannel();
        console.log("RabbitMQ channel created.");
    }

    process.on("SIGINT", async () => {
        console.log("Closing RabbitMQ connection...");
        await channel?.close();
        await connection?.close();
        process.exit(0);
    });

    return { channel, connection };
}

/**
 * Fetch data from a RabbitMQ queue
 * @param queue Name of the queue to send the message
 * @param message Payload to send to the queue
 */
export const fetchDataFromQueue = async (queue: string, message: any): Promise<any> => {
    const { channel } = await createChannel();
    const replyQueue = await channel.assertQueue("", { exclusive: true });
    const correlationId = generateUuid();

    console.log(`Sending message to queue: ${queue}`);
    console.log(`Message: ${JSON.stringify(message)}`);
    console.log(`Correlation ID: ${correlationId}`);

    return new Promise((resolve, reject) => {
        channel.consume(
            replyQueue.queue,
            (msg: any) => {
                if (msg?.properties.correlationId === correlationId) {
                    const response = JSON.parse(msg.content.toString());
                    console.log(`Received response from ${queue}:`, response);
                    resolve(response);
                }
            },
            { noAck: true }
        );

        channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
            replyTo: replyQueue.queue,
            correlationId,
        });
    });
};

/**
 * Utility function to generate unique correlation IDs
 */
function generateUuid() {
    return Math.random().toString() + Math.random().toString() + Math.random().toString();
}

/**
 * Set up a queue for Service3: search-review-service
 */
export async function setupService3Queue() {
    const { channel } = await createChannel();

    try {
        const queue = "search-review-service"; // Queue specific to Service3
        await channel.assertQueue(queue, { durable: false });
        console.log(`Queue "${queue}" is ready for Service3.`);
    } catch (error) {
        console.error("Error setting up queue for Service3:", error);
    }
}


export async function setupQueues() {
    const { channel } = await createChannel();

    try {
        const queues = ["user-service", "media-service", "genre-service"];
        for (const queue of queues) {
            await channel.assertQueue(queue, { durable: false });
            console.log(`Queue "${queue}" is ready.`);
        }
        console.log("All necessary queues have been set up.");
    } catch (error) {
        console.error("Error setting up queues:", error);
    }
}
