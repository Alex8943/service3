import express from "express";
import { Review as Reviews, User, Media, Genre, ReviewGenres } from "../other_services/model/seqModel";
import logger from "../other_services/winstonLogger";
import {fetchDataFromQueue, createChannel } from "../other_services/rabbitMQ";
import { Op } from "sequelize";
import verifyUser from "./authenticateUser";

const router = express.Router();



// Route to search for a review by title
router.get("/review/:title", verifyUser, async (req, res) => {
    
    try {
        // Access title from req.params instead of req.body
        const result = await searchReviewByTitle(req.params.title);
        console.log("Result: ", result)

        res.status(200).send(result); 
    } catch (error) {
        console.error("Error searching review by title: ", error);
        res.status(500).send("Something went wrong while searching the review by title");
    }
});

export async function searchReviewByTitle(value: string) {
    try {
        // create channel:
        const { connection, channel } = await createChannel();
        const queue = "search-review-service";

        // assert queue:
        await channel.assertQueue(queue, { durable: false });
        console.log(`Listening for messages in ${queue}...`);

        // 1. Fetch reviews matching the title
        const reviews = await Reviews.findAll({
            where: {
                title: {
                    [Op.like]: `%${value}%`,
                },
            },
        });

        if (!reviews.length) {
            logger.error("No reviews found matching the title.");
            return [];
        }

        console.log("Fetched reviews from the database:", reviews);

        // 2. Enrich reviews with data from RabbitMQ
        const enrichedReviews = await Promise.all(
            reviews.map(async (review) => {
                const [user, media, genres] = await Promise.all([
                    fetchDataFromQueue("user-service", { userId: review.user_fk }),
                    fetchDataFromQueue("media-service", { mediaId: review.media_fk }),
                    fetchDataFromQueue("genre-service", { reviewId: review.id }),
                ]);

                return {
                    id: review.id,
                    title: review.title,
                    description: review.description,
                    userName: user?.name || "Unknown",
                    mediaName: media?.name || "Unknown",
                    genreNames: genres?.map((genre: any) => genre.name).join(", ") || "None",
                };
            })
        );

        return enrichedReviews;
    } catch (error) {
        logger.error("Error searching reviews by title:", error);
        throw error;
    }
}




export default router;
