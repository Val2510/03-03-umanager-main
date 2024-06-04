package link_updater

import (
	"context"
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/database"
	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/pkg/scrape"
)

type Story struct {
	repository repository
	consumer   amqpConsumer
}

func New(repository repository, consumer amqpConsumer) *Story {
	return &Story{repository: repository, consumer: consumer}
}

func (s *Story) Run(ctx context.Context) error {
	msgs, err := s.consumer.Consume(
		"your_queue_name",
		"your_consumer_name",
		true,  // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return err
	}

	for d := range msgs {
		go func(d amqp.Delivery) {
			var m message
			if err := json.Unmarshal(d.Body, &m); err != nil {
				log.Printf("Error decoding message: %v", err)
				return
			}

			linkID, err := primitive.ObjectIDFromHex(m.ID)
			if err != nil {
				log.Printf("Invalid ID format: %v", err)
				return
			}

			link, err := s.repository.FindByID(ctx, linkID)
			if err != nil {
				log.Printf("Error finding link: %v", err)
				return
			}

			scrapedData, err := scrape.Parse(ctx, link.URL)
			if err != nil {
				log.Printf("Error scraping link: %v", err)
				return
			}

			if scrapedData != nil {
				req := database.UpdateLinkReq{
					ID:     link.ID,
					URL:    link.URL,
					Title:  scrapedData.Title,
					Tags:   link.Tags,
					Images: link.Images,
					UserID: link.UserID,
				}
				_, err = s.repository.Update(ctx, req)
				if err != nil {
					log.Printf("Error updating link: %v", err)
					return
				}
			}
		}(d)
	}

	return nil
}

type message struct {
	ID string `json:"id"`
}
