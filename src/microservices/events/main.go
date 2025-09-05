package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

// EventService represents the events service with Kafka integration
type EventService struct {
	producer sarama.SyncProducer
	consumer sarama.ConsumerGroup
	topics   []string
}

// Event represents a generic event structure
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// EventResponse represents the response structure
type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	ready chan bool
}

func main() {
	// Get configuration from environment
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	// Initialize Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer([]string{kafkaBrokers}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer producer.Close()

	// Initialize Kafka consumer
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumer, err := sarama.NewConsumerGroup([]string{kafkaBrokers}, "events-service-group", consumerConfig)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer group: %v", err)
	}
	defer consumer.Close()

	// Create event service
	eventService := &EventService{
		producer: producer,
		consumer: consumer,
		topics:   []string{"movie-events", "user-events", "payment-events"},
	}

	// Start consumer in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &ConsumerGroupHandler{
		ready: make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := eventService.consumer.Consume(ctx, eventService.topics, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	// Wait for consumer to be ready
	<-handler.ready
	log.Println("Kafka consumer is ready")

	// Set up HTTP routes
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", eventService.handleMovieEvent)
	http.HandleFunc("/api/events/user", eventService.handleUserEvent)
	http.HandleFunc("/api/events/payment", eventService.handlePaymentEvent)

	// Start HTTP server
	log.Printf("Starting events service on port %s with Kafka brokers: %s", port, kafkaBrokers)

	// Graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Fatal(http.ListenAndServe(":"+port, nil))
	}()

	<-sigterm
	log.Println("Terminating...")
	cancel()
	wg.Wait()
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func (es *EventService) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var eventData map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        fmt.Sprintf("movie-event-%d", time.Now().UnixNano()),
		Type:      "movie",
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   eventData,
	}

	// Send to Kafka
	partition, offset, err := es.sendEvent("movie-events", event)
	if err != nil {
		log.Printf("Error sending movie event to Kafka: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	log.Printf("Movie event sent to Kafka - Topic: movie-events, Partition: %d, Offset: %d, Event: %+v", partition, offset, event)

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (es *EventService) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var eventData map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        fmt.Sprintf("user-event-%d", time.Now().UnixNano()),
		Type:      "user",
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   eventData,
	}

	// Send to Kafka
	partition, offset, err := es.sendEvent("user-events", event)
	if err != nil {
		log.Printf("Error sending user event to Kafka: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	log.Printf("User event sent to Kafka - Topic: user-events, Partition: %d, Offset: %d, Event: %+v", partition, offset, event)

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (es *EventService) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var eventData map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create event
	event := Event{
		ID:        fmt.Sprintf("payment-event-%d", time.Now().UnixNano()),
		Type:      "payment",
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   eventData,
	}

	// Send to Kafka
	partition, offset, err := es.sendEvent("payment-events", event)
	if err != nil {
		log.Printf("Error sending payment event to Kafka: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	log.Printf("Payment event sent to Kafka - Topic: payment-events, Partition: %d, Offset: %d, Event: %+v", partition, offset, event)

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (es *EventService) sendEvent(topic string, event Event) (int32, int64, error) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(eventJSON),
		Key:   sarama.StringEncoder(event.ID),
	}

	partition, offset, err := es.producer.SendMessage(message)
	return partition, offset, err
}

// ConsumerGroupHandler methods
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Process the message
			var event Event
			if err := json.Unmarshal(message.Value, &event); err != nil {
				log.Printf("Error unmarshaling event: %v", err)
				continue
			}

			log.Printf("Processing event from Kafka - Topic: %s, Partition: %d, Offset: %d, Event: %+v",
				message.Topic, message.Partition, message.Offset, event)

			// Mark message as processed
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
