package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	AppPort        string
	PostgresDSN    string
	KafkaBrokers   []string
	KafkaTopic     string
	MaxOpenConns   int
	ConnectTimeout time.Duration
	BatchSize      int
	FlushInterval  time.Duration
}

type Order struct {
	UserID int     `json:"user_id"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

type Metrics struct {
	startedAt         time.Time
	requestsTotal     atomic.Uint64
	errorsTotal       atomic.Uint64
	queuedAccepted    atomic.Uint64
	directAccepted    atomic.Uint64
	rowsWrittenTotal  atomic.Uint64
	producedMessages  atomic.Uint64
	consumedMessages  atomic.Uint64
	writeLatencyNanos atomic.Uint64
	workerErrorsTotal atomic.Uint64
}

type App struct {
	cfg     Config
	db      *sql.DB
	writer  *kafka.Writer
	reader  *kafka.Reader
	metrics *Metrics
}

type statusResponse struct {
	DBRows             int64   `json:"db_rows"`
	QueueDepth         int64   `json:"queue_depth"`
	ArrivalRateRPS     float64 `json:"arrival_rate_rps"`
	AvgWriteLatencyMS  float64 `json:"avg_write_latency_ms"`
	ServiceRatePerConn float64 `json:"service_rate_per_conn"`
	ActiveConnections  int     `json:"active_connections"`
	MaxConnections     int     `json:"max_connections"`
	UtilizationRho     float64 `json:"utilization_rho"`
	ErrorsTotal        uint64  `json:"errors_total"`
	RequestsTotal      uint64  `json:"requests_total"`
	RowsWrittenTotal   uint64  `json:"rows_written_total"`
	WorkerErrorsTotal  uint64  `json:"worker_errors_total"`
}

func main() {
	cfg := loadConfig()

	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxOpenConns)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := waitForDatabase(ctx, db); err != nil {
		log.Fatalf("wait for database: %v", err)
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.KafkaTopic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic,
		GroupID:     "orders-writer",
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     200 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})

	app := &App{
		cfg:     cfg,
		db:      db,
		writer:  writer,
		reader:  reader,
		metrics: &Metrics{startedAt: time.Now()},
	}

	go app.runConsumer(ctx)

	server := &http.Server{
		Addr:              ":" + cfg.AppPort,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown error: %v", err)
		}

		if err := app.reader.Close(); err != nil {
			log.Printf("close kafka reader: %v", err)
		}
		if err := app.writer.Close(); err != nil {
			log.Printf("close kafka writer: %v", err)
		}
		if err := app.db.Close(); err != nil {
			log.Printf("close database: %v", err)
		}
	}()

	log.Printf(
		"listening on :%s (topic=%s max_open_conns=%d batch_size=%d flush_interval=%s)",
		cfg.AppPort,
		cfg.KafkaTopic,
		cfg.MaxOpenConns,
		cfg.BatchSize,
		cfg.FlushInterval,
	)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen and serve: %v", err)
	}
}

func loadConfig() Config {
	return Config{
		AppPort:        getEnv("APP_PORT", "8080"),
		PostgresDSN:    getEnv("POSTGRES_DSN", "host=localhost port=5432 user=postgres password=postgres dbname=simulator sslmode=disable"),
		KafkaBrokers:   splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:     getEnv("KAFKA_TOPIC", "orders"),
		MaxOpenConns:   getEnvInt("MAX_OPEN_CONNS", 10),
		ConnectTimeout: getEnvDuration("CONNECT_TIMEOUT", 2*time.Second),
		BatchSize:      getEnvInt("BATCH_SIZE", 100),
		FlushInterval:  getEnvDuration("FLUSH_INTERVAL", 500*time.Millisecond),
	}
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /write-direct", a.handleWriteDirect)
	mux.HandleFunc("POST /write-queue", a.handleWriteQueue)
	mux.HandleFunc("GET /status", a.handleStatus)
	mux.HandleFunc("GET /healthz", a.handleHealth)
	return mux
}

func (a *App) handleWriteDirect(w http.ResponseWriter, r *http.Request) {
	a.metrics.requestsTotal.Add(1)

	order, err := decodeOrder(r)
	if err != nil {
		a.metrics.errorsTotal.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.ConnectTimeout)
	defer cancel()

	start := time.Now()
	if err := insertOrder(ctx, a.db, order); err != nil {
		a.metrics.errorsTotal.Add(1)
		http.Error(w, fmt.Sprintf("database insert failed: %v", err), http.StatusInternalServerError)
		return
	}

	a.metrics.directAccepted.Add(1)
	a.recordWriteMetrics(time.Since(start), 1)

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "inserted",
	})
}

func (a *App) handleWriteQueue(w http.ResponseWriter, r *http.Request) {
	a.metrics.requestsTotal.Add(1)

	order, err := decodeOrder(r)
	if err != nil {
		a.metrics.errorsTotal.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	payload, err := json.Marshal(order)
	if err != nil {
		a.metrics.errorsTotal.Add(1)
		http.Error(w, fmt.Sprintf("marshal order: %v", err), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.ConnectTimeout)
	defer cancel()

	if err := a.writer.WriteMessages(ctx, kafka.Message{Value: payload}); err != nil {
		a.metrics.errorsTotal.Add(1)
		http.Error(w, fmt.Sprintf("kafka publish failed: %v", err), http.StatusInternalServerError)
		return
	}

	a.metrics.queuedAccepted.Add(1)
	a.metrics.producedMessages.Add(1)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status": "queued",
	})
}

func (a *App) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var dbRows int64
	if err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders").Scan(&dbRows); err != nil {
		http.Error(w, fmt.Sprintf("query row count: %v", err), http.StatusInternalServerError)
		return
	}

	stats := a.db.Stats()
	resp := statusResponse{
		DBRows:             dbRows,
		QueueDepth:         maxInt64(int64(a.metrics.producedMessages.Load())-int64(a.metrics.consumedMessages.Load()), 0),
		ArrivalRateRPS:     a.arrivalRatePerSecond(),
		AvgWriteLatencyMS:  a.avgWriteLatencyMS(),
		ServiceRatePerConn: a.serviceRatePerConn(),
		ActiveConnections:  stats.InUse,
		MaxConnections:     a.cfg.MaxOpenConns,
		UtilizationRho:     a.utilizationRho(),
		ErrorsTotal:        a.metrics.errorsTotal.Load(),
		RequestsTotal:      a.metrics.requestsTotal.Load(),
		RowsWrittenTotal:   a.metrics.rowsWrittenTotal.Load(),
		WorkerErrorsTotal:  a.metrics.workerErrorsTotal.Load(),
	}

	writeJSON(w, http.StatusOK, resp)
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) runConsumer(ctx context.Context) {
	var (
		messages []kafka.Message
		orders   []Order
		deadline time.Time
	)

	for {
		if len(orders) > 0 && time.Now().After(deadline) {
			if err := a.flushBatch(ctx, messages, orders); err != nil {
				log.Printf("flush batch: %v", err)
			} else {
				messages = messages[:0]
				orders = orders[:0]
				deadline = time.Time{}
			}
		}

		if ctx.Err() != nil {
			if len(orders) > 0 {
				if err := a.flushBatch(context.Background(), messages, orders); err != nil {
					log.Printf("flush final batch: %v", err)
				}
			}
			return
		}

		fetchCtx := ctx
		cancel := func() {}
		if len(orders) > 0 {
			waitFor := time.Until(deadline)
			if waitFor <= 0 {
				continue
			}
			fetchCtx, cancel = context.WithTimeout(ctx, waitFor)
		}

		msg, err := a.reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			a.metrics.workerErrorsTotal.Add(1)
			log.Printf("fetch message: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var order Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			a.metrics.workerErrorsTotal.Add(1)
			log.Printf("decode queued order: %v", err)
			if err := a.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit malformed message: %v", err)
			}
			continue
		}

		order.normalize()
		messages = append(messages, msg)
		orders = append(orders, order)
		if len(orders) == 1 {
			deadline = time.Now().Add(a.cfg.FlushInterval)
		}

		if len(orders) >= a.cfg.BatchSize {
			if err := a.flushBatch(ctx, messages, orders); err != nil {
				log.Printf("flush full batch: %v", err)
			} else {
				messages = messages[:0]
				orders = orders[:0]
				deadline = time.Time{}
			}
		}
	}
}

func (a *App) flushBatch(ctx context.Context, messages []kafka.Message, orders []Order) error {
	if len(orders) == 0 {
		return nil
	}

	flushCtx, cancel := context.WithTimeout(ctx, a.cfg.ConnectTimeout)
	defer cancel()

	start := time.Now()
	if err := bulkInsertOrders(flushCtx, a.db, orders); err != nil {
		a.metrics.workerErrorsTotal.Add(1)
		return err
	}

	if err := a.reader.CommitMessages(flushCtx, messages...); err != nil {
		a.metrics.workerErrorsTotal.Add(1)
		return err
	}

	a.metrics.consumedMessages.Add(uint64(len(messages)))
	a.recordWriteMetrics(time.Since(start), len(orders))
	return nil
}

func insertOrder(ctx context.Context, db *sql.DB, order Order) error {
	order.normalize()
	_, err := db.ExecContext(
		ctx,
		"INSERT INTO orders (user_id, amount, status) VALUES ($1, $2, $3)",
		order.UserID,
		order.Amount,
		order.Status,
	)
	return err
}

func bulkInsertOrders(ctx context.Context, db *sql.DB, orders []Order) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("orders", "user_id", "amount", "status"))
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare copy in: %w", err)
	}

	for _, order := range orders {
		if _, err := stmt.ExecContext(ctx, order.UserID, order.Amount, order.Status); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return fmt.Errorf("copy order: %w", err)
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		_ = stmt.Close()
		_ = tx.Rollback()
		return fmt.Errorf("finalize copy: %w", err)
	}

	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close copy statement: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit copy transaction: %w", err)
	}

	return nil
}

func decodeOrder(r *http.Request) (Order, error) {
	defer r.Body.Close()

	var order Order
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&order); err != nil {
		return Order{}, fmt.Errorf("decode order: %w", err)
	}

	order.normalize()

	if order.UserID <= 0 {
		return Order{}, errors.New("user_id must be > 0")
	}
	if order.Amount <= 0 {
		return Order{}, errors.New("amount must be > 0")
	}

	return order, nil
}

func (o *Order) normalize() {
	o.Status = strings.TrimSpace(o.Status)
	if o.Status == "" {
		o.Status = "pending"
	}
}

func (a *App) recordWriteMetrics(duration time.Duration, rows int) {
	if rows <= 0 {
		return
	}
	a.metrics.rowsWrittenTotal.Add(uint64(rows))
	a.metrics.writeLatencyNanos.Add(uint64(duration))
}

func (a *App) avgWriteLatencyMS() float64 {
	rows := a.metrics.rowsWrittenTotal.Load()
	if rows == 0 {
		return 0
	}

	totalNanos := a.metrics.writeLatencyNanos.Load()
	avg := float64(totalNanos) / float64(rows)
	return avg / float64(time.Millisecond)
}

func (a *App) serviceRatePerConn() float64 {
	rows := a.metrics.rowsWrittenTotal.Load()
	if rows == 0 {
		return 0
	}

	totalSeconds := float64(a.metrics.writeLatencyNanos.Load()) / float64(time.Second)
	if totalSeconds <= 0 {
		return 0
	}

	return float64(rows) / totalSeconds
}

func (a *App) arrivalRatePerSecond() float64 {
	uptime := time.Since(a.metrics.startedAt).Seconds()
	if uptime <= 0 {
		return 0
	}

	return float64(a.metrics.requestsTotal.Load()) / uptime
}

func (a *App) utilizationRho() float64 {
	serviceRate := a.serviceRatePerConn()
	if serviceRate <= 0 || a.cfg.MaxOpenConns <= 0 {
		return 0
	}

	rho := a.arrivalRatePerSecond() / (float64(a.cfg.MaxOpenConns) * serviceRate)
	if math.IsNaN(rho) || math.IsInf(rho, 0) {
		return 0
	}
	return rho
}

func waitForDatabase(ctx context.Context, db *sql.DB) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("database not ready yet: %v", err)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := getEnv(key, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("invalid integer for %s=%q, using %d", key, value, fallback)
		return fallback
	}
	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value := getEnv(key, fallback.String())
	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("invalid duration for %s=%q, using %s", key, value, fallback)
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
