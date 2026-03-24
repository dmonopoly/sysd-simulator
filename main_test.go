package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// ---- decodeOrder -------------------------------------------------------

func makeRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "/write-direct", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestDecodeOrder_Valid(t *testing.T) {
	req := makeRequest(t, `{"user_id":42,"amount":19.99,"status":"pending"}`)
	order, err := decodeOrder(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if order.UserID != 42 {
		t.Errorf("user_id: got %d, want 42", order.UserID)
	}
	if order.Amount != 19.99 {
		t.Errorf("amount: got %f, want 19.99", order.Amount)
	}
	if order.Status != "pending" {
		t.Errorf("status: got %q, want %q", order.Status, "pending")
	}
}

func TestDecodeOrder_MissingStatus_DefaultsPending(t *testing.T) {
	req := makeRequest(t, `{"user_id":1,"amount":5.00}`)
	order, err := decodeOrder(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if order.Status != "pending" {
		t.Errorf("status: got %q, want %q", order.Status, "pending")
	}
}

func TestDecodeOrder_InvalidUserID_Zero(t *testing.T) {
	req := makeRequest(t, `{"user_id":0,"amount":5.00}`)
	_, err := decodeOrder(req)
	if err == nil {
		t.Fatal("expected error for user_id=0, got nil")
	}
}

func TestDecodeOrder_InvalidUserID_Negative(t *testing.T) {
	req := makeRequest(t, `{"user_id":-1,"amount":5.00}`)
	_, err := decodeOrder(req)
	if err == nil {
		t.Fatal("expected error for negative user_id, got nil")
	}
}

func TestDecodeOrder_InvalidAmount_Zero(t *testing.T) {
	req := makeRequest(t, `{"user_id":1,"amount":0}`)
	_, err := decodeOrder(req)
	if err == nil {
		t.Fatal("expected error for amount=0, got nil")
	}
}

func TestDecodeOrder_InvalidAmount_Negative(t *testing.T) {
	req := makeRequest(t, `{"user_id":1,"amount":-5.00}`)
	_, err := decodeOrder(req)
	if err == nil {
		t.Fatal("expected error for negative amount, got nil")
	}
}

func TestDecodeOrder_UnknownField_Rejected(t *testing.T) {
	// DisallowUnknownFields is enforced; extra keys must return an error
	// so that callers know their payload doesn't match the schema.
	req := makeRequest(t, `{"user_id":1,"amount":5.00,"note":"debug"}`)
	_, err := decodeOrder(req)
	if err == nil {
		t.Fatal("expected error for unknown field 'note', got nil")
	}
}

func TestDecodeOrder_EmptyBody(t *testing.T) {
	req := makeRequest(t, `{}`)
	_, err := decodeOrder(req)
	if err == nil {
		t.Fatal("expected error for empty object (user_id=0), got nil")
	}
}

// ---- Order.normalize ---------------------------------------------------

func TestNormalize_EmptyStatus(t *testing.T) {
	o := Order{UserID: 1, Amount: 1.0, Status: ""}
	o.normalize()
	if o.Status != "pending" {
		t.Errorf("got %q, want %q", o.Status, "pending")
	}
}

func TestNormalize_WhitespaceStatus(t *testing.T) {
	o := Order{UserID: 1, Amount: 1.0, Status: "   "}
	o.normalize()
	if o.Status != "pending" {
		t.Errorf("got %q, want %q", o.Status, "pending")
	}
}

func TestNormalize_PreservesExplicitStatus(t *testing.T) {
	o := Order{UserID: 1, Amount: 1.0, Status: "shipped"}
	o.normalize()
	if o.Status != "shipped" {
		t.Errorf("got %q, want %q", o.Status, "shipped")
	}
}

func TestNormalize_TrimsWhitespaceAroundStatus(t *testing.T) {
	o := Order{UserID: 1, Amount: 1.0, Status: "  shipped  "}
	o.normalize()
	if o.Status != "shipped" {
		t.Errorf("got %q, want %q", o.Status, "shipped")
	}
}

// ---- Config helpers ----------------------------------------------------

func TestGetEnvInt_ExplicitValue(t *testing.T) {
	t.Setenv("TEST_INT", "42")
	got := getEnvInt("TEST_INT", 10)
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestGetEnvInt_Fallback_Unset(t *testing.T) {
	os.Unsetenv("TEST_INT_UNSET")
	got := getEnvInt("TEST_INT_UNSET", 7)
	if got != 7 {
		t.Errorf("got %d, want 7", got)
	}
}

func TestGetEnvInt_Fallback_Invalid(t *testing.T) {
	t.Setenv("TEST_INT_BAD", "notanumber")
	got := getEnvInt("TEST_INT_BAD", 5)
	if got != 5 {
		t.Errorf("got %d, want 5", got)
	}
}

func TestGetEnvDuration_ExplicitValue(t *testing.T) {
	t.Setenv("TEST_DUR", "500ms")
	got := getEnvDuration("TEST_DUR", 2*time.Second)
	if got != 500*time.Millisecond {
		t.Errorf("got %v, want 500ms", got)
	}
}

func TestGetEnvDuration_Fallback_Unset(t *testing.T) {
	os.Unsetenv("TEST_DUR_UNSET")
	got := getEnvDuration("TEST_DUR_UNSET", 3*time.Second)
	if got != 3*time.Second {
		t.Errorf("got %v, want 3s", got)
	}
}

func TestGetEnvDuration_Fallback_Invalid(t *testing.T) {
	t.Setenv("TEST_DUR_BAD", "notaduration")
	got := getEnvDuration("TEST_DUR_BAD", time.Minute)
	if got != time.Minute {
		t.Errorf("got %v, want 1m0s", got)
	}
}

func TestSplitCSV_Single(t *testing.T) {
	got := splitCSV("localhost:9092")
	if len(got) != 1 || got[0] != "localhost:9092" {
		t.Errorf("got %v", got)
	}
}

func TestSplitCSV_Multiple(t *testing.T) {
	got := splitCSV("broker1:9092, broker2:9092, broker3:9092")
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3: %v", len(got), got)
	}
	if got[1] != "broker2:9092" {
		t.Errorf("second entry: got %q, want %q", got[1], "broker2:9092")
	}
}

func TestSplitCSV_Empty_ReturnsDefault(t *testing.T) {
	got := splitCSV("")
	if len(got) != 1 || got[0] != "localhost:9092" {
		t.Errorf("got %v, want default [localhost:9092]", got)
	}
}

// ---- Metric calculations -----------------------------------------------

func newTestApp() *App {
	return &App{
		cfg:     Config{MaxOpenConns: 10},
		metrics: &Metrics{},
	}
}

func TestAvgDirectWriteLatencyMS_NoWrites(t *testing.T) {
	app := newTestApp()
	if got := app.avgDirectWriteLatencyMS(); got != 0 {
		t.Errorf("got %f, want 0 before any writes", got)
	}
}

func TestAvgDirectWriteLatencyMS_KnownValues(t *testing.T) {
	app := newTestApp()
	// Simulate 4 writes each taking 8ms.
	app.metrics.directRowsWritten.Store(4)
	app.metrics.directLatencyNanos.Store(uint64(4 * 8 * time.Millisecond))

	got := app.avgDirectWriteLatencyMS()
	if got != 8.0 {
		t.Errorf("got %f, want 8.0", got)
	}
}

func TestDirectServiceRatePerConn_NoWrites(t *testing.T) {
	app := newTestApp()
	if got := app.directServiceRatePerConn(); got != 0 {
		t.Errorf("got %f, want 0", got)
	}
}

func TestDirectServiceRatePerConn_KnownValues(t *testing.T) {
	app := newTestApp()
	// 1000 rows, each taking 8ms → total 8s → rate = 1000/8 = 125 writes/sec.
	app.metrics.directRowsWritten.Store(1000)
	app.metrics.directLatencyNanos.Store(uint64(1000 * 8 * time.Millisecond))

	got := app.directServiceRatePerConn()
	const want = 125.0
	if got < want*0.99 || got > want*1.01 {
		t.Errorf("got %f, want ~%f", got, want)
	}
}

func TestWorkerBatchThroughput_NoWrites(t *testing.T) {
	app := newTestApp()
	if got := app.workerBatchThroughput(); got != 0 {
		t.Errorf("got %f, want 0", got)
	}
}

func TestWorkerBatchThroughput_IsDistinctFromDirectRate(t *testing.T) {
	app := newTestApp()
	// Direct path: 10 rows at 8ms each.
	app.metrics.directRowsWritten.Store(10)
	app.metrics.directLatencyNanos.Store(uint64(10 * 8 * time.Millisecond))

	// Worker path: 100 rows in a single 20ms COPY flush.
	app.metrics.workerRowsWritten.Store(100)
	app.metrics.workerLatencyNanos.Store(uint64(20 * time.Millisecond))

	directRate := app.directServiceRatePerConn()
	workerRate := app.workerBatchThroughput()

	// They must differ: direct is per-row cost, worker is batch throughput.
	if directRate == workerRate {
		t.Errorf("direct rate (%f) and worker throughput (%f) should differ", directRate, workerRate)
	}
	// Worker throughput should be much higher (5000 vs 125).
	if workerRate <= directRate {
		t.Errorf("worker throughput (%f) should exceed direct rate (%f)", workerRate, directRate)
	}
}

func TestUtilizationRho_NoData(t *testing.T) {
	app := newTestApp()
	if got := app.utilizationRho(); got != 0 {
		t.Errorf("got %f, want 0 with no data", got)
	}
}

func TestUtilizationRho_KnownValues(t *testing.T) {
	app := newTestApp()
	// mu = 100 writes/sec/conn (10ms per write), c = 10, lambda = 500 rps.
	// rho = 500 / (10 * 100) = 0.5.
	app.metrics.directRowsWritten.Store(1000)
	app.metrics.directLatencyNanos.Store(uint64(1000 * 10 * time.Millisecond)) // mu = 100

	// Manually set the rolling window to simulate 500 RPS.
	app.metrics.arrivalRateCurrent.Store(500 * 1000) // stored as milliRPS

	got := app.utilizationRho()
	const want = 0.5
	if got < want*0.99 || got > want*1.01 {
		t.Errorf("got %f, want ~%f", got, want)
	}
}

// TestUtilizationRho_UsesDirectMetricsOnly verifies that worker writes
// do not inflate utilization_rho. This was the original bug: shared counters
// caused COPY batch throughput to inflate mu, making rho artificially low.
func TestUtilizationRho_UsesDirectMetricsOnly(t *testing.T) {
	app := newTestApp()

	// Direct path: mu = 100 writes/sec.
	app.metrics.directRowsWritten.Store(100)
	app.metrics.directLatencyNanos.Store(uint64(100 * 10 * time.Millisecond))

	// Worker path: very fast COPY (5000 writes/sec aggregate).
	app.metrics.workerRowsWritten.Store(5000)
	app.metrics.workerLatencyNanos.Store(uint64(1000 * time.Millisecond))

	app.metrics.arrivalRateCurrent.Store(500 * 1000) // 500 RPS

	rho := app.utilizationRho()
	// rho should be 500 / (10 * 100) = 0.5, NOT inflated by the worker's 5000 rate.
	const want = 0.5
	if rho < want*0.99 || rho > want*1.01 {
		t.Errorf("rho = %f, want ~%f; worker metrics must not affect rho", rho, want)
	}
}

// ---- Rolling arrival rate window ---------------------------------------

func TestArrivalRatePerSecond_ZeroBeforeWindowTick(t *testing.T) {
	app := newTestApp()
	// No window tick yet; arrivalRateCurrent is 0.
	if got := app.arrivalRatePerSecond(); got != 0 {
		t.Errorf("got %f, want 0 before first window tick", got)
	}
}

func TestArrivalRatePerSecond_ReflectsWindowSnapshot(t *testing.T) {
	app := newTestApp()
	// Simulate what runArrivalRateWindow writes after a 10s tick with 3000 requests:
	// milliRPS = (3000 * 1000) / 10 = 300_000.
	app.metrics.arrivalRateCurrent.Store(300_000)

	got := app.arrivalRatePerSecond()
	const want = 300.0
	if got != want {
		t.Errorf("got %f, want %f", got, want)
	}
}

func TestArrivalRatePerSecond_IsNotLifetimeAverage(t *testing.T) {
	app := newTestApp()
	// Even if many requests have arrived (requestsTotal large), the reported
	// arrival rate only reflects what the window goroutine last stored.
	app.metrics.requestsTotal.Store(1_000_000) // very high lifetime total
	app.metrics.arrivalRateCurrent.Store(50_000) // window says 50 RPS

	got := app.arrivalRatePerSecond()
	const want = 50.0
	if got != want {
		t.Errorf("got %f, want %f (must not use lifetime total)", got, want)
	}
}

// ---- /healthz handler --------------------------------------------------

func TestHealthHandler(t *testing.T) {
	app := &App{cfg: Config{}, metrics: &Metrics{}}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	app.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body.status: got %q, want %q", body["status"], "ok")
	}
}

// ---- writeJSON helper --------------------------------------------------

func TestWriteJSON_SetsHeaderAndStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusCreated, map[string]string{"key": "value"})

	if rec.Code != http.StatusCreated {
		t.Errorf("status: got %d, want 201", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type: got %q", ct)
	}

	var got map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["key"] != "value" {
		t.Errorf("body: got %v", got)
	}
}

// ---- maxInt64 helper ---------------------------------------------------

func TestMaxInt64(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{5, 3, 5},
		{3, 5, 5},
		{0, 0, 0},
		{-1, 1, 1},
	}
	for _, c := range cases {
		if got := maxInt64(c.a, c.b); got != c.want {
			t.Errorf("maxInt64(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

