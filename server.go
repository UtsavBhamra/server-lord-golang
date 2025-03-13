package main

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "log"
    "net/http"
    "strings"
    "time"
    "io"
    "sync"

    "github.com/gorilla/mux"
    "github.com/jackc/pgx/v4/pgxpool"
    "strconv"

	"go.opentelemetry.io/otel"
	// "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)
type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
}

type Task struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	PingURL    string    `json:"pingUrl"`
	UserID     int64    `json:"userId"`
	LastPing   time.Time `json:"lastPing"`
	Interval   int       `json:"interval"`
	TaskNumber int       `json:"taskNumber"`
	Status     string    `json:"status"`
}

var db *pgxpool.Pool
type CustomSpanProcessor struct{}

func (c *CustomSpanProcessor) OnStart(parent context.Context, span trace.ReadWriteSpan) {}

func (c *CustomSpanProcessor) OnEnd(span trace.ReadOnlySpan) {
	// Ignore internal OpenTelemetry spans (like otelmux)
	if span.InstrumentationScope().Name == "go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux" {
		return
	}

	// Get span details
	route := span.Name()
	startTime := span.StartTime()
	endTime := span.EndTime()
	duration := endTime.Sub(startTime)

	// Format output
	output := fmt.Sprintf(
		"Route: %s | Start: %s | End: %s | Duration: %dms\n",
		route, startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano), duration.Milliseconds(),
	)

	// Write to stdout only
	os.Stdout.WriteString(output)
}

func (c *CustomSpanProcessor) Shutdown(ctx context.Context) error  { return nil }
func (c *CustomSpanProcessor) ForceFlush(ctx context.Context) error { return nil }

// Initialize OpenTelemetry with the custom span processor
func initTracer() func() {
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(&CustomSpanProcessor{}), // Use custom processor
		trace.WithResource(resource.Empty()),           // No extra metadata
	)

	otel.SetTracerProvider(tp)

	return func() {
		_ = tp.Shutdown(context.Background()) // No logging on shutdown
	}
}


func main() {
    // Initialize the tracer
    shutdown := initTracer()
    defer shutdown()

	// Database connection
	dbURL := "postgres://postgres:1234@localhost:5432/task_tracker"
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatal("Error parsing the dbURL: ", err)
	}

	// Connect to database
	db, err = pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal("Error connecting to database: ", err)
	}
	defer db.Close()

	// Initialize database
	err = initDB(db)
	if err != nil {
		log.Fatal("Error initializing the database: ", err)
	}

	// Start task monitor
	go startTaskMonitor()

	// Setup router
	r := mux.NewRouter()
    r.Use(otelmux.Middleware("task-tracker"))

	r.HandleFunc("/register", registerHandler).Methods("POST")
	r.HandleFunc("/tasks", createTaskHandler).Methods("POST")
	r.HandleFunc("/users/{userId}", getUserHandler).Methods("GET")
	r.HandleFunc("/tasks/{taskId}/heartbeat", heartbeatHandler).Methods("POST")

	// Start server
	port := ":3000"
	log.Println("Server running on port: ", port)
	log.Fatal(http.ListenAndServe(port, r))
}

func initDB(db *pgxpool.Pool) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(255) UNIQUE NOT NULL,
			email VARCHAR(255) UNIQUE NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS tasks (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			ping_url VARCHAR(255),
			user_id INTEGER REFERENCES users(id),
			last_ping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			interval INTEGER NOT NULL,
			task_number INTEGER NOT NULL,
			status VARCHAR(50) DEFAULT 'alive'
		)`,
	}

	for _, query := range queries {
		_, err := db.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("error executing query: %v", err)
		}
	}

	return nil
}

func registerHandler(w http.ResponseWriter, r *http.Request) {
    // Start a span
    ctx, span := otel.Tracer("task-tracker").Start(r.Context(), "registerHandler")
    defer span.End() // Make sure the span ends

	var user User

	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

	query := `INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id`

	// Measure database query time
	dbCtx, dbSpan := otel.Tracer("task-tracker").Start(ctx, "dbQuery")
	err := db.QueryRow(dbCtx, query, user.Username, user.Email).Scan(&user.ID)
	dbSpan.End()

	if err != nil {
        dbSpan.RecordError(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

    // Add attributes to DB span
    dbSpan.SetAttributes(
        attribute.String("query", query),
        attribute.String("user", user.Username),
    )

	
	// Response time measurement
	_, respSpan := otel.Tracer("task-tracker").Start(ctx, "sendResponse")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
	respSpan.End() // Ends the response span
}

func createTaskHandler(w http.ResponseWriter, r *http.Request) {
    log.Printf("Received request with Content-Type: %s", r.Header.Get("Content-Type"))

    body, err := io.ReadAll(r.Body)
    if err != nil {
        log.Printf("Error reading body: %v", err)
        http.Error(w, "Error reading request body", http.StatusBadRequest)
        return
    }
    log.Printf("Received body: %s", string(body))

    // Intermediate struct for JSON decoding (string values)
    var rawTask struct {
        Name       string `json:"name"`
        PingURL    string `json:"pingUrl"`
        UserID     string `json:"userId"`
        Interval   string `json:"interval"`
        TaskNumber string `json:"taskNumber"`
    }

    if err := json.Unmarshal(body, &rawTask); err != nil {
        log.Printf("Error decoding JSON: %v", err)
        http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
        return
    }

    // Convert string values to integers
    userID, err := strconv.ParseInt(rawTask.UserID, 10, 64)
    if err != nil {
        log.Printf("Invalid userId: %v", err)
        http.Error(w, "Invalid userId value", http.StatusBadRequest)
        return
    }

    interval, err := strconv.Atoi(rawTask.Interval)
    if err != nil {
        log.Printf("Invalid interval: %v", err)
        http.Error(w, "Invalid interval value", http.StatusBadRequest)
        return
    }

    taskNumber, err := strconv.Atoi(rawTask.TaskNumber)
    if err != nil {
        log.Printf("Invalid taskNumber: %v", err)
        http.Error(w, "Invalid taskNumber value", http.StatusBadRequest)
        return
    }

    // Validate required fields
    if rawTask.Name == "" || interval <= 0 || taskNumber <= 0 {
        log.Printf("Missing required fields: name=%s, interval=%d, taskNumber=%d", rawTask.Name, interval, taskNumber)
        http.Error(w, "Missing or invalid required fields", http.StatusBadRequest)
        return
    }

    // Create task object
    task := Task{
        Name:       rawTask.Name,
        PingURL:    rawTask.PingURL,
        UserID:     userID,
        Interval:   interval,
        TaskNumber: taskNumber,
    }

    // Insert into database
    query := `INSERT INTO tasks (name, ping_url, user_id, interval, task_number, status) 
              VALUES ($1, $2, $3, $4, $5, 'alive') 
              RETURNING id, last_ping`

    err = db.QueryRow(context.Background(), query, task.Name, task.PingURL, task.UserID, task.Interval, task.TaskNumber).Scan(&task.ID, &task.LastPing)
    if err != nil {
        log.Printf("Database error: %v", err)
        http.Error(w, "Failed to create task", http.StatusInternalServerError)
        return
    }

    // Set response
    task.Status = "alive"
    w.WriteHeader(http.StatusCreated)
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(task)
}


func getUserHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["userId"]

	var user User
	userQuery := `SELECT id, username, email FROM users WHERE id = $1`
	err := db.QueryRow(context.Background(), userQuery, userID).Scan(&user.ID, &user.Username, &user.Email)
	if err != nil {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	// Get user's tasks
	tasksQuery := `SELECT id, name, ping_url, last_ping, interval, task_number, status 
		FROM tasks WHERE user_id = $1`
	rows, err := db.Query(context.Background(), tasksQuery, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var task Task
		err := rows.Scan(
			&task.ID,
			&task.Name,
			&task.PingURL,
			&task.LastPing,
			&task.Interval,
			&task.TaskNumber,
			&task.Status,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tasks = append(tasks, task)
	}

	response := struct {
		User  User   `json:"user"`
		Tasks []Task `json:"tasks"`
	}{
		User:  user,
		Tasks: tasks,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
    ctx, span := otel.Tracer("task-tracker").Start(r.Context(), "heartbeatHandler")
    defer span.End()

	vars := mux.Vars(r)
	taskID := vars["taskId"]

	query := `
		UPDATE tasks
		SET last_ping = CURRENT_TIMESTAMP, status = 'alive'
		WHERE task_number = $1
		RETURNING id`

	var id int64

    // Instrument database query
    dbCtx, dbSpan := otel.Tracer("task-tracker").Start(ctx, "dbQuery")
	err := db.QueryRow(dbCtx, query, taskID).Scan(&id)  //Execute the db query
    dbSpan.End() // Ends the database query span

    //Error handling
	if err != nil {
        dbSpan.RecordError(err)
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

    // Add attributes to DB span
    dbSpan.SetAttributes(
        attribute.String("query", query),
        attribute.String("taskID", taskID),
    )

    // Instrument response writing
    _, respSpan := otel.Tracer("task-tracker").Start(ctx, "sendResponse")
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"message": "Heartbeat received"})
    respSpan.End() // Ends the response span
}

// taskmonitoring function
// ShardInfo tracks monitoring information about shards
type ShardInfo struct {
    Name          string
    LastMonitored time.Time
}

// Global map to track when each shard was last monitored
var shardMonitoringInfo = make(map[string]*ShardInfo)
var shardMutex sync.RWMutex

func checkTaskStatus() {
    // Start an OpenTelemetry span
    ctx, span := otel.Tracer("task-tracker").Start(context.Background(), "checkTaskStatus")
    startTime := time.Now()
    defer func() {
        endTime := time.Now()
        duration := endTime.Sub(startTime)
        span.End()

        // Print timing information
        output := fmt.Sprintf("Route: checkTaskStatus | Start: %s | End: %s | Duration: %dms\n",
            startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano), duration.Milliseconds(),
        )
        os.Stdout.WriteString(output)
    }()

    log.Println("Fetching shards for task monitoring...")

    // Fetch shard names from Citus metadata
    shardQuery := `SELECT shard_name FROM citus_shards WHERE table_name = (SELECT oid FROM pg_class WHERE relname = 'tasks');`

    shardRows, err := db.Query(ctx, shardQuery)
    if err != nil {
        log.Printf("Error fetching shard names: %v", err)
        return
    }
    defer shardRows.Close()

    var shards []string
    for shardRows.Next() {
        var shardName string
        if err := shardRows.Scan(&shardName); err != nil {
            log.Printf("Error scanning shard name: %v", err)
            continue
        }
        shards = append(shards, shardName)
    }

    if len(shards) == 0 {
        log.Println("No task shards found.")
        return
    }

    log.Printf("Found %d shards: %v", len(shards), shards)

    // Create a semaphore with fixed capacity
    maxConcurrentShards := 3
    sem := make(chan struct{}, maxConcurrentShards)
    
    // Create a wait group to wait for all goroutines to finish
    var wg sync.WaitGroup

    // Track monitoring stats
    var monitoringSummary struct {
        sync.Mutex
        TotalTasks     int
        AliveTasks     int
        DeadTasks      int
        UpdatedTasks   int
        WarningTasks   int  // Tasks approaching their timeout
    }

    // Process each shard
    for _, shard := range shards {
        // Check if this shard needs monitoring
        shardMutex.RLock()
        info, exists := shardMonitoringInfo[shard]
        needsMonitoring := !exists || time.Since(info.LastMonitored) > 15*time.Second
        shardMutex.RUnlock()

        if !needsMonitoring {
            log.Printf("Skipping shard %s - recently monitored at %v", shard, info.LastMonitored)
            continue
        }

        // Increment wait group counter
        wg.Add(1)
        
        // Acquire semaphore slot (this will block if all slots are in use)
        sem <- struct{}{}
        
        // Process shard in a separate goroutine
        go func(shardName string) {
            defer wg.Done()
            defer func() { <-sem }() // Release semaphore when done
            
            // Create a child span for this shard
            shardCtx, shardSpan := otel.Tracer("task-tracker").Start(ctx, fmt.Sprintf("process-shard-%s", shardName))
            defer shardSpan.End()
            
            log.Printf("Processing shard: %s", shardName)
            
            // Update task statuses in this shard
            updateQuery := fmt.Sprintf(`
                UPDATE %s
                SET status = 'dead'
                WHERE 
                    status = 'alive' 
                    AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_ping)) > interval
                RETURNING id;`, shardName)

            // Create DB span for update operation
            dbUpdateCtx, dbUpdateSpan := otel.Tracer("task-tracker").Start(shardCtx, "update-tasks")
            updateRows, err := db.Query(dbUpdateCtx, updateQuery)
            dbUpdateSpan.End()

            if err != nil {
                dbUpdateSpan.RecordError(err)
                log.Printf("Error updating tasks in shard %s: %v", shardName, err)
                return
            }
            defer updateRows.Close()

            var updatedTasks []int
            for updateRows.Next() {
                var taskID int
                if err := updateRows.Scan(&taskID); err != nil {
                    log.Printf("Error scanning updated task ID in shard %s: %v", shardName, err)
                    continue
                }
                updatedTasks = append(updatedTasks, taskID)
            }

            // Track updated tasks count
            monitoringSummary.Lock()
            monitoringSummary.UpdatedTasks += len(updatedTasks)
            monitoringSummary.Unlock()

            if len(updatedTasks) > 0 {
                log.Printf("Shard %s: Updated %d tasks to 'dead' state: %v", shardName, len(updatedTasks), updatedTasks)
            }

            // Fetch all tasks and log their statuses with more detailed information
            query := fmt.Sprintf(`
                SELECT 
                    id, 
                    name,
                    task_number,
                    status, 
                    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_ping)) AS time_diff, 
                    interval,
                    last_ping
                FROM %s;`, shardName)

            // Create DB span for fetch operation
            dbFetchCtx, dbFetchSpan := otel.Tracer("task-tracker").Start(shardCtx, "fetch-tasks")
            taskRows, err := db.Query(dbFetchCtx, query)
            dbFetchSpan.End()

            if err != nil {
                dbFetchSpan.RecordError(err)
                log.Printf("Error fetching task statuses from shard %s: %v", shardName, err)
                return
            }
            defer taskRows.Close()

            // Local counters for this shard
            aliveCount := 0
            deadCount := 0
            warningCount := 0
            
            // Log output for this shard
            fmt.Printf("\n===== SHARD %s STATUS REPORT =====\n", shardName)
            fmt.Printf("%-5s | %-20s | %-10s | %-8s | %-15s | %-10s | %s\n", 
                "ID", "Name", "Task#", "Status", "Last Ping", "Interval", "Time Since Ping")
            fmt.Println(strings.Repeat("-", 95))

            for taskRows.Next() {
                var taskID int
                var name string
                var taskNumber int
                var status string
                var timeDiff float64
                var interval int
                var lastPing time.Time
                
                if err := taskRows.Scan(&taskID, &name, &taskNumber, &status, &timeDiff, &interval, &lastPing); err != nil {
                    log.Printf("Error scanning task row in shard %s: %v", shardName, err)
                    continue
                }
                
                // Update counters
                if status == "alive" {
                    aliveCount++
                } else {
                    deadCount++
                }
                
                // Check if task is approaching timeout (> 80% of interval passed)
                isWarning := status == "alive" && timeDiff > float64(interval)*0.8
                if isWarning {
                    warningCount++
                }
                
                // Print status with color coding for better visibility
                statusDisplay := status
                timeSinceStr := fmt.Sprintf("%.1f sec", timeDiff)
                
                // Output task status (always print status for visibility)
                fmt.Printf("%-5d | %-20s | %-10d | %-8s | %-15s | %-10d | %s%s\n",
                    taskID,
                    truncateString(name, 20),
                    taskNumber,
                    statusDisplay,
                    lastPing.Format("15:04:05"),
                    interval,
                    timeSinceStr,
                    warningNote(isWarning, interval, timeDiff),
                )
            }
            
            fmt.Printf("\nSHARD SUMMARY: %d total tasks (%d alive, %d dead, %d warnings)\n", 
                aliveCount+deadCount, aliveCount, deadCount, warningCount)
            fmt.Println(strings.Repeat("=", 50))
            
            // Update global counters
            monitoringSummary.Lock()
            monitoringSummary.TotalTasks += (aliveCount + deadCount)
            monitoringSummary.AliveTasks += aliveCount
            monitoringSummary.DeadTasks += deadCount
            monitoringSummary.WarningTasks += warningCount
            monitoringSummary.Unlock()
            
            // Update last monitored timestamp
            shardMutex.Lock()
            shardMonitoringInfo[shardName] = &ShardInfo{
                Name:          shardName,
                LastMonitored: time.Now(),
            }
            shardMutex.Unlock()
            
            log.Printf("Finished processing shard: %s", shardName)
        }(shard)
    }
    
    // Wait for all goroutines to complete
    wg.Wait()
    
    // Print overall summary
    fmt.Printf("\n===== MONITORING SUMMARY =====\n")
    fmt.Printf("Total Tasks: %d\n", monitoringSummary.TotalTasks)
    fmt.Printf("Alive Tasks: %d\n", monitoringSummary.AliveTasks)
    fmt.Printf("Dead Tasks: %d\n", monitoringSummary.DeadTasks)
    fmt.Printf("Tasks Updated to Dead: %d\n", monitoringSummary.UpdatedTasks)
    fmt.Printf("Warning Tasks (approaching timeout): %d\n", monitoringSummary.WarningTasks)
    fmt.Println(strings.Repeat("=", 30))
    
    log.Println("Completed task status check for all shards")
}

// Helper functions for formatting output
func truncateString(s string, maxLen int) string {
    if len(s) <= maxLen {
        return s
    }
    return s[:maxLen-3] + "..."
}

func warningNote(isWarning bool, interval int, timeDiff float64) string {
    if !isWarning {
        return ""
    }
    
    percentRemaining := 100 - (timeDiff * 100 / float64(interval))
    return fmt.Sprintf(" WARNING: %.1f%% time remaining", percentRemaining)
}

func startTaskMonitor() {
    log.Println("Starting task status monitor...")
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        checkTaskStatus()
    }
}
