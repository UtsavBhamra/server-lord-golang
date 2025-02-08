package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"io"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4/pgxpool"
	"strconv"
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

func main() {
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
	var user User
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := `INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id`
	//instrument the time taken for the rw operations (open telemetry)
	err := db.QueryRow(context.Background(), query, user.Username, user.Email).Scan(&user.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
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
	vars := mux.Vars(r)
	taskID := vars["taskId"]

	query := `
		UPDATE tasks
		SET last_ping = CURRENT_TIMESTAMP, status = 'alive'
		WHERE task_number = $1
		RETURNING id`

	var id int64
	err := db.QueryRow(context.Background(), query, taskID).Scan(&id)
	if err != nil {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "Heartbeat received"})
}

func checkTaskStatus() {
    log.Println("Checking task statuses...")
    
    updateQuery := `
        UPDATE tasks
        SET status = 'dead'
        WHERE 
            status = 'alive' AND
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_ping)) > tasks.interval
        RETURNING id;`

    rows, err := db.Query(context.Background(), updateQuery)
    if err != nil {
        log.Printf("Error updating task status: %v", err)
        return
    }
    defer rows.Close()

    var updatedTasks []int
    for rows.Next() {
        var taskID int
        if err := rows.Scan(&taskID); err != nil {
            log.Printf("Error scanning row: %v", err)
            continue
        }
        updatedTasks = append(updatedTasks, taskID)
    }

    query := `
        SELECT id, status, EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_ping)) AS time_diff, interval FROM tasks;`

    rows, err = db.Query(context.Background(), query)
    if err != nil {
        log.Printf("Error fetching task statuses: %v", err)
        return
    }
    defer rows.Close()

    for rows.Next() {
        var taskID int
        var status string
        var timeDiff float64
        var interval int
        if err := rows.Scan(&taskID, &status, &timeDiff, &interval); err != nil {
            log.Printf("Error scanning row: %v", err)
            continue
        }
        log.Printf("Task %d - Time since last ping: %.2f seconds, Interval: %d seconds, Status: %s", taskID, timeDiff, interval, status)
    }
}

func startTaskMonitor() {
    log.Println("Starting task status monitor...")
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        checkTaskStatus()
    }
}
