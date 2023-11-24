package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultPort    int    = 20000
	TimeoutSeconds int    = 10
	AdminDbName    string = "admin"
	DefaultURI     string = "mongodb://localhost:27017"
)

// connect to the mongo database.
func connect(uri string, timeout time.Duration) (*mongo.Client, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connection error: %w", err)
	}
	return client, nil
}

// get replication status
func getRsStatus(client *mongo.Client, admindb string, timeout time.Duration) (*mongo.SingleResult, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	result := client.Database(admindb).RunCommand(ctx, bson.D{{"replSetGetStatus", 1}})
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("replSetGetStatus error: %w", err)
	}
	return result, nil
}

// Initiate the replicaset
func rsInitiate(client *mongo.Client, admindb string, document []byte, timeout time.Duration) (*mongo.SingleResult, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	var command bson.D
	if err := bson.UnmarshalExtJSON(document, true, &command); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}
	result := client.Database(admindb).RunCommand(ctx, bson.D{{"replSetInitiate", command}})
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("replSetInitiate error: %w")
	}
	return result, nil
}

// dump a mongo.SingleResult to json
func singleResultJson(result *mongo.SingleResult) ([]byte, error) {
	raw, err := result.Raw()
	if err != nil {
		return nil, fmt.Errorf("raw error: %w", err)
	}
	str, err := bson.MarshalExtJSONIndent(raw, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}
	return str, nil
}

// HTTP Request handler
type handler struct {
	Timeout time.Duration
	AdminDb string
	URI     string
}

// GET request: return replicaSet status
func (h *handler) GET(w http.ResponseWriter, r *http.Request) (*mongo.SingleResult, error) {
	client, err := connect(h.URI, h.Timeout)
	if err != nil {
		return nil, err
	}
	return getRsStatus(client, h.AdminDb, h.Timeout)
}

// POST request: initiate a replicaSet
func (h *handler) POST(w http.ResponseWriter, r *http.Request) (*mongo.SingleResult, error) {
	client, err := connect(h.URI, h.Timeout)
	if err != nil {
		return nil, err
	}
	initiateDoc, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body error: %w", err)
	}
	return rsInitiate(client, h.AdminDb, initiateDoc, h.Timeout)
}

// serve HTTP requests
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// exhaust the request body on return
	defer func() {
		if r.Body != nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
	}()
	// Manage HTTP method (GET, POST, others)
	var (
		result *mongo.SingleResult
		err    error
	)
	switch r.Method {
	case http.MethodGet:
		result, err = h.GET(w, r)
	case http.MethodPost:
		contentType := r.Header.Get("Content-Type")
		if contentType != "application/json" {
			http.Error(w, fmt.Sprintf("unsupported content type %s", contentType), http.StatusUnsupportedMediaType)
			return
		}
		result, err = h.POST(w, r)
	default:
		http.Error(w, fmt.Sprintf("unsupported method %s", r.Method), http.StatusMethodNotAllowed)
		return
	}
	// If no error while handling, encode result
	str, err := singleResultJson(result)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Else, return json response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	n, err := w.Write(str)
	if err != nil {
		log.Printf("failed to serve request after %d bytes: %s", n, err)
	}
}

// parse flags and return replication status
func main() {
	var (
		portFlag    int
		timeoutFlag int
		uriDbFlag   string
		adminDbFlag string
	)

	flag.IntVar(&portFlag, "port", DefaultPort, "HTTP port to listen on")
	flag.IntVar(&timeoutFlag, "timeout", TimeoutSeconds, "timeout for calls to mongodb")
	flag.StringVar(&adminDbFlag, "admindb", AdminDbName, "name of the admin db")
	flag.StringVar(&uriDbFlag, "uri", "", "mongo URI")
	flag.Parse()

	if portFlag <= 1024 || portFlag >= 65536 {
		log.Fatal("allowed port values are between 1025 and 65535")
	}
	if timeoutFlag < 1 || timeoutFlag > 1800 {
		log.Fatal("allowed timeout values are between 1 and 1800 seconds")
	}
	if adminDbFlag == "" {
		log.Fatal("admindb name must not be empty")
	}
	if uriDbFlag == "" {
		uriDbFlag = os.Getenv("MONGODB_URI")
	}
	if uriDbFlag == "" {
		uriDbFlag = DefaultURI
	}

	timeoutDuration := time.Duration(timeoutFlag) * time.Second
	server := http.Server{
		Addr: fmt.Sprintf(":%d", portFlag),
		Handler: &handler{
			Timeout: timeoutDuration,
			AdminDb: adminDbFlag,
			URI:     uriDbFlag,
		},
		ReadTimeout:  3 * timeoutDuration,
		WriteTimeout: 3 * timeoutDuration,
		IdleTimeout:  3 * timeoutDuration,
	}
	fmt.Printf("Listening at port %d", portFlag)
	panic(server.ListenAndServe())
}
