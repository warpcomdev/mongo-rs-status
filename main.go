package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
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
		return nil, err
	}
	return client, nil
}

// Disconnect from the mongo database
func disconnect(client *mongo.Client, timeout time.Duration) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	return client.Disconnect(ctx)
}

// get replication status
func getRsStatus(client *mongo.Client, admindb string, timeout time.Duration) (*mongo.SingleResult, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	result := client.Database(admindb).RunCommand(ctx, bson.D{{"replSetGetStatus", 1}})
	if err := result.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Initiate the replicaset
func rsInitiate(client *mongo.Client, admindb string, document []byte, timeout time.Duration) (*mongo.SingleResult, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	var command bson.D
	if err := bson.UnmarshalExtJSON(document, true, &command); err != nil {
		return nil, fmt.Errorf("rsInitiate::unmarshal: %w", err)
	}
	result := client.Database(admindb).RunCommand(ctx, bson.D{{"replSetInitiate", command}})
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("rsInitiate::runCommand: %w", err)
	}
	return result, nil
}

// dump a mongo.SingleResult to json
func singleResultJson(result *mongo.SingleResult) ([]byte, error) {
	raw, err := result.Raw()
	if err != nil {
		return nil, fmt.Errorf("singleResult::raw: %w", err)
	}
	str, err := bson.MarshalExtJSONIndent(raw, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("singleResult::marshal: %w", err)
	}
	return str, nil
}

// HttpError is an error that includes HTTP Status Code
type HttpError interface {
	error
	Code() int
}

type httpError struct {
	err  error
	code int
}

// Error implements Error
func (e httpError) Error() string {
	return e.err.Error()
}

// Unwrap implements Unwrap
func (e httpError) Unwrap() error {
	return e.err
}

// Code implements Code
func (e httpError) Code() int {
	return e.code
}

// HTTP Request handler
type handler struct {
	ClientMutex sync.Mutex
	Client      *mongo.Client
	Timeout     time.Duration
	AdminDb     string
	URI         string
}

// Acquire a client connection
func (h *handler) acquireClient() (*mongo.Client, error) {
	h.ClientMutex.Lock()
	defer h.ClientMutex.Unlock()
	if h.Client == nil {
		client, err := connect(h.URI, h.Timeout)
		if err != nil {
			return nil, err
		}
		h.Client = client
	}
	return h.Client, nil
}

// Release a client connection
func (h *handler) releaseClient(client *mongo.Client) {
	h.ClientMutex.Lock()
	if h.Client == nil || h.Client != client {
		h.ClientMutex.Unlock()
	} else {
		h.Client = nil
		h.ClientMutex.Unlock()
	}
	disconnect(client, h.Timeout)
}

// writeError writes an error message to the writer
func writeError(w http.ResponseWriter, err error, code int) {
	if codeProvider, ok := err.(HttpError); ok {
		code = codeProvider.Code()
	}
	if err != nil {
		http.Error(w, err.Error(), code)
	}
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
	// Get a client connection
	client, err := h.acquireClient()
	if err != nil {
		writeError(w, err, http.StatusInternalServerError)
		return
	}
	// Manage HTTP method (GET, POST, others)
	var result []byte
	switch r.Method {
	case http.MethodGet:
		result, err = h.GET(client, w, r)
	default:
		err = httpError{err: fmt.Errorf("unsupported method %s", r.Method), code: http.StatusMethodNotAllowed}
	}
	if err != nil {
		writeError(w, err, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	n, err := w.Write(result)
	if err != nil {
		log.Printf("failed to serve request after %d bytes: %s", n, err)
	}
}

// GET request: return replicaSet status
func (h *handler) GET(client *mongo.Client, w http.ResponseWriter, r *http.Request) ([]byte, error) {
	result, err := getRsStatus(client, h.AdminDb, h.Timeout)
	if err != nil {
		// Release client which might be failed
		h.releaseClient(client)
		return nil, fmt.Errorf("GET::getRsStatus: %w", err)
	}
	return singleResultJson(result)
}

// parse flags and return replication status
func main() {
	var (
		timeoutFlag  int
		adminDbFlag  string
		uriDbFlag    string
		initiateFlag string
		serveFlag    bool
		portFlag     int
	)

	flag.IntVar(&timeoutFlag, "timeout", TimeoutSeconds, "timeout for calls to mongodb")
	flag.StringVar(&adminDbFlag, "admindb", AdminDbName, "name of the admin db")
	flag.StringVar(&uriDbFlag, "uri", "", "mongo URI")
	flag.StringVar(&initiateFlag, "initiate", "", "initiate replicaset")
	flag.BoolVar(&serveFlag, "serve", false, "run HTTP server")
	flag.IntVar(&portFlag, "port", DefaultPort, "HTTP port to listen on")
	flag.Parse()

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
	if portFlag <= 1024 || portFlag >= 65536 {
		log.Fatal("allowed port values are between 1025 and 65535")
	}
	timeoutDuration := time.Duration(timeoutFlag) * time.Second

	// If serveFlag given, just serve HTTP requests
	if serveFlag {
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
		log.Fatal(server.ListenAndServe())
	}

	client, err := connect(uriDbFlag, timeoutDuration)
	if err != nil {
		log.Fatal("failed to connect to mongo: ", err)
	}
	defer func() {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutDuration)
		defer cancelFunc()
		if err := client.Disconnect(ctx); err != nil {
			log.Fatal("failed to disconnect from mongo: ", err)
		}
	}()
	var result *mongo.SingleResult

	if initiateFlag == "" {
		result, err = getRsStatus(client, adminDbFlag, timeoutDuration)
		if err != nil {
			log.Fatal("failed to get replicaSet status: ", err)
		}
	} else {
		// if initiateFlag != "", initialize the replicaSet with the given document
		var reader io.Reader
		if initiateFlag == "-" {
			// Read initiation doc from stdin
			fmt.Fprint(os.Stderr, "reading replicaSet config document from stdin")
			reader = os.Stdin
		} else {
			file, err := os.Open(initiateFlag)
			if err != nil {
				log.Fatal("failed to open replicaSet config document: ", err)
			}
			defer file.Close()
			reader = file
		}
		initiateDoc, err := io.ReadAll(reader)
		if err != nil {
			log.Fatal("failed to read replicaSet config document: ", err)
		}
		result, err = rsInitiate(client, adminDbFlag, initiateDoc, timeoutDuration)
		if err != nil {
			log.Fatal("failed to initiate replicaSet: ", err)
		}
	}

	str, err := singleResultJson(result)
	if err != nil {
		log.Fatal("failed to produce string result: ", err)
	}
	fmt.Printf("%s\n", string(str))
}
