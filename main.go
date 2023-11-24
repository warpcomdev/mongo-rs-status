package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	TimeoutSeconds int    = 10
	AdminDbName    string = "admin"
	DefaultURI     string = "mongodb://localhost:27017"
)

// connect to the mongo database, log.Fatal on error
func connect(uri string, timeout time.Duration) *mongo.Client {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal("Failed to connect to mongo: ", err)
	}
	return client
}

// get replication status, log.Fatal on error
func getRsStatus(client *mongo.Client, admindb string, timeout time.Duration) *mongo.SingleResult {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	result := client.Database(admindb).RunCommand(ctx, bson.D{{"replSetGetStatus", 1}})
	if err := result.Err(); err != nil {
		log.Fatal("Failed to run replSetGetStatus: ", err)
	}
	return result
}

// Initiate the replicaset, if not already initiated
func rsInitiate(client *mongo.Client, admindb string, document []byte, timeout time.Duration) *mongo.SingleResult {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	var command bson.D
	if err := bson.UnmarshalExtJSON(document, true, &command); err != nil {
		log.Fatal("failed to parse replicaSet document: ", err)
	}
	result := client.Database(admindb).RunCommand(ctx, bson.D{{"replSetInitiate", command}})
	if err := result.Err(); err != nil {
		log.Fatal("Failed to run replSetGetStatus: ", err)
	}
	return result
}

// parse flags and return replication status
func main() {
	var (
		timeoutFlag  int
		adminDbFlag  string
		uriDbFlag    string
		initiateFlag string
	)

	flag.IntVar(&timeoutFlag, "timeout", TimeoutSeconds, "timeout for calls to mongodb")
	flag.StringVar(&adminDbFlag, "admindb", AdminDbName, "name of the admin db")
	flag.StringVar(&uriDbFlag, "uri", "", "mongo URI")
	flag.StringVar(&initiateFlag, "initiate", "", "initiate replicaset")
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

	timeoutDuration := time.Duration(timeoutFlag) * time.Second
	client := connect(uriDbFlag, timeoutDuration)
	defer func() {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutDuration)
		defer cancelFunc()
		if err := client.Disconnect(ctx); err != nil {
			log.Fatal("failed to disconnect from mongo: ", err)
		}
	}()
	result := getRsStatus(client, adminDbFlag, timeoutDuration)

	// if initiateFlag != "", initialize the replicaSet with the given document
	if initiateFlag != "" {
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
		result = rsInitiate(client, adminDbFlag, initiateDoc, timeoutDuration)
	}

	raw, err := result.Raw()
	if err != nil {
		log.Fatal("failed to decode result: ", err)
	}
	str, err := bson.MarshalExtJSONIndent(raw, false, false, "", "  ")
	if err != nil {
		log.Fatal("failed to marshal result: ", err)
	}
	fmt.Printf("%s\n", string(str))
}
