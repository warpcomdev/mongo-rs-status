# mongo-rs-status

This app checks the replicaSet status of a mongo cluster, as seeen by one of its members, and (optionally) starts a replicaSet in the cluster.

## Usage

`mongo-rs-status [-uri mongodb://uri] [-timeout timeoutSeconds] [-admindb AdminDB] [-initiate rsConfigDocument]`

Where:

- `-uri` is the mongodb URL of the mongo server to connect to. Defaults to the value of the environment variable `MONGODB_URI`, or `mongodb://localhost:27017` if it is not defined.
- `-timeout` is the timeout for any request to mongo. Defaults to 10 seconds.
- `-admindb` is the name of the administrative db. Defaults to `admin`.

By default, the program prints the result of running the [replSetGetStatus](https://www.mongodb.com/docs/manual/reference/command/replSetGetStatus/) command in the server, if `-initiate` is not provided.

If `-initiate` is provided, with the path to a replicaSet initial configuration document (a json file in the same format as would be used to call [rs.initiate](https://www.mongodb.com/docs/manual/reference/method/rs.initiate/)), the application prints the result of running the [replSetInitiate](https://www.mongodb.com/docs/manual/reference/command/replSetInitiate/) command with that document.

The value of `initiate` can be `-`, in which case the replicaSet initial configuration document is read from stdin.

## Build

To build the image, run:

```bash
docker build --rm -t warpcomdev/mongo-rs-status:latest .
```
