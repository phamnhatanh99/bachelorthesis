# BScThesisNhatAnhPham


## Implementation of a Distributed Dataset Search Infrastructure


## Installation
To build the jar files, run the following command in the root directory of the project:

```mvn clean install```

The build process requires a running MongoDB database at localhost:27017 for the tests to pass. This can be skipped by running the following command:

```mvn clean install -DskipTests```

The jar file for the client application is located in dsclient/target, while the jar file for the server application is located in dsserver/target.

## Usage

To start the system, a running MongoDB server is required. 

First, start the server application. By default, the server is connected to a MongoDB server at localhost:27017. This can be changed at startup by providing the additional arguments `--mongo-host=<host>` and `--mongo-port=<port>`. The server will start listening for incoming connections on port 8080. This port can be changed by providing the additional argument `-Dserver.port=<port>`.

Then, start the client application. By default, the client will connect to the server at localhost:8080. This can be changed at startup by providing the additional argument `--base-url=<url>`. The web interface of the client can be accessed at `http://localhost:8081`.

## Tests
The three datasets used for evaluation in the thesis can be found [here](https://delftdata.github.io/valentine/) (TPC-DI, WikiData) and [here](https://github.com/RJMillerLab/table-union-search-benchmark) (synthesized Open Data).