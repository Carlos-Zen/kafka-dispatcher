The project was built for dispatcher kafka event to command processes.Act as a middleware by subscribe kafka consumer group.Its simple and easy to use.  

### Usage
Instruction for several config  
topics: A comma-separated list of topics to consume  
zookeeper: A comma-separated Zookeeper connection string  
command: (e.g. ·/usr/bin/php·)  
params: (e.g. ·/data/shell/test.php·)  

### Source Path  
path: $GOPATH/uuzu.com/

### Run
go run main.go

### Build
go build main.go

