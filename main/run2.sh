 go build -buildmode=plugin ../mrapps/wc.go
 go build mrworker.go
 ./mrworker wc.so
