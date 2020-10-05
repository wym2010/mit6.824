 #go build -buildmode=plugin ../mrapps/wc.go
 rm mr-out*
 rm mr-*
 go build -race mrmaster.go 
 ./mrmaster pg*.txt 
