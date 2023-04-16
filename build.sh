kill -9 $(lsof -t -i tcp:8000)
go build
go run main.go