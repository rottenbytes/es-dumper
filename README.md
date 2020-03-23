# ES dumper

Super simple tool to dump an ES index to local file. Written because elastic-dump is slooooooow
No options, no tests, use at your own risk

## Build

`go get ; CGO_ENABLED=0 GOOS=linux go build -v -o es-dumper .`
