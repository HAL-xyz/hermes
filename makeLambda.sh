gofmt -s -w .
rm lambda.zip
env GOOS=linux GOARCH=amd64 go build -o lambda
chmod 777 lambda
zip -j lambda.zip lambda
rm lambda
