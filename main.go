package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
	"github.com/onrik/ethrpc"
	"os"
)

var db *sql.DB

// env variables
const (
	dbHost  = "DB_HOST"
	dbPort  = "DB_PORT"
	dbUsr   = "DB_USR"
	dbPwd   = "DB_PWD"
	dbName  = "DB_NAME"
	ethNode = "ETH_NODE"
)

type Config struct {
	dbHost  string
	dbPort  string
	dbUser  string
	dbPass  string
	dbName  string
	ethNode string
}

func loadConfig() Config {
	conf := Config{}

	conf.dbName = os.Getenv(dbName)
	conf.dbHost = os.Getenv(dbHost)
	conf.dbPass = os.Getenv(dbPwd)
	conf.dbPort = os.Getenv(dbPort)
	conf.dbUser = os.Getenv(dbUsr)
	conf.ethNode = os.Getenv(ethNode)

	return conf
}

func connectDB(c Config) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.dbHost, c.dbPort, c.dbUser, c.dbPass, c.dbName)

	var err error
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}
}

func readLastBlocksProcessed(tgType string) int {
	var blockNo int
	q := fmt.Sprintf("SELECT %s_last_block_processed FROM %s", tgType, "state")
	err := db.QueryRow(q).Scan(&blockNo)
	if err != nil {
		panic(err)
	}
	return blockNo
}

func fetchLastBlockFromInfura(url string) int {
	cli := ethrpc.New(url)
	lastSeen, err := cli.EthBlockNumber()
	if err != nil {
		panic(err)
	}
	return lastSeen
}

func HandleRequest(ctx context.Context, event events.CloudWatchEvent) {

	config := loadConfig()
	connectDB(config)

	lastWaT := readLastBlocksProcessed("wat")
	lastWaC := readLastBlocksProcessed("wac")
	lastWaE := readLastBlocksProcessed("wae")

	lastInfura := fetchLastBlockFromInfura(config.ethNode)

	deltaWaT := lastInfura - lastWaT
	deltaWaC := lastInfura - lastWaC
	deltaWaE := lastInfura - lastWaE

	fmt.Printf("DELTAS: WaT: %d, WaC: %d, WaE: %d\n", deltaWaT, deltaWaC, deltaWaE)

	if deltaWaT > 16 || deltaWaC > 32 || deltaWaE > 16 {
		panic("Zoroaster is more than 16 blocks behind Infura")
	}
}

func main() {
	lambda.Start(HandleRequest)
}
