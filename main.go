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
	dbUsr   = "DB_USR"
	dbPwd   = "DB_PWD"
	ethNode = "HERMES_ETH_NODE"
	network = "HERMES_NETWORK"
)

type Config struct {
	dbHost  string
	dbUser  string
	dbPass  string
	dbName  string
	ethNode string
	network string
}

func loadConfig() Config {
	conf := Config{}

	conf.dbName = "hal_prod"
	conf.dbHost = os.Getenv(dbHost)
	conf.dbPass = os.Getenv(dbPwd)
	conf.dbUser = os.Getenv(dbUsr)
	conf.ethNode = os.Getenv(ethNode)
	conf.network = os.Getenv(network)

	if conf.ethNode == "" || conf.network == "" {
		panic("HERMES_ETH_NODE and/or NETWORK not found")
	}
	return conf
}

func connectDB(c Config) {
	psqlInfo := fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=%s sslmode=disable",
		c.dbHost, 5432, c.dbUser, c.dbPass, c.dbName)

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

func readLastBlocksProcessed(tgType, network string) int {
	var blockNo int
	q := fmt.Sprintf(`SELECT %s_last_block_processed FROM state WHERE network_id ='%s'`, tgType, network)
	err := db.QueryRow(q).Scan(&blockNo)
	if err != nil {
		panic(err)
	}
	return blockNo
}

func fetchLastBlockFromControlNode(url string) int {
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
	defer db.Close()

	lastWaT := readLastBlocksProcessed("wat", config.network)
	lastWaC := readLastBlocksProcessed("wac", config.network)
	lastWaE := readLastBlocksProcessed("wae", config.network)

	lastBlockFromControl := fetchLastBlockFromControlNode(config.ethNode)
	fmt.Println("=> last block from control node: ", lastBlockFromControl)

	deltaWaT := lastBlockFromControl - lastWaT
	deltaWaC := lastBlockFromControl - lastWaC
	deltaWaE := lastBlockFromControl - lastWaE

	fmt.Printf("DELTAS: WaT: %d, WaC: %d, WaE: %d\n", deltaWaT, deltaWaC, deltaWaE)

	if deltaWaT > 20 || deltaWaC > 20 || deltaWaE > 20 {
		panic("Zoroaster is more than 20 blocks behind control node")
	}
}

func main() {
	lambda.Start(HandleRequest)
}

//use this to run locally
//func main() {
//	HandleRequest(nil, events.CloudWatchEvent{})
//}
