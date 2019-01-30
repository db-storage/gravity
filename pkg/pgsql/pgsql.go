package pgsql

import (
	_ "bytes"
	"context"
	_ "database/sql"
	_ "database/sql/driver"
	_ "encoding/binary"
	"fmt"
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/pgtype"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/logutil"
	"github.com/moiot/gravity/pkg/utils"
	"log"
	"pg_test/gravity"
	"time"
)

//We can start a replication connection manually
//psql -p 5433 -U postgres replication=datebase
//addr: TCP host:port or Unix socket depending on Network
func NewRepConnection(cfg *utils.DBConfig) (*pgx.ReplicationConn, error) {
	//connStr := "user=postgres password=manning dbname=testdb host=8f945dd9f9ab port=5433 sslmode=disable replication=database"
	//pg.Options don't support "replication=database", we can use database/sql if we need to create a slot, and then close that db
	//ctx := context.Background()

	config := pgx.ConnConfig{
		Database: Schema,
		User:     cfg.Username,
		Password: cfg.Password,
		Host:     cfg.Host,
		Port:     cfg.Port,
	}
	config.RuntimeParams = make(map[string]string)
	config.RuntimeParams["replication"] = "database"
	var err error
	pgConn, err := pgx.ReplicationConnect(config)
	if err != nil {
		log.Fatal("Connect failed:", err)
	}
	return pgConn, err
}

/*
//functions moved to member function of Trailer
func startReplication(slot string, rep_set string, start_pos int64) error {
	//user -1 as timeline argument
	//err := pgConn.StartReplication(slot, 0, -1, "pglogical.replication_set_names", rep_set)
	pluginArgs := fmt.Sprintf(`"startup_params_format" '%s'`, "1")

	add := fmt.Sprintf(`, "proto_version" '%s'`, "1")
	pluginArgs += add

	add = fmt.Sprintf(`, "startup_params_format" '%s'`, "1")
	pluginArgs += add

	add = fmt.Sprintf(`, "min_proto_version" '%s'`, "1")
	pluginArgs += add

	add = fmt.Sprintf(`, "max_proto_version" '%s'`, "1")
	pluginArgs += add

	add = fmt.Sprintf(`, "pglogical.replication_set_names" '%s'`, rep_set)
	pluginArgs += add

	add = fmt.Sprintf(`, "binary.want_internal_basetypes" '%s'`, "1")
	pluginArgs += add

	add = fmt.Sprintf(`, "binary.want_binary_basetypes" '%s'`, "1")
	pluginArgs += add

	err := pgConn.StartReplication(slot, 0, -1, pluginArgs)
	if err != nil {
		log.Fatal("StartReplication Failed:", err)
	}
	return err
}

func sendStandbyStatus(lsn uint64) error {
	standbyStatus, err := pgx.NewStandbyStatus(lsn)
	if err != nil {
		return err
	}
	//log.Println(standbyStatus.String()) // the output of this confirms ReplyRequested is indeed 0
	standbyStatus.ReplyRequested = 0 // still set it
	err = pgConn.SendStandbyStatus(standbyStatus)
	if err != nil {
		return err
	}
	return nil
}

func GetCopyData() {
	ctx, cancelFn := context.WithTimeout(context.Background(), 500*time.Second) //, 5*time.Second)
	defer cancelFn()
	var walStart uint64

	for {
		log.Println("Waiting for message")
		var message *pgx.ReplicationMessage

		message, err := pgConn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Println("get msg failed:", err)
			continue
		}

		if message.WalMessage != nil {
			walStart = message.WalMessage.WalStart
			//var logmsg gravity.Message
			walString := string(message.WalMessage.WalData)
			log.Println("Get Msg, size:", len(message.WalMessage.WalData), ", lsn:", walStart, walString)
			_, err = gravity.Parse(message.WalMessage.WalData)
			//log.Println(logmsg)

		} else if message.ServerHeartbeat != nil {
			log.Println("Heartbeat requested")
			// set the flushed LSN (and other LSN values) in the standby status and send to PG
			//log.Println(message.ServerHeartbeat)

			// send Standby Status with the LSN position
			err = sendStandbyStatus(walStart)
			if err != nil {
				log.Fatal("Unable to send standby status:")
			}

		}
		time.Sleep(2 * time.Second)
	}
}
func main() {
	initDb("8f945dd9f9ab", 5433)
	defer pgConn.Close()

	err := startReplication("slot1", "default", 0)
	if err != nil {
		log.Fatal("Start replication failed: ", err)
	}
	GetCopyData()

	return
}
*/
