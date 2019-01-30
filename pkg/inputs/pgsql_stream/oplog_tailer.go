package pgsql_stream

import (
	"context"
	"fmt"
	"time"

	"github.com/moiot/gravity/pkg/utils"

	"github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"github.com/juju/errors"

	"github.com/jackc/pgx"
	"github.com/moiot/gravity/pkg/config"
	"github.com/moiot/gravity/pkg/position_store"
)

type pglogicalTailer struct {
	pipelineName  string
	slot          string
	repSet        string
	emitter       core.Emitter
	ctx           context.Context
	cancel        context.CancelFunc
	idx           int
	session       *pgx.ReplicationConn
	sourceHost    string
	positionStore position_store.PgsqlPositionStore
	stopped       bool
}

type filterOpt struct {
	allowInsert  bool
	allowUpdate  bool
	allowDelete  bool
	allowCommand bool
}

func GetRowDataFromOp(op *gtm.Op) *map[string]interface{} {
	var row *map[string]interface{}
	if op.IsInsert() {
		row = &op.Data
	} else if op.IsUpdate() {
		row = &op.Row
	}

	return row
}

func (tailer *pglogicalTailer) Filter(op *gtm.Op, option *filterOpt) bool {
	// handle control msg
	if dbName == internalDB {
		return true
	}

	log.Debugf("[oplog_tailer] Filter dbName: %v, tableName: %v", dbName, tableName)

	return true
}

func (tailer *pglogicalTailer) startReplication(slot string, repSet string, startLsn int64) error {
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

	add = fmt.Sprintf(`, "pglogical.replication_set_names" '%s'`, repSet)
	pluginArgs += add

	add = fmt.Sprintf(`, "binary.want_internal_basetypes" '%s'`, "1")
	pluginArgs += add

	add = fmt.Sprintf(`, "binary.want_binary_basetypes" '%s'`, "1")
	pluginArgs += add

	err := session.StartReplication(slot, startLsn, -1, pluginArgs)
	if err != nil {
		log.Fatal("StartReplication Failed:", err)
	}
	return err
}

func (tailer *pglogicalTailer) sendStandbyStatus(lsn uint64) error {
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

func (tailer *pglogicalTailer) getNextData() (pgx.ReplicationMessage, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 500*time.Second) //, 5*time.Second)
	defer cancelFn()
	for {
		log.Info("Waiting for message")
		var message *pgx.ReplicationMessage

		message, err := pgConn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Info("get msg failed:", err)
			return nil, err
		}

		if message.WalMessage != nil {
			walStart = message.WalMessage.WalStart
			//var logmsg gravity.Message
			walString := string(message.WalMessage.WalData)
			log.Info("Get Msg, size:", len(message.WalMessage.WalData), ", lsn:", walStart, walString)
			return message.WalMessage, nil
		} else if message.ServerHeartbeat != nil {
			log.Println("Heartbeat requested")
			// send Standby Status with the LSN position
			err = sendStandbyStatus(startLsn)
			if err != nil {
				log.Error("Unable to send standby status:", err)
			}

		}
		time.Sleep(1 * time.Second)
	}
}

func (tailer *pglogicalTailer) Run() {
	log.Infof("running tailer worker idx: %v", tailer.idx)

	startLsn := tailer.positionStore.Get()
	if 0 == startLsn {
		log.Infof("[oplog_tailer] start from the latest timestamp")
		after = nil
	} else {
		log.Infof("[oplog_tailer] start from the configured timestamp")
	}

	err := tailer.session.StartReplication(slot, repSet, startLsn)
	if err != nil {
		log.Infof("running tailer worker idx: %v", tailer.idx)
		return
	}
	//TODO: handle filters?

	for {
		msg, err := getCopyData()
		if err != nil {
			log.Error("get data from upstream failed", err)
			return
		}
		err = HandleMsg(msg)
		if err != nil {
			log.Error("HandleMsg failed", err)
			return
		}

	}
}

func (tailer *pglogicalTailer) AfterMsgCommit(msg *core.Msg) error {
	position, ok := msg.InputContext.(config.PgsqlPosition)
	if !ok {
		return errors.Errorf("invalid InputContext")
	}

	tailer.positionStore.Put(position)
	return nil
}

const internalDB = "drc"
const deadSignalCollection = "dead_signals"

func (tailer *pglogicalTailer) Wait() {
	<-tailer.ctx.Done()
}

func (tailer *pglogicalTailer) Stop() {
	if tailer.stopped {
		return
	}
	log.Infof("[oplog_tailer]: stop idx: %v", tailer.idx)
	tailer.stopped = true
	tailer.cancel()
}

type pglogicalTailerOpt struct {
	pipelineName     string
	uniqueSourceName string
	// mqMsgType        protocol.JobMsgType
	session       *pgx.ReplicationConn
	sourceHost    string
	positionStore position_store.MongoPositionStore
	emitter       core.Emitter
	logger        log.Logger
	idx           int
	ctx           context.Context
}

func NewpglogicalTailer(opts *pglogicalTailerOpt) *pglogicalTailer {
	if opts.pipelineName == "" {
		log.Fatalf("[oplog_tailer] pipeline name is empty")
	}

	tailer := pglogicalTailer{
		pipelineName:     opts.pipelineName,
		uniqueSourceName: opts.uniqueSourceName,
		session:          opts.session,
		oplogChecker:     opts.oplogChecker,
		gtmConfig:        opts.gtmConfig,
		emitter:          opts.emitter,
		idx:              opts.idx,
		sourceHost:       opts.sourceHost,
		timestampStore:   opts.timestampStore,
	}
	tailer.ctx, tailer.cancel = context.WithCancel(opts.ctx)
	log.Infof("[oplog_tailer] tailer created")
	return &tailer
}
