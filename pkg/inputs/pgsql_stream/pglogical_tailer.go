package pgsql_stream

import (
	"context"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/utils"

	_ "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"

	"github.com/moiot/gravity/pkg/core"

	"github.com/jackc/pgx"
	_ "github.com/moiot/gravity/pkg/config"
	_ "github.com/moiot/gravity/pkg/pgsql"
	"github.com/moiot/gravity/pkg/position_store"
	"github.com/moiot/gravity/pkg/schema_store"
)

type PglogicalTailer struct {
	pipelineName   string
	slot           string
	replicationSet string
	startPosition  uint64
	emitter        core.Emitter
	ctx            context.Context
	cancel         context.CancelFunc
	idx            int
	session        *pgx.ReplicationConn
	sourceHost     string
	positionStore  position_store.PgsqlPositionStore
	stopped        bool
	//After a Begin, before a Commit
	inRemoteTxn       bool
	sourceSchemaStore schema_store.SchemaStore
	relations         Relations
}

type filterOpt struct {
	allowInsert  bool
	allowUpdate  bool
	allowDelete  bool
	allowCommand bool
}

/*
func (tailer *PglogicalTailer) Filter(op *gtm.Op, option *filterOpt) bool {
	// handle control msg
	if dbName == internalDB {
		return true
	}

	log.Debugf("[oplog_tailer] Filter dbName: %v, tableName: %v", dbName, tableName)

	return true
}
*/

func (tailer *PglogicalTailer) startReplication(slot string, repSet string, startLsn uint64) error {
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

	err := tailer.session.StartReplication(slot, startLsn, -1, pluginArgs)
	if err != nil {
		log.Fatalf("StartReplication Failed, slot:%v, startLsn:%v, err:%v", slot, startLsn, err)
	}
	log.Infof(" [pgsql] StartReplication OK, slot:%v, repSet:%v, startLsn:%v", slot, repSet, startLsn)
	return err
}

func (tailer *PglogicalTailer) sendStandbyStatus(lsn uint64) error {
	standbyStatus, err := pgx.NewStandbyStatus(lsn)
	if err != nil {
		return err
	}
	//log.Println(standbyStatus.String()) // the output of this confirms ReplyRequested is indeed 0
	standbyStatus.ReplyRequested = 0 // still set it
	err = tailer.session.SendStandbyStatus(standbyStatus)
	if err != nil {
		return err
	}
	return nil
}

//For ServerHeartbeat, ack immediately
func (tailer *PglogicalTailer) getNextMsg() (*pgx.ReplicationMessage, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 500*time.Second) //, 5*time.Second)
	defer cancelFn()
	for {
		//log.Info("Waiting for message")
		var message *pgx.ReplicationMessage

		message, err := tailer.session.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Info("get msg failed:", err)
			return nil, err
		}

		if message.WalMessage != nil {
			//walStart := message.WalMessage.WalStart
			//var logmsg gravity.Message
			//walString := string(message.WalMessage.WalData)
			//log.Info("Get Msg, size:", len(message.WalMessage.WalData), ", lsn:", walStart, walString)
			return message, nil
		} else if message.ServerHeartbeat != nil {
			//log.Println("Heartbeat requested")
			// send Standby Status with the LSN position
			err = tailer.sendStandbyStatus(tailer.positionStore.Get().WalPosition)
			if err != nil {
				log.Error("Unable to send standby status:", err)
			}

		}
		time.Sleep(1 * time.Second)
	}
}

//refer to pglogical's replication_handler()
func (tailer *PglogicalTailer) handleMsg(repMsg *pgx.ReplicationMessage) error {
	msgType := repMsg.WalMessage.WalData[0]
	switch msgType {
	/* BEGIN */
	case 'B':
		tailer.handleBegin(repMsg)
	/* COMMIT */
	case 'C':
		tailer.handleCommit(repMsg)
	/* ORIGIN */
	case 'O':
		tailer.handleOrigin(repMsg)
	/* RELATION */
	case 'R':
		tailer.handleRelation(repMsg)
	/* INSERT */
	case 'I':
		tailer.handleInsert(repMsg)
	/* UPDATE */
	case 'U':
		tailer.handleUpdate(repMsg)
	/* DELETE */
	case 'D':
		tailer.handleDelete(repMsg)
		break
	/* STARTUP MESSAGE */
	case 'S':
		tailer.handleStartup(repMsg)
		break
	default:
		log.Errorf("unknown action of type %c", msgType)
		return fmt.Errorf("unknown action of type %c", msgType)
	}
	return nil
}

func (tailer *PglogicalTailer) Run() {
	log.Infof("running tailer worker idx: %v", tailer.idx)

	startLsn := tailer.startPosition
	if 0 == startLsn {
		log.Infof("[oplog_tailer] start from the latest timestamp")
	} else {
		log.Infof("[oplog_tailer] start from the configured timestamp")
	}
	//TODO: which startLsn to use? log? position_store?
	err := tailer.startReplication(tailer.slot, tailer.replicationSet, startLsn)
	if err != nil {
		log.Infof("running tailer worker idx: %v", tailer.idx)
		return
	}
	//TODO: handle filters?

	for {
		repMsg, err := tailer.getNextMsg()
		if err != nil {
			log.Error("get data from upstream failed", err)
			return
		}
		err = tailer.handleMsg(repMsg)
		if err != nil {
			log.Error("handleMsg failed", err)
			return
		}

	}
}

/*
func (tailer *PglogicalTailer) AfterMsgCommit(msg *core.Msg) error {
	position, ok := msg.InputContext.(config.PgsqlPosition)
	if !ok {
		return errors.Errorf("invalid InputContext")
	}

	tailer.positionStore.Put(position)
	return nil
}
*/

const internalDB = "drc"
const deadSignalCollection = "dead_signals"

func (tailer *PglogicalTailer) Wait() {
	<-tailer.ctx.Done()
}

func (tailer *PglogicalTailer) Stop() {
	if tailer.stopped {
		return
	}
	log.Infof("[oplog_tailer]: stop idx: %v", tailer.idx)
	tailer.stopped = true
	tailer.cancel()
}

type binlogOp string

const (
	insert           binlogOp = "insert"
	update           binlogOp = "update"
	updatePrimaryKey binlogOp = "updatePrimaryKey"
	del              binlogOp = "delete"
	ddl              binlogOp = "ddl"
	//xid              binlogOp = "xid"
	//barrier          binlogOp = "barrier"
)

type inputContext struct {
	op       binlogOp
	position utils.MySQLBinlogPosition
}

func (tailer *PglogicalTailer) handleStartup(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Startup msg")
	//The first byte is type, skipped here
	walData := StringInfoData{repMsg.WalMessage.WalData, 1}
	//msgver, unused now
	if _, err := walData.GetUInt8(); err != nil {
		log.Errorf("get msgver faild: %v", err)
		return err
	}
	for {
		if !walData.HasMoreData() {
			break
		}
		key, err := walData.GetString()
		if err != nil {
			log.Errorf("get key faild: %v", err)
			return err
		}

		val, err := walData.GetString()
		if err != nil {
			log.Errorf("get val for key:%v faild: %v, ", key, err)
			return err
		}
		log.Infof("Get param, key: %v, val: %v", key, val)
		//TODO handle params later
		//handleParam(key, val)
	}
	tailer.relations = make(map[string]*Relation)
	return nil
}

//TODO: should create a batch for all messages in a txn?
func (tailer *PglogicalTailer) handleBegin(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Begin msg")
	walData := StringInfoData{repMsg.WalMessage.WalData, 1}
	flags, err := walData.GetUInt8()
	if err != nil {
		log.Errorf("[pgsql] get flags faild: %v", err)
		return err
	}
	if flags != 0 {
		return errors.Errorf("[pgsql] Unkown flags: %v", flags)
	}

	remoteLsn, err := walData.GetInt64()
	if err != nil {
		log.Errorf("[pgsql] get remote lsn faild: %v", err)
		return err
	}

	commitTime, err := walData.GetInt64()
	if err != nil {
		log.Errorf("[pgsql] get commit time faild: %v", err)
		return err
	}

	remoteXid, err := walData.GetInt32()
	if err != nil {
		log.Errorf("[pgsql] get remote xid faild: %v", err)
		return err
	}
	log.Infof("remoteLsn: %v, commitTime: %v, remoteXid: %v", remoteLsn, commitTime, remoteXid)
	tailer.inRemoteTxn = true
	return nil

}

func (tailer *PglogicalTailer) handleCommit(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Commit msg")
	/*
		if !inRemoteTxn {
			log.Fatalf("Get commit with no begin")
			return errors.New("Get commit with no begin")
		}
		d := &pgsql.decoder{order: binary.BigEndian, buf: bytes.NewBuffer(repMsg[1:])}
		flags := d.uint8()
		if flags != 0 {
			log.Fatalf("Unkown flags:%c", flags)
			return errors.New("Unkown flags:%c", flags)
		}
		commitLsn := d.int64()
		endLsn := d.int64()
		commitTime := d.int64()
		log.Info("Get txn Commit, commitLsn:%v, endLsn:%v", commitLsn, endLsn)
		inRemoteTxn = false
	*/
	tailer.inRemoteTxn = false
	return nil

}
func (tailer *PglogicalTailer) handleOrigin(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Origin msg")
	/*
		if !inRemoteTxn {
			log.Fatalf("Get commit with no begin")
			return errors.New("Get commit with no begin")
		}
		d := &pgsql.decoder{order: binary.BigEndian, buf: bytes.NewBuffer(repMsg[1:])}
	*/
	return nil
}

func (tailer *PglogicalTailer) handleRelation(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Relation msg")
	rel, err := RelationFromMsg(repMsg)
	if err != nil {
		return err
	}
	tailer.relations.UpdateRelation(rel)
	return nil
}

//refer to NewInsertMsgs of mysql binlog trailer
func (tailer *PglogicalTailer) handleInsert(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Insert msg")
	/*
		msgs := make([]*core.Msg, len(ev.Rows))
		columns := tableDef.Columns
		pkColumns := tableDef.PrimaryKeyColumns

		pkColumnNames := make([]string, len(pkColumns))
		for i, c := range pkColumns {
			pkColumnNames[i] = c.Name
		}

		for rowIndex, dataRow := range ev.Rows {

			if len(dataRow) != len(columns) {
				log.Warnf("insert %s.%s columns and data mismatch in length: %d vs %d, table %v",
					ev.Table.Schema, ev.Table.Table, len(columns), len(dataRow), tableDef)
			}
			msg := core.Msg{
				Type:         core.MsgDML,
				Host:         host,
				Database:     database,
				Table:        table,
				Timestamp:    time.Unix(ts, 0),
				InputContext: inputContext{op: insert},
				Metrics: core.Metrics{
					MsgCreateTime: time.Now(),
				},
			}

			dmlMsg := &core.DMLMsg{}
			dmlMsg.Operation = core.Insert

			data := make(map[string]interface{})
			for i := 0; i < len(dataRow); i++ {
				data[columns[i].Name] = deserialize(dataRow[i], columns[i])
			}
			dmlMsg.Data = data
			pks, err := mysql.GenPrimaryKeys(pkColumns, data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dmlMsg.Pks = pks
			msg.DmlMsg = dmlMsg
			msg.Done = make(chan struct{})
			msg.InputStreamKey = utils.NewStringPtr(inputStreamKey)
			msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
			msgs[rowIndex] = &msg
		}
		err = tailer.emitter.Emit(msg)
		if err != nil {
			log.Fatalf("failed to emit, idx: %d, schema: %v, table: %v, msgType: %v, err: %v",
				i, m.Database, m.Table, m.Type, errors.ErrorStack(err))
		}
	*/
	return nil
}

func (tailer *PglogicalTailer) handleDelete(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Delete msg")
	return nil
}
func (tailer *PglogicalTailer) handleUpdate(repMsg *pgx.ReplicationMessage) error {
	log.Info("[pgsql] get Update msg")
	/*
		//	tableDef (*schema_store.Table) ([]*core.Msg, error) {

		var msgs []*core.Msg
		columns := tableDef.Columns
		pkColumns := tableDef.PrimaryKeyColumns
		pkColumnNames := make([]string, len(pkColumns))
		for i, c := range pkColumns {
			pkColumnNames[i] = c.Name
		}
		for rowIndex := 0; rowIndex < len(ev.Rows); rowIndex += 2 {
			oldDataRow := ev.Rows[rowIndex]
			newDataRow := ev.Rows[rowIndex+1]

			if len(oldDataRow) != len(newDataRow) {
				return nil, errors.Errorf("update %s.%s data mismatch in length: %d vs %d",
					tableDef.Schema, tableDef.Name, len(oldDataRow), len(newDataRow))
			}

			if len(oldDataRow) != len(columns) {
				log.Warnf("update %s.%s columns and data mismatch in column length: %d vs, old data length: %d",
					tableDef.Schema, tableDef.Name, len(columns), len(oldDataRow))
			}

			data := make(map[string]interface{})
			old := make(map[string]interface{})
			pkUpdate := false
			for i := 0; i < len(oldDataRow); i++ {
				data[columns[i].Name] = deserialize(newDataRow[i], columns[i])
				old[columns[i].Name] = deserialize(oldDataRow[i], columns[i])

				if columns[i].IsPrimaryKey && data[columns[i].Name] != old[columns[i].Name] {
					pkUpdate = true
				}
			}

			if !pkUpdate {
				msg := core.Msg{
					Type:         core.MsgDML,
					Host:         host,
					Database:     database,
					Table:        table,
					Timestamp:    time.Unix(ts, 0),
					InputContext: inputContext{op: update},
					Metrics: core.Metrics{
						MsgCreateTime: time.Now(),
					},
				}

				dmlMsg := &core.DMLMsg{}
				dmlMsg.Operation = core.Update
				pks, err := mysql.GenPrimaryKeys(pkColumns, data)
				if err != nil {
					return nil, errors.Trace(err)
				}
				dmlMsg.Pks = pks

				dmlMsg.Data = data
				dmlMsg.Old = old

				msg.DmlMsg = dmlMsg
				msg.Done = make(chan struct{})
				msg.InputStreamKey = utils.NewStringPtr(inputStreamKey)
				msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
				msgs = append(msgs, &msg)
			} else {
				// first delete old row
				msgDelete := core.Msg{
					Type:         core.MsgDML,
					Host:         host,
					Database:     database,
					Table:        table,
					Timestamp:    time.Unix(ts, 0),
					InputContext: inputContext{op: updatePrimaryKey},
					Metrics: core.Metrics{
						MsgCreateTime: time.Now(),
					},
				}
				dmlMsg1 := &core.DMLMsg{}
				dmlMsg1.Operation = core.Delete

				pks, err := mysql.GenPrimaryKeys(pkColumns, old)
				if err != nil {
					return nil, errors.Trace(err)
				}
				dmlMsg1.Pks = pks
				dmlMsg1.Data = old
				msgDelete.DmlMsg = dmlMsg1
				msgDelete.Done = make(chan struct{})
				msgDelete.InputStreamKey = utils.NewStringPtr(inputStreamKey)
				msgDelete.OutputStreamKey = utils.NewStringPtr(msgDelete.GetPkSign())
				msgs = append(msgs, &msgDelete)

				// then insert new row
				msgInsert := core.Msg{
					Type:         core.MsgDML,
					Host:         host,
					Database:     database,
					Table:        table,
					Timestamp:    time.Unix(ts, 0),
					InputContext: inputContext{op: updatePrimaryKey},
					Metrics: core.Metrics{
						MsgCreateTime: time.Now(),
					},
				}
				dmlMsg2 := &core.DMLMsg{}
				dmlMsg2.Operation = core.Insert

				pks, err = mysql.GenPrimaryKeys(pkColumns, data)
				if err != nil {
					return nil, errors.Trace(err)
				}
				dmlMsg2.Pks = pks

				dmlMsg2.Data = data
				msgInsert.DmlMsg = dmlMsg2
				msgInsert.Done = make(chan struct{})
				msgInsert.InputStreamKey = utils.NewStringPtr(inputStreamKey)
				msgInsert.OutputStreamKey = utils.NewStringPtr(msgInsert.GetPkSign())
				msgs = append(msgs, &msgInsert)
			}
		}
		return msgs, nil
	*/
	return nil
}

/*
func deserialize(raw interface{}, column schema_store.Column) interface{} {
	// fix issue: https://github.com/siddontang/go-mysql/issues/242
	if raw == nil {
		return nil
	}

	ct := strings.ToLower(column.ColType)
	if ct == "text" || ct == "json" {
		return string(raw.([]uint8))
	}

	// https://github.com/siddontang/go-mysql/issues/338
	// binlog itself doesn't specify whether it's signed or not
	if column.IsUnsigned {
		switch t := raw.(type) {
		case int8:
			return uint8(t)
		case int16:
			return uint16(t)
		case int32:
			return uint32(t)
		case int64:
			return uint64(t)
		case int:
			return uint(t)
		default:
			// nothing to do
		}
	}

	return raw
}

func (tailer *pglogicalTailer) NewDeleteMsgs(
	ev *replication.RowsEvent,
	tableDef *schema_store.Table) ([]*core.Msg, error) {

	msgs := make([]*core.Msg, len(ev.Rows))
	columns := tableDef.Columns
	pkColumns := tableDef.PrimaryKeyColumns
	pkColumnNames := make([]string, len(pkColumns))
	for i, c := range pkColumns {
		pkColumnNames[i] = c.Name
	}

	for rowIndex, row := range ev.Rows {
		if len(row) != len(columns) {
			return nil, errors.Errorf("delete %s.%s columns and data mismatch in length: %d vs %d",
				tableDef.Schema, tableDef.Name, len(columns), len(row))
		}
		msg := core.Msg{
			Type:         core.MsgDML,
			Host:         host,
			Database:     database,
			Table:        table,
			Timestamp:    time.Unix(ts, 0),
			InputContext: inputContext{op: del},
			Metrics: core.Metrics{
				MsgCreateTime: time.Now(),
			},
		}

		dmlMsg := &core.DMLMsg{}
		dmlMsg.Operation = core.Delete

		data := make(map[string]interface{})
		for i := 0; i < len(columns); i++ {
			data[columns[i].Name] = deserialize(row[i], columns[i])
		}
		dmlMsg.Data = data
		pks, err := mysql.GenPrimaryKeys(pkColumns, data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dmlMsg.Pks = pks
		msg.DmlMsg = dmlMsg
		msg.Done = make(chan struct{})
		msg.InputStreamKey = utils.NewStringPtr(inputStreamKey)
		msg.OutputStreamKey = utils.NewStringPtr(msg.GetPkSign())
		msgs[rowIndex] = &msg
	}

	return msgs, nil

}

func NewDDLMsg(
	callback core.AfterMsgCommitFunc,
	dbName string,
	table string,
	ast ast.StmtNode,
	ddlSQL string,
	ts int64,
	position utils.MySQLBinlogPosition) *core.Msg {

	return &core.Msg{
		Type:                core.MsgDDL,
		Timestamp:           time.Unix(ts, 0),
		Database:            dbName,
		Table:               table,
		DdlMsg:              &core.DDLMsg{Statement: ddlSQL, AST: ast},
		Done:                make(chan struct{}),
		InputContext:        inputContext{op: ddl, position: position},
		InputStreamKey:      utils.NewStringPtr(inputStreamKey),
		OutputStreamKey:     utils.NewStringPtr(""),
		AfterCommitCallback: callback,
		Metrics: core.Metrics{
			MsgCreateTime: time.Now(),
		},
	}
}
*/

type PglogicalTailerOpt struct {
	pipelineName   string
	slot           string
	replicationSet string
	startPosition  uint64
	//uniqueSourceName string
	// mqMsgType        protocol.JobMsgType
	session       *pgx.ReplicationConn
	sourceHost    string
	positionStore position_store.PgsqlPositionStore
	emitter       core.Emitter
	logger        log.Logger
	idx           int
	ctx           context.Context
}

func NewpglogicalTailer(opts *PglogicalTailerOpt) *PglogicalTailer {
	if opts.pipelineName == "" {
		log.Fatalf("[oplog_tailer] pipeline name is empty")
		return nil
	}

	tailer := PglogicalTailer{
		pipelineName: opts.pipelineName,
		//uniqueSourceName: opts.uniqueSourceName,
		session: opts.session,
		//oplogChecker:   opts.oplogChecker,
		//gtmConfig:      opts.gtmConfig,
		emitter:        opts.emitter,
		idx:            opts.idx,
		sourceHost:     opts.sourceHost,
		positionStore:  opts.positionStore,
		slot:           opts.slot,
		replicationSet: opts.replicationSet,
		startPosition:  opts.startPosition,
		//timestampStore: opts.timestampStore,
	}
	tailer.ctx, tailer.cancel = context.WithCancel(opts.ctx)
	log.Infof("[oplog_tailer] tailer created")
	return &tailer
}
