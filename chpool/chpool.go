package chpool

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "nicklib/clickhouse"
)

const DefaultReadTimeout = 3600
const DefaultWriteTimeout = 3600

type CHClient struct {
	db      *sql.DB
	tx      *sql.Tx
	stmt    *sql.Stmt
	query   string
	execCnt int
	errFlag bool
}

func (c *CHClient) ExecInsert(args ...interface{}) (err error) {
	if c.tx == nil {
		if c.tx, err = c.db.Begin(); err != nil {
			return
		}
		if c.stmt, err = c.tx.Prepare(c.query); err != nil {
			return
		}
	}
	if _, err = c.stmt.Exec(args...); err == nil {
		c.execCnt++
	} else {
		c.errFlag = true
	}
	return
}

func (c *CHClient) Commit() (err error) {

	if c.tx == nil {
		return nil
	}

	err = c.tx.Commit()

	c.stmt.Close()
	c.stmt = nil
	c.tx = nil
	c.execCnt = 0
	c.errFlag = false

	return
}

func (c *CHClient) FinishInsert(cp *CHPool) {

	// check for error...
	if c.execCnt == 0 || c.errFlag {
		c.db.Close()
		cp.decConn()
		return
	}

	// check for tx is created...
	if c.tx == nil {
		cp.pool <- c
		return
	}

	if err := c.Commit(); err != nil {
		c.db.Close()
		cp.decConn()
	} else {
		cp.pool <- c
	}
}

type CHPool struct {
	pool        chan *CHClient
	addrs       []string
	clusterName string
	connStr     string
	curSize     int32
	maxSize     int32
	scanTime    time.Duration
	lock        sync.RWMutex
	connLock    sync.Mutex
}

func checkConnStr(connStr string) string {
	// check connStr for timeout...
	if len(connStr) == 0 {
		connStr = "?write_timeout=3600&read_timeout=3600"
	} else {
		s := strings.ToLower(connStr)

		if strings.Index(s, "write_timeout=") == -1 {
			connStr += "&write_timeout=3600"
		}

		if strings.Index(s, "read_timeout") == -1 {
			connStr += "&read_timeout=3600"
		}
	}
	return connStr
}

func NewCHPool(addrs []string, connStr string, size int32) (result *CHPool, err error) {

	connStr = checkConnStr(connStr)

	p := &CHPool{
		pool:    make(chan *CHClient, size),
		curSize: 0,
		maxSize: size,
		addrs:   addrs,
		connStr: connStr,
	}

	client, err := p.getNewClient()
	if err != nil {
		return nil, err
	}
	p.pool <- client
	return p, nil
}

func NewCHPoolCluster(clusterName string, addrs []string, connStr string, size int32, scanTime time.Duration) (result *CHPool, err error) {

	connStr = checkConnStr(connStr)

	p := &CHPool{
		pool:     make(chan *CHClient, size),
		curSize:  0,
		maxSize:  size,
		addrs:    append(make([]string, 0, 1), addrs...),
		connStr:  connStr,
		scanTime: scanTime,
	}

	p.addrs, err = p.getClusterNodes()
	if err != nil {
		return nil, err
	}

	client, err := p.getNewClient()
	if err != nil {
		return nil, err
	}
	p.pool <- client

	go p.rescanCluster()

	return p, nil
}

func (p *CHPool) getClusterNodes() (addrs []string, err error) {
	db, err := p.getNewConn()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query("SELECT `host_address`, `port` FROM system.clusters")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	host_address := ""
	port := uint16(0)
	for rows.Next() {

		if err = rows.Scan(&host_address, &port); err != nil {
			return nil, err
		}

		addrs = append(addrs, host_address+":"+strconv.FormatUint(uint64(port), 10))

	}
	return
}

func (p *CHPool) rescanCluster() {

	for {
		time.Sleep(p.scanTime)

		if addrs, err := p.getClusterNodes(); err == nil {
			p.lock.Lock()
			p.addrs = addrs
			p.lock.Unlock()
		}

	}
}

func (p *CHPool) Close() {

}

func (p *CHPool) getNewClient() (client *CHClient, err error) {

	db, err := p.getNewConn()
	if err != nil {
		return nil, err
	}

	return &CHClient{db: db}, nil
}

func (p *CHPool) getNewConn() (db *sql.DB, err error) {

	p.lock.RLock()
	defer p.lock.RUnlock()

	for i := 0; i < len(p.addrs); i++ {
		db, err = sql.Open("clickhouse", "tcp://"+p.addrs[i]+p.connStr)
		if err != nil {
			continue
		}
		if err = db.Ping(); err != nil {
			continue
		} else {
			return
		}
	}
	if err != nil {
		db = nil
	}
	return
}

func (p *CHPool) addConn() bool {
	p.connLock.Lock()
	defer p.connLock.Unlock()

	if p.curSize < p.maxSize {
		p.curSize++
		return true
	} else {
		return false
	}
}

func (p *CHPool) decConn() {
	p.connLock.Lock()
	defer p.connLock.Unlock()

	if p.curSize > 0 {
		p.curSize--
	}
}

func (p *CHPool) getClient() (client *CHClient, err error) {
	for {
		select {
		case client = <-p.pool:
			return

		default:
			if p.addConn() {
				if client, err = p.getNewClient(); err != nil {
					p.decConn()
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (p *CHPool) Get() (*sql.DB, error) {

	if cl, err := p.getClient(); err != nil {
		return nil, err
	} else {
		return cl.db, nil
	}

}

func (p *CHPool) Put(db *sql.DB) {
	select {
	case p.pool <- &CHClient{db: db}:
		return
	default:
		db.Close()
	}
}

func (p *CHPool) PrepareInsert(query string) (client *CHClient, err error) {

	// try to get client...
	for i := 1; i < 10; i++ {
		if client, err = p.getClient(); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if client == nil {
		return
	}

	client.query = query
	client.tx = nil
	client.execCnt = 0
	return
}

func (p *CHPool) QueryRow(query string, args ...interface{}) (row *sql.Row) {

	db, err := p.Get()
	if err != nil {
		return nil
	}
	defer p.Put(db)

	return db.QueryRow(query, args...)
}

func (p *CHPool) Exec(query string, args ...interface{}) error {

	db, err := p.Get()
	if err != nil {
		return err
	}
	defer p.Put(db)

	_, err = db.Exec(query, args...)
	return err
}

func (p *CHPool) ClusterExec(cluster string, query string, args ...interface{}) (shard_cnt int, ok_cnt int, err error) {

	db, err := p.Get()
	if err != nil {
		return 0, 0, err
	}
	defer p.Put(db)

	// get shard count...
	sqlstr := "SELECT count(distinct shard_num) " +
		" FROM system.clusters " +
		" WHERE cluster = '" + cluster + "' "

	if err = db.QueryRow(sqlstr).Scan(&shard_cnt); err != nil {
		return
	}

	// get cluster info...
	sqlstr = "SELECT shard_num, replica_num, host_address, port " +
		" FROM system.clusters " +
		" WHERE cluster = '" + cluster + "' " +
		" ORDER BY shard_num"

	rows, err := db.Query(sqlstr)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var (
		shard_num      int
		replica_num    int
		host_address   string
		host_port      string
		last_shard_num int
	)
	errStr := ""
	for rows.Next() {
		if err = rows.Scan(&shard_num, &replica_num, &host_address, &host_port); err != nil {
			return
		}

		// get new conn
		shardDb, err := sql.Open("clickhouse", "tcp://"+host_address+":"+host_port+p.connStr)
		if err != nil {
			errStr += "Error on shard: " + strconv.Itoa(shard_num) + ", replica: " + strconv.Itoa(replica_num) + ". " + err.Error() + "\n"
		}
		if err := db.Ping(); err != nil {
			shardDb.Close()
			errStr += "Error on shard: " + strconv.Itoa(shard_num) + ", replica: " + strconv.Itoa(replica_num) + ". " + err.Error() + "\n"
		}

		if _, err := shardDb.Exec(query, args...); err != nil {
			errStr += "Error on shard: " + strconv.Itoa(shard_num) + ", replica: " + strconv.Itoa(replica_num) + ". " + err.Error() + "\n"
		} else {
			if last_shard_num != shard_num {
				last_shard_num = shard_num
				ok_cnt++
			}
		}
		shardDb.Close()
	}
	if errStr != "" {
		err = errors.New(errStr)
	}

	return
}
