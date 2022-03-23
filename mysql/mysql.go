package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type mysqlClient struct {
	db        *sql.DB
	mysqlInfo *MysqlInfo
	//checkBreak bool //是否停止检查连通性的开关,   mysql库内部已实现断开自动重连机制，无需再实现此功能
}
type MysqlInfo struct {
	UserName     string
	Password     string
	IP           string
	Port         string
	DatabaseName string
	//MaxOpenConns int //用于设置最大打开的连接数，默认值为0表示不限制。
	MaxIdleConns int //用于设置闲置的连接数，默认值为0表示不保留空闲连接。但是在远程连接中，0会因为并发报错
}
type Stmt struct {
	Sql  string
	Args []interface{}
}

func NewMysqlClient(mysqlInfo *MysqlInfo) *mysqlClient {
	////uri: "root:zaq12wsx1@tcp(localhost:3306)/mm?charset=utf8"
	uri := mysqlInfo.UserName + ":" + mysqlInfo.Password + "@tcp(" + mysqlInfo.IP + ":" + mysqlInfo.Port + ")/" + mysqlInfo.DatabaseName + "?charset=utf8mb4&allowOldPasswords=1&parseTime=true&loc=Local" //allowOldPasswords=1是为了兼容老版本mysql，parseTime=true让驱动直接返回time.time类型
	db, _ := sql.Open("mysql", uri)
	err := db.Ping()
	if err != nil {
		panic(err)
	}
	if mysqlInfo.MaxIdleConns < 100 {
		mysqlInfo.MaxIdleConns = 100
	}
	//db.SetMaxOpenConns(mysqlInfo.MaxOpenConns) //用于设置最大打开的连接数，默认值为0表示不限制。
	db.SetMaxIdleConns(mysqlInfo.MaxIdleConns) //用于设置闲置的连接数，默认值为0表示不保留空闲连接,
	m := &mysqlClient{
		db:        db,
		mysqlInfo: mysqlInfo,
	}
	return m
}

func Close(c *mysqlClient) {
	if c.db != nil {
		_ = c.db.Close()
	}
}

func Search[T any](c *mysqlClient, stmt *Stmt) (result T, err error) {
	stmtIns, err := c.db.Prepare(stmt.Sql)
	if err != nil {
		return
	}
	defer stmtIns.Close()
	rows, err := stmtIns.Query(stmt.Args...)
	if err != nil {
		return
	}
	defer rows.Close()
	err = scanAll(rows, &result)
	if err != nil {
		return
	}
	return
}

func Execute(c *mysqlClient, stmt *Stmt) (rowsAffected int64, err error) {
	prepare, err := c.db.Prepare(stmt.Sql)
	if err != nil {
		return
	}
	defer prepare.Close()

	r, err := prepare.Exec(stmt.Args...)
	if err != nil {
		return
	}
	return r.RowsAffected()
}

//用事务批量执行sql命令
func ExecuteByTransaction(c *mysqlClient, stmts []*Stmt) (err error) {
	tx, err := c.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	for _, stmt := range stmts {
		prepare, err := tx.Prepare(stmt.Sql)
		if err != nil {
			return err
		}
		defer prepare.Close()
		_, err = prepare.Exec(stmt.Args...)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

//手动开启一个事务
func GetTransaction(c *mysqlClient) (tx *sql.Tx, err error) {
	return c.db.Begin()
}

//添加要执行的sql
func AddTransactionSql(tx *sql.Tx, stmt *Stmt) (err error) {
	prepare, err := tx.Prepare(stmt.Sql)
	if err != nil {
		return err
	}
	defer prepare.Close()
	_, err = prepare.Exec(stmt.Args...)
	if err != nil {
		return err
	}
	return err
}

//提交事务
func Commit(tx *sql.Tx) (err error) {
	return tx.Commit()
}
