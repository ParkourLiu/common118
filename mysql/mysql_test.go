package mysql_test

import (
	"common118/mysql"
	"fmt"
	"testing"
)

var (
	mysqlClient = mysql.NewMysqlClient(&mysql.MysqlInfo{
		UserName:     "root",
		Password:     "root",
		IP:           "127.0.0.1",
		Port:         "3306",
		DatabaseName: "test",
	})
)

type User struct {
	Uid      string
	Name     string
	Nickname string
	Age      int
	Gold     float64
	Ct       string
	Ut       string
	dt       string
}

func TestSearch(t *testing.T) {
	row, err := mysql.Search[User](mysqlClient, &mysql.Stmt{Sql: "select * from user where uID=?;", Args: []interface{}{2}})
	fmt.Printf("1:%+v,%+v\n", row, err)

	rows, err := mysql.Search[[]User](mysqlClient, &mysql.Stmt{Sql: "select * from user;"})
	fmt.Printf("2:%+v,%+v\n", rows, err)

	intResult, err := mysql.Search[int](mysqlClient, &mysql.Stmt{Sql: "SELECT COUNT(*) FROM `user`;"})
	fmt.Printf("3:%+v,%+v\n", intResult, err)

	intsResult, err := mysql.Search[[]int](mysqlClient, &mysql.Stmt{Sql: "SELECT COUNT(*) FROM `user` UNION ALL SELECT COUNT(*) FROM `user`;"})
	fmt.Printf("4:%+v,%+v,%d\n", intsResult, err, intsResult[0])

	mapResult, err := mysql.Search[map[string]string](mysqlClient, &mysql.Stmt{Sql: "select * from user where uID=1;"})
	fmt.Printf("5:%+v,%+v\n", mapResult, err)
	
	mapsResult, err := mysql.Search[[]map[string]interface{}](mysqlClient, &mysql.Stmt{Sql: "select * from user;"})
	fmt.Printf("6:%+v,%+v\n", mapsResult, err)
}

func TestExecute(t *testing.T) {
	execute, err := mysql.Execute(mysqlClient, &mysql.Stmt{Sql: "INSERT INTO `test`.`user` (`uID`)VALUES  (56);"})
	fmt.Printf("1:%d,%+v\n", execute, err)

	execute, err = mysql.Execute(mysqlClient, &mysql.Stmt{Sql: "INSERT INTO `test`.`user` (`uID`)VALUES  ('3') ON DUPLICATE KEY UPDATE nickname=NOW();"})
	fmt.Printf("2:%d,%+v\n", execute, err)

	execute, err = mysql.Execute(mysqlClient, &mysql.Stmt{Sql: "UPDATE `user` SET age=2 where;"})
	fmt.Printf("3:%d,%+v\n", execute, err)
}

func TestExecuteByTransaction(t *testing.T) {
	err := mysql.ExecuteByTransaction(mysqlClient, []*mysql.Stmt{
		{Sql: "UPDATE `user` SET age=8 where uid=1;"},
		{Sql: "UPDATE `user` SET age=9 where uid=2;"},
		{Sql: "UPDATE `user` SET age=10 where uid=3;"},
	})
	fmt.Printf("1:%+v\n", err)
}

func TestGetTransaction(t *testing.T) {
	tx, err := mysql.GetTransaction(mysqlClient)
	if err != nil {
		t.Error(err)
		return
	}
	err = mysql.AddTransactionSql(tx, &mysql.Stmt{Sql: "UPDATE `user` SET age=4 where uid=1;"})
	if err != nil {
		t.Error(err)
		return
	}
	err = mysql.AddTransactionSql(tx, &mysql.Stmt{Sql: "UPDATE `user` SET age=5 where;"})
	if err != nil {
		t.Error(err)
		return
	}
	err = mysql.AddTransactionSql(tx, &mysql.Stmt{Sql: "UPDATE `user` SET age=6 where uid=3;"})
	if err != nil {
		t.Error(err)
		return
	}
	err = mysql.Commit(tx)
	if err != nil {
		t.Error(err)
		return
	}
}
