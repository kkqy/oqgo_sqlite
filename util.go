package oqgo_sqlite

import (
	"context"
	"database/sql"

	"github.com/mattn/go-sqlite3"
)

func checkError(e error) {
	if e != nil {
		panic(e)
	}
}
func backup(source *sql.DB, target *sql.DB) {
	sourceConnect, err := source.Conn(context.Background())
	checkError(err)
	defer sourceConnect.Close()
	targetConnect, err := target.Conn(context.Background())
	checkError(err)
	defer targetConnect.Close()
	err = targetConnect.Raw(func(driverConn any) error {
		targetConnect := driverConn.(*sqlite3.SQLiteConn)
		sourceConnect.Raw(func(driverConn any) error {
			sourceConnect := driverConn.(*sqlite3.SQLiteConn)
			sqliteBackup, err := targetConnect.Backup("main", sourceConnect, "main")
			checkError(err)
			_, err = sqliteBackup.Step(-1)
			checkError(err)
			err = sqliteBackup.Finish()
			checkError(err)
			return nil
		})
		return nil
	})
	checkError(err)
}
