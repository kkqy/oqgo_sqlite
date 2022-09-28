package oqgo_sqlite

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/oqgo/oqgo"
	model "github.com/oqgo/oqgo_model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type db = *gorm.DB
type Database struct {
	db
	options map[string]string
}

func (p *Database) restore() {
	dumpFileName := p.options["file"]
	_, err := os.Stat(dumpFileName)
	if err == nil {
		log.Println("加载数据库开始")
		dump, err := sql.Open("sqlite3", dumpFileName)
		checkError(err)
		db, err := p.DB()
		checkError(err)
		backup(dump, db)
		dump.Close()
		log.Println("加载数据库完成")
	}
}
func (p *Database) dump() {
	log.Println("保存数据库开始")
	dumpFileName := p.options["file"]
	dump, err := sql.Open("sqlite3", dumpFileName)
	checkError(err)
	_, err = dump.Exec(`PRAGMA journal_mode = OFF;`)
	checkError(err)
	db, err := p.DB()
	checkError(err)
	backup(db, dump)
	dump.Close()
	log.Println("保存数据库完成")
}
func (p *Database) SaveKline(kline oqgo.Kline) error {
	splitedSymbol := strings.Split(kline.Symbol, ".")
	exchangeID := splitedSymbol[0]
	instrumentID := splitedSymbol[1]
	klineTableName := fmt.Sprintf("kline_%v_%v", exchangeID, instrumentID)
	p.Table(klineTableName).AutoMigrate(&model.Kline{})
	p.Table(klineTableName).Create(&model.Kline{Kline: kline})
	return nil
}
func (p *Database) SaveTick(tick oqgo.Tick) error {
	splitedSymbol := strings.Split(tick.Symbol, ".")
	exchangeID := splitedSymbol[0]
	instrumentID := splitedSymbol[1]
	tickTableName := fmt.Sprintf("tick_%v_%v", exchangeID, instrumentID)
	p.Table(tickTableName).AutoMigrate(&model.Tick{})
	p.Table(tickTableName).Create(&model.Tick{Tick: tick})
	return nil
}
func (p *Database) SelectLastHistoryKlines(symbol string, duration time.Duration, count int) []oqgo.Kline {
	splitedSymbol := strings.Split(symbol, ".")
	exchangeID := splitedSymbol[0]
	instrumentID := splitedSymbol[1]
	klineTableName := fmt.Sprintf("kline_%v_%v", exchangeID, instrumentID)
	klines := make([]oqgo.Kline, 0)
	p.Table(klineTableName).Limit(count).Find(&klines)
	return klines
}
func New(options map[string]string) *Database {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	checkError(err)
	sqlDB, err := db.DB()
	sqlDB.SetMaxOpenConns(1)
	p := &Database{
		db:      db,
		options: options,
	}
	p.restore()
	// 退出时，Dump数据库到文件
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		for sig := range c {
			switch sig {
			case os.Interrupt:
				p.dump()
				os.Exit(0)
			}
		}
	}()
	return p
}
