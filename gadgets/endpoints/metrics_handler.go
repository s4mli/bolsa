package endpoints

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/samwooo/bolsa/gadgets/config"
	"github.com/samwooo/bolsa/gadgets/logging"
	mgo "gopkg.in/mgo.v2"
)

type metricsHandler struct {
	mysqlInfo    *config.MySql
	mongoInfo    *config.Mongo
	postgresInfo *config.Postgres
	logger       logging.Logger
}

func (h *metricsHandler) ping() map[string]int {
	mongoStatus := 1
	mysqlStatus := 1
	postgresStatus := 1
	if err := h.pingMongo(h.mongoInfo.Url); err != nil {
		mongoStatus = 0
		h.logger.Infof("unable to connect mongodb ( %s ) : %s", h.mongoInfo.Url, err.Error())
	}
	if err := h.pingMysql(h.mysqlInfo.Host, h.mysqlInfo.Port, h.mysqlInfo.User,
		h.mysqlInfo.Password, h.mysqlInfo.DBName); err != nil {
		mysqlStatus = 0
		h.logger.Infof("unable to connect mysql ( %s ) : %s", h.mysqlInfo.Host, err.Error())
	}
	if err := h.pingPostgres(h.postgresInfo.Host, h.postgresInfo.Port, h.postgresInfo.User,
		h.postgresInfo.Password, h.postgresInfo.DBName); err != nil {
		postgresStatus = 0
		h.logger.Infof("unable to connect postgres ( %s ) : %s", h.postgresInfo.Host, err.Error())
	}
	return map[string]int{
		"mongo_status":    mongoStatus,
		"mysql_status":    mysqlStatus,
		"postgres_status": postgresStatus,
	}
}

func (h *metricsHandler) pingMongo(url string) error {
	s, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	defer s.Close()
	return nil
}

func (h *metricsHandler) pingMysql(host, port, user, password, db string) error {
	mySqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", user, password, host, port, db)
	if mysql, err := sql.Open("mysql", mySqlInfo); err != nil {
		return err
	} else {
		defer mysql.Close()
		return nil
	}
}

func (h *metricsHandler) pingPostgres(host, port, user, password, db string) error {
	pgInfo := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s",
		host, port, user, db, password)
	if pg, err := sql.Open("postgres", pgInfo); err != nil {
		return err
	} else {
		defer pg.Close()
		return nil
	}
}

func (h *metricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status := h.ping()
	var buffer bytes.Buffer
	for k, v := range status {
		buffer.WriteString(fmt.Sprintf("%s %d\n", k, v))
	}
	fmt.Fprint(w, string(buffer.Bytes()))
}

func NewMetricsHandler(mongoInfo *config.Mongo, mysqlInfo *config.MySql,
	postgresInfo *config.Postgres) *metricsHandler {

	return &metricsHandler{
		mongoInfo:    mongoInfo,
		mysqlInfo:    mysqlInfo,
		postgresInfo: postgresInfo,
		logger:       logging.GetLogger(" < metricsHandler > ")}
}
