package main

import (
	"fmt"
	"strconv"
	"sync"

	"flag"
	"os"

	"github.com/samwooo/bolsa/gadgets/config"
	"github.com/samwooo/bolsa/gadgets/endpoints"
	"github.com/samwooo/bolsa/gadgets/job"
	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/piezas"
	"github.com/samwooo/bolsa/gadgets/queue"
	"github.com/samwooo/bolsa/gadgets/store"
	"github.com/samwooo/bolsa/gadgets/util"
)

func main() {
	if true {
		env := os.Getenv(fmt.Sprintf("%s_env", util.APP_NAME))
		if env == "" {
			env = "development"
		}
		var configFile string
		flag.StringVar(&configFile, "config", "./config.yaml", "configuration file to load")
		flag.Parse()
		var c *config.Config
		c, errs := config.LoadConfig(env, configFile)
		if len(errs) > 0 {
			os.Exit(1)
		}
		logging.DefaultLogger(fmt.Sprintf(" < %s > ", util.APP_NAME),
			logging.LogLevelFromString(c.Log.Level), 100)
		logging.GetLogger("").Debugf("running with %s", c)

		s := store.NewDBStore()
		q := queue.NewSqsQueue(&c.Queue)
		job.NewPublisherTelegraph(q, s, &c.Job, c.Queue.Wait).Start()

		httpServer := endpoints.NewServer(&c.Web)
		httpServer.RegisterResource(&job.Telegram{}, "/telegram")
		httpServer.RegisterHandler(endpoints.NewMetricsHandler(&c.Mongo, &c.MySql, &c.Postgres), "/metrics")
		httpServer.RegisterHandler(endpoints.NewTelegramHandler("/telegrams/", c.Web.Port, q, s),
			"/telegrams/")
		httpServer.Start()
	}

	if false { // waterfall
		piezas.NewWaterfall(piezas.Tasks{
			func(a ...int) (int, int, error) {
				total := 1
				count := 0
				for _, arg := range a {
					total = total * arg
					count = count + arg
				}
				return total, count, nil
			},
			func(a int, b int, e error) int {
				return a + b
			},
			func(a int) {
				fmt.Println("waterfall result :", a)
			}}, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	}
	if false { // map slice
		r := piezas.Map([]int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345,
			623, 5, 12, 3123, 123334, 23412, 21341234, 1234, 123, 412, 34, 123, 413, 45, 324,
			52, 4523, 45, 134, 523, 4312, 34, 1234, 12, 341, 234, 123, 1234, 1345, 34, 564, 325,
			5623463, 67324523, 423452345, 323452345, 22345234, 2345678963},
			func(k int64) int64 {
				return k * k
			}, true)
		fmt.Println(r)
	}
	if false { // map
		r := piezas.Map("8",
			func(k string) int {
				if r, err := strconv.Atoi(k); err != nil {
					return 0
				} else {
					return r * r
				}
			}, true)
		fmt.Println(r)
	}
	if false { // every
		r := piezas.Every(
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345, 623},
			func(k int64) bool { return k < 0 })
		fmt.Println(r)
	}
	if false { // each
		callCount := 0
		mutex := sync.Mutex{}
		input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345, 623}
		piezas.Each(input,
			func(k int64) {
				mutex.Lock()
				callCount++
				mutex.Unlock()
			})
		fmt.Println(callCount == len(input))
	}
	if false { // reduce
		input := []int64{1, 2, 3, 4, 5, 6, 7, 8, 99, 1112, 213, 123, 542, 45, 56, 345, 623}
		r := piezas.Reduce(input, 0,
			func(k int64, memo int) int {
				return memo + int(k)
			})
		fmt.Println(r)
	}
}
