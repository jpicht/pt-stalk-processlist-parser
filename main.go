package main

import (
	"bufio"
	"github.com/ftloc/exception"
	"github.com/jpicht/logger"
	"github.com/ruiaylin/sqlparser/parser"
	"os"
	"strconv"
	"strings"
	"time"
	"sync"
	"fmt"
)

type (
	Process struct {
		TS      time.Time
		Id      int64
		User    string
		Host    string
		Db      string
		Command string
		Time    int64
		State   string
		Info    string
	}
	Statement struct {
		String string
		Time   int64
	}
)

func main() {
	wg := sync.WaitGroup{}
	log := logger.NewStderrLogger()
	for it := range os.Args[1:] {
		f := os.Args[it+1]
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := load(log.WithData("file", f), f)
			for p := range c {
				if p.Info == "NULL" {
					continue
				}
				l := log.WithData("Statement", p.Info)
				l.Info("Parsing")
				parsed, err := parser.New().Parse(p.Info, "utf8", "utf8_general_ci")
				if err != nil {
					l.Warning(err.Error())

				}
				exception.ThrowOnError(err, err)
				fmt.Printf("%v", parsed)
				return
			}
		}()
	}
	wg.Wait()
}

func load(log logger.Logger, fn string) chan *Process {
	log.Info("Starting file")
	f, err := os.Open(fn)
	exception.ThrowOnError(err, err)

	c := make(chan *Process)

	go func() {
		var (
			sampleTime time.Time
			row        *Process
		)
		defer close(c)
		defer f.Close()
		defer log.Info("Done")
		sendRow := func() {
			if row != nil && row.Id > 0 {
				row.TS = sampleTime
				c <- row
			}
			row = &Process{}
		}
		r := bufio.NewReader(f)

		var last *string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				sendRow()
				return
			}

			switch line[0] {
			case 'T':
				parts := strings.Split(line, " ")
				ts, err := strconv.ParseFloat(parts[1], 64)
				exception.ThrowOnError(err, err)
				sampleTime = time.Unix(int64(ts), 0)
				log.WithData("timestamp", sampleTime).Trace("T")
			case '*':
				sendRow()
			default:
				if strings.Contains(line, ": ") {
					parts := strings.SplitN(strings.TrimRight(line, "\n"), ": ", 2)
					switch strings.TrimSpace(parts[0]) {
					case "Id":
						id, err := strconv.ParseInt(parts[1], 10, 64)
						exception.ThrowOnError(err, err)
						row.Id = id
						last = nil
					case "User":
						row.User = parts[1]
						last = &row.User
					case "Host":
						row.Host = parts[1]
						last = &row.Host
					case "db":
						row.Db = parts[1]
						last = &row.Db
					case "Command":
						row.Command = parts[1]
						last = &row.Command
					case "Time":
						if parts[1] != "NULL" {
							t, err := strconv.ParseInt(parts[1], 10, 64)
							exception.ThrowOnError(err, err)
							row.Time = t
						}
						last = nil
					case "State":
						row.State = parts[1]
						last = &row.State
					case "Info":
						row.Info = parts[1]
						last = &row.Info
					default:
						panic("Unknown query part: " + parts[0])
					}
				} else {
					if last == nil {
						panic("Continuation for non-string line: " + line)
					}
					*last += "\n" + strings.TrimRight(line, "\n")
				}
			}
		}
	}()

	return c
}
