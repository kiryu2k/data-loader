package loader

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kiryu2k/data-loader/conf"
	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/db"
	"github.com/txix-open/isp-kit/db/query"
	"github.com/txix-open/isp-kit/json"
	"github.com/txix-open/isp-kit/log"
)

const (
	maxScannerBuf = 1 << 20 /* 1 MB */
)

type loader struct {
	scanner *bufio.Scanner
	db      db.DB
	logger  log.Logger
	wg      *sync.WaitGroup
	errChan chan error

	readCounter *atomic.Uint64
	loadCounter *atomic.Uint64

	table                    string
	batchSize                uint64
	logInterval              time.Duration
	shouldIgnoreInsertErrors bool
}

func New(db db.DB, logger log.Logger, cfg conf.Loader, filePath string, table string) (loader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return loader{}, errors.WithMessagef(err, "open file '%s'", filePath)
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, maxScannerBuf), maxScannerBuf)

	return loader{
		scanner:                  scanner,
		db:                       db,
		logger:                   logger,
		wg:                       new(sync.WaitGroup),
		errChan:                  make(chan error),
		readCounter:              new(atomic.Uint64),
		loadCounter:              new(atomic.Uint64),
		table:                    table,
		batchSize:                cfg.BatchSize,
		logInterval:              cfg.LogInterval,
		shouldIgnoreInsertErrors: cfg.ShouldIgnoreInsertErrors,
	}, nil
}

// nolint:funlen
func (l loader) LoadData(ctx context.Context) error {
	l.readCounter.Store(0)
	l.loadCounter.Store(0)

	if l.logInterval > 0 {
		l.logProgress(ctx)
	}

	var (
		columns           []string
		columnToIdx       = make(map[string]int)
		done              = make(chan struct{})
		builder           = query.New().Insert(l.table)
		shouldInitColumns = true
	)
	for l.scanner.Scan() {
		bytes := l.scanner.Bytes()
		var data map[string]any
		err := json.Unmarshal(bytes, &data)
		if err != nil {
			return errors.WithMessage(err, "json unmarshal")
		}

		values := make([]any, len(data))
		for k, v := range data {
			if shouldInitColumns {
				columns = append(columns, k)
				columnToIdx[k] = len(columns) - 1
			}
			idx := columnToIdx[k]
			values[idx] = v
		}
		if shouldInitColumns {
			builder = builder.Columns(columns...)
			shouldInitColumns = false
		}
		builder = builder.Values(values...)

		count := l.readCounter.Add(1)
		if count%l.batchSize == 0 {
			q, args, err := builder.ToSql()
			if err != nil {
				return errors.WithMessage(err, "build batch insert query")
			}
			l.execQueryAsync(ctx, q, args, len(columns))

			builder = query.New().
				Insert(l.table).
				Columns(columns...)
		}
	}

	if l.readCounter.Load()%l.batchSize != 0 {
		q, args, err := builder.ToSql()
		if err != nil {
			return errors.WithMessage(err, "build batch insert query")
		}
		l.execQueryAsync(ctx, q, args, len(columns))
	}

	go func() {
		l.wg.Wait()
		close(done)
	}()

	err := l.scanner.Err()
	if err != nil {
		return errors.WithMessage(err, "scan")
	}

	select {
	case err := <-l.errChan:
		return errors.WithMessage(err, "err chan")
	case <-done:
		return nil
	}
}

func (l loader) execQueryAsync(ctx context.Context, query string, args []any, columnCount int) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		_, err := l.db.Exec(ctx, query, args...)
		if err == nil {
			count := uint64(len(args) / columnCount) // nolint:gosec
			l.loadCounter.Add(count)
			return
		}

		if l.shouldIgnoreInsertErrors {
			l.logger.Warn(ctx, errors.WithMessage(err, "exec batch insert query"))
			return
		}
		l.errChan <- errors.WithMessage(err, "exec batch insert query")
	}()
}

func (l loader) logProgress(ctx context.Context) {
	ticker := time.NewTicker(l.logInterval)
	go func() {
		defer ticker.Stop()
		var (
			readCount = uint64(0)
			loadCount = uint64(0)
		)
		for range ticker.C {
			newReadCount := l.readCounter.Load()
			newLoadCount := l.loadCounter.Load()

			l.logger.Info(ctx, fmt.Sprintf(
				"read %d data rows in %s; loaded %d data rows in %s; %0.2f%% successful data loading",
				newReadCount-readCount,
				l.logInterval,
				newLoadCount-loadCount,
				l.logInterval,
				float64(newLoadCount)/float64(newReadCount)*100, // nolint:mnd
			))

			readCount = newReadCount
			loadCount = newLoadCount
		}
	}()
}
