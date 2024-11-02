package main

import (
	"context"
	"flag"

	"github.com/kiryu2k/data-loader/conf"
	"github.com/kiryu2k/data-loader/loader"
	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/dbrx"
	"github.com/txix-open/isp-kit/log"
)

func main() {
	logger, err := log.New(log.WithLevel(log.DebugLevel))
	if err != nil {
		panic(errors.WithMessage(err, "new logger"))
	}

	cfgPath := flag.String("config", "./conf/config.yml", "path to config in YAML format")
	srcPath := flag.String("source", "", "path to data source in JSON format")
	flag.Parse()

	ctx := context.Background()
	cfg, err := conf.LoadConfig(*cfgPath)
	if err != nil {
		logger.Fatal(ctx, errors.WithMessage(err, "load config"))
	}

	db := dbrx.New()
	defer func() {
		err := db.Close()
		if err != nil {
			logger.Fatal(ctx, errors.WithMessage(err, "close db conn"))
		}
	}()

	err = db.Upgrade(ctx, cfg.DataBase.Client)
	if err != nil {
		logger.Fatal(ctx, errors.WithMessage(err, "upgrade db conn with config"))
	}

	dataLoader, err := loader.New(db, logger, cfg.Loader, *srcPath, cfg.DataBase.Table)
	if err != nil {
		logger.Fatal(ctx, errors.WithMessage(err, "new data loader"))
	}

	err = dataLoader.LoadData(ctx)
	if err != nil {
		logger.Fatal(ctx, errors.WithMessage(err, "load data"))
	}
}
