package conf

import (
	"time"

	"github.com/pkg/errors"
	cfg "github.com/txix-open/isp-kit/config"
	"github.com/txix-open/isp-kit/dbx"
	"github.com/txix-open/isp-kit/validator"
)

type config struct {
	DataBase dataBase
	Loader   Loader
}

type dataBase struct {
	Client dbx.Config
	Table  string
}

type Loader struct {
	BatchSize                uint64 `validate:"required,min=100,max=10000"`
	LogInterval              time.Duration
	ShouldIgnoreInsertErrors bool
}

func LoadConfig(cfgPath string) (*config, error) {
	cfgReader, err := cfg.New(
		cfg.WithExtraSource(cfg.NewYamlConfig(cfgPath)),
		cfg.WithValidator(validator.Default),
	)
	if err != nil {
		return nil, errors.WithMessage(err, "cfg new")
	}
	config := new(config)
	if err := cfgReader.Read(config); err != nil {
		return nil, errors.WithMessage(err, "read config")
	}
	return config, nil
}
