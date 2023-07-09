package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	Logger = zap.NewNop().Sugar()
)

func InitLogger() {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	defer func() {
		cerr := logger.Sync()
		if cerr != nil {
			err = cerr
		}
	}()

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	zap.ReplaceGlobals(logger)

	Logger = logger.Sugar()
}

func CaptureOutput(funcToRun func()) (string, error) {
	var buffer bytes.Buffer

	oldLogger := Logger

	encoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	writer := bufio.NewWriter(&buffer)

	Logger = zap.New(zapcore.NewCore(encoder, zapcore.AddSync(writer), zapcore.DebugLevel)).Sugar()

	funcToRun()

	err := writer.Flush()
	if err != nil {
		return "can't flush logger writer", err
	}

	Logger = oldLogger

	return buffer.String(), nil
}

func CheckErr(e error) {
	if e != nil {
		Logger.Error(e.Error())
		fmt.Println(e.Error())
		os.Exit(1)
	}
}
