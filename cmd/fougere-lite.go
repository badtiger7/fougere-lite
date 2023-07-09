// ©Copyright 2022 Metrio
package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"metrio.net/fougere-lite/internal/client"
	"metrio.net/fougere-lite/internal/utils"
)

var (
	version string
	root    = &cobra.Command{
		Use:     "fougere-lite",
		Short:   "DevOps tool to rule them all",
		Version: version,
	}
	cfgFile string
)

func main() {
	utils.InitLogger()

	initCobra()

	err := root.Execute()
	if err != nil {
		utils.Logger.Error(err.Error())
		os.Exit(1)
	}
}

func initCobra() {
	cobra.OnInitialize(initConfig)
	root.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file")
	root.AddCommand(client.NewClientsCommand())
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		viper.AutomaticEnv()
		err := viper.ReadInConfig()
		utils.CheckErr(err)
		utils.Logger.Infof("Using config file: %s", viper.ConfigFileUsed())
	} else {
		utils.Logger.Infof("Config file not used")
	}
}
