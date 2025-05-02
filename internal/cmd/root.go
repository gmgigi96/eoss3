package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/gmgigi96/eoss3/meta"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	yaml "sigs.k8s.io/yaml/goyaml.v3"
)

var globalFlags = struct {
	Config string // Path of the config file to use
}{}

var rootCmd = &cobra.Command{
	Use:   "eoss3",
	Short: "A brief description of your application",
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&globalFlags.Config, "config", "c", "/etc/eoss3.yaml", "Path of the config file to use")

	rootCmd.AddCommand(createBucketCmd)
	createBucketCmd.Flags().IntVarP(&createBucketFlags.Owner, "owner", "o", 0, "User id of the owner of the bucket")
	createBucketCmd.Flags().StringVarP(&createBucketFlags.Name, "name", "n", "", "Name of the new bucket")
	createBucketCmd.Flags().StringVarP(&createBucketFlags.Path, "path", "p", "", "Path on EOS where the bucket is located")

	rootCmd.MarkFlagRequired("config")
	createBucketCmd.MarkFlagRequired("owner")
	createBucketCmd.MarkFlagRequired("name")
	createBucketCmd.MarkFlagRequired("path")
}

type Config struct {
	Endpoint   string         `mapstructure:"endpoint"`
	Buckets    map[string]any `mapstructure:"buckets"`
	RootAccess string         `mapstructure:"root_access"`
	RootSecret string         `mapstructure:"root_secret"`
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var createBucketFlags = struct {
	Owner int    // User id of the owner of the bucket
	Name  string // Name of the bucket
	Path  string // Path on EOS where the bucket is located
}{}

func getConfig() (*Config, error) {
	fmt.Println(globalFlags.Config)
	f, err := os.Open(globalFlags.Config)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var c map[string]any
	if err := yaml.NewDecoder(f).Decode(&c); err != nil {
		return nil, err
	}

	var cfg Config
	if err := mapstructure.Decode(c, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

var createBucketCmd = &cobra.Command{
	Use:   "create-bucket",
	Short: "Create an S3 bucket",
	Run: func(cmd *cobra.Command, args []string) {
		cfg, err := getConfig()
		cobra.CheckErr(err)

		buckets, err := meta.New(cfg.Buckets)
		cobra.CheckErr(err)

		bucket := meta.Bucket{
			Name:      createBucketFlags.Name,
			Path:      createBucketFlags.Path,
			CreatedAt: time.Now(),
		}
		err = buckets.CreateBucket(bucket)
		cobra.CheckErr(err)

		err = buckets.AssignBucket(createBucketFlags.Name, createBucketFlags.Owner)
		cobra.CheckErr(err)
	},
}
