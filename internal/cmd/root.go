package cmd

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"time"

	"github.com/gmgigi96/eoss3/eos"
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
	createBucketCmd.Flags().StringVarP(&createBucketFlags.Owner, "owner", "o", "", "User id of the owner of the bucket")
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
	GrpcURL    string         `mapstructure:"grpc_url"`
	HttpURL    string         `mapstructure:"http_url"`
	AuthKey    string         `mapstructure:"authkey"`
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var createBucketFlags = struct {
	Owner string // Username owner of the bucket
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

		client, err := eos.NewClient(eos.Config{
			GrpcURL: cfg.GrpcURL,
			HttpURL: cfg.HttpURL,
			AuthKey: cfg.AuthKey,
		})
		cobra.CheckErr(err)

		owner, err := user.Lookup(createBucketFlags.Owner)
		cobra.CheckErr(err)

		bucket := meta.Bucket{
			Name:      createBucketFlags.Name,
			Path:      createBucketFlags.Path,
			CreatedAt: time.Now(),
		}
		err = buckets.CreateBucket(bucket)
		cobra.CheckErr(err)

		uid, err := strconv.ParseUint(owner.Uid, 10, 64)
		cobra.CheckErr(err)

		gid, err := strconv.ParseUint(owner.Gid, 10, 64)
		cobra.CheckErr(err)

		err = buckets.AssignBucket(createBucketFlags.Name, int(uid))
		cobra.CheckErr(err)

		auth := eos.Auth{
			Uid: uid,
			Gid: gid,
		}
		err = client.Mkdir(cmd.Context(), auth, bucket.Path, 0755)
		cobra.CheckErr(err)
	},
}
