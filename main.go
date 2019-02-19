package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	s3provider "github.com/rancher/rke-tools/s3provider/s3"
	"github.com/rancher/rke-tools/types"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
)

const (
	backupBaseDir = "/backup"
	backupRetries = 4
	ServerPort    = "2379"
)

var commonFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "endpoints",
		Usage: "Etcd endpoints",
		Value: "127.0.0.1:2379",
	},
	cli.BoolFlag{
		Name:   "debug",
		Usage:  "Verbose logging information for debugging purposes",
		EnvVar: "RANCHER_DEBUG",
	},
	cli.StringFlag{
		Name:  "name",
		Usage: "Backup name to take once",
	},
	cli.StringFlag{
		Name:   "cacert",
		Usage:  "Etcd CA client certificate path",
		EnvVar: "ETCD_CACERT",
	},
	cli.StringFlag{
		Name:   "cert",
		Usage:  "Etcd client certificate path",
		EnvVar: "ETCD_CERT",
	},
	cli.StringFlag{
		Name:   "key",
		Usage:  "Etcd client key path",
		EnvVar: "ETCD_KEY",
	},
	cli.StringFlag{
		Name:   "local-endpoint",
		Usage:  "Local backup download endpoint",
		EnvVar: "LOCAL_ENDPOINT",
	},
	cli.BoolFlag{
		Name:   "s3-backup",
		Usage:  "Backup etcd sanpshot to your s3 server, set true or false",
		EnvVar: "S3_BACKUP",
	},
	cli.StringFlag{
		Name:   "s3-provider",
		Usage:  "Specify etcd s3 storage provider",
		EnvVar: "S3_PROVIDER",
		Value:  "s3",
	},
	cli.StringFlag{
		Name:   "s3-endpoint",
		Usage:  "Specify s3 endpoint address",
		EnvVar: "S3_ENDPOINT",
	},
	cli.StringFlag{
		Name:   "s3-accessKey",
		Usage:  "Specify s3 access key",
		EnvVar: "S3_ACCESS_KEY",
	},
	cli.StringFlag{
		Name:   "s3-secretKey",
		Usage:  "Specify s3 secret key",
		EnvVar: "S3_SECRET_KEY",
	},
	cli.StringFlag{
		Name:   "s3-bucketName",
		Usage:  "Specify s3 bucket name",
		EnvVar: "S3_BUCKET_NAME",
	},
	cli.StringFlag{
		Name:   "s3-region",
		Usage:  "Specify s3 bucket region",
		EnvVar: "S3_BUCKET_REGION",
	},
}

func init() {
	log.SetOutput(os.Stderr)
}

func main() {
	err := os.Setenv("ETCDCTL_API", "3")
	if err != nil {
		log.Fatal(err)
	}

	app := cli.NewApp()
	app.Name = "Etcd Wrapper"
	app.Usage = "Utility services for Etcd cluster backup"
	app.Commands = []cli.Command{
		RollingBackupCommand(),
	}
	err = app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func RollingBackupCommand() cli.Command {
	snapshotFlags := []cli.Flag{
		cli.DurationFlag{
			Name:  "creation",
			Usage: "Create backups after this time interval in minutes",
			Value: 5 * time.Minute,
		},
		cli.DurationFlag{
			Name:  "retention",
			Usage: "Retain backups within this time interval in hours",
			Value: 24 * time.Hour,
		},
		cli.BoolFlag{
			Name:  "once",
			Usage: "Take backup only once",
		},
	}

	snapshotFlags = append(snapshotFlags, commonFlags...)

	return cli.Command{
		Name:  "etcd-backup",
		Usage: "Perform etcd backup tools",
		Subcommands: []cli.Command{
			{
				Name:   "save",
				Usage:  "Take snapshot on all etcd hosts and backup to s3 compatible storage",
				Flags:  snapshotFlags,
				Action: RollingBackupAction,
			},
			{
				Name:   "download",
				Usage:  "Download specified snapshot from s3 compatible storage or another local endpoint",
				Flags:  commonFlags,
				Action: DownloadBackupAction,
			},
			{
				Name:  "serve",
				Usage: "Provide HTTPS endpoint to pull local snapshot",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "name",
						Usage: "Backup name to take once",
					},
					cli.StringFlag{
						Name:   "cacert",
						Usage:  "Etcd CA client certificate path",
						EnvVar: "ETCD_CACERT",
					},
					cli.StringFlag{
						Name:   "cert",
						Usage:  "Etcd client certificate path",
						EnvVar: "ETCD_CERT",
					},
					cli.StringFlag{
						Name:   "key",
						Usage:  "Etcd client key path",
						EnvVar: "ETCD_KEY",
					},
				},
				Action: ServeBackupAction,
			},
		},
	}
}

func SetLoggingLevel(debug bool) {
	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
}

func RollingBackupAction(c *cli.Context) error {
	SetLoggingLevel(c.Bool("debug"))

	creationPeriod := c.Duration("creation")
	retentionPeriod := c.Duration("retention")
	etcdCert := c.String("cert")
	etcdCACert := c.String("cacert")
	etcdKey := c.String("key")
	etcdEndpoints := c.String("endpoints")
	if len(etcdCert) == 0 || len(etcdCACert) == 0 || len(etcdKey) == 0 {
		log.WithFields(log.Fields{
			"etcdCert":   etcdCert,
			"etcdCACert": etcdCACert,
			"etcdKey":    etcdKey,
		}).Errorf("Failed to find etcd cert or key paths")
		return fmt.Errorf("Failed to find etcd cert or key paths")
	}
	log.WithFields(log.Fields{
		"creation":  creationPeriod,
		"retention": retentionPeriod,
	}).Info("Initializing Rolling Backups")

	s3Backup := c.Bool("s3-backup")
	bc := &types.BackupOpts{
		Enabled:    s3Backup,
		Endpoint:   c.String("s3-endpoint"),
		AccessKey:  c.String("s3-accessKey"),
		SecretKey:  c.String("s3-secretKey"),
		BucketName: c.String("s3-bucketName"),
		Region:     c.String("s3-region"),
		Provider:   c.String("s3-provider"),
		Name:       c.String("name"),
	}

	if c.Bool("once") {
		if len(bc.Name) == 0 {
			bc.Name = fmt.Sprintf("%s_etcd", time.Now().Format(time.RFC3339))
		}
		if err := CreateBackup(etcdCACert, etcdCert, etcdKey, etcdEndpoints, bc); err != nil {
			return err
		}
		prefix := getNamePrefix(bc.Name)
		// we only clean named backups if we have a retention period and a cluster name prefix
		if retentionPeriod != 0 && len(prefix) != 0 {
			if err := DeleteNamedBackups(retentionPeriod, prefix); err != nil {
				return err
			}
		}
		return nil
	}

	// routine retention and removes
	timeoutCtx, cancel := context.WithCancel(context.Background())
	ticker := TickerContext(timeoutCtx, creationPeriod)
	defer cancel()
	for {
		select {
		case backupTime := <-ticker:
			bc.Name = fmt.Sprintf("%s_etcd", backupTime.Format(time.RFC3339))
			CreateBackup(etcdCACert, etcdCert, etcdKey, etcdEndpoints, bc)
			DeleteBackups(backupTime, retentionPeriod)
			if s3Backup {
				DeleteRemoteBackups(backupTime, retentionPeriod, bc)
			}
		}
	}
}

func CreateBackup(etcdCACert, etcdCert, etcdKey, endpoints string, bc *types.BackupOpts) error {
	failureInterval := 15 * time.Second
	backupDir := fmt.Sprintf("%s/%s", backupBaseDir, bc.Name)
	for retries := 0; retries <= backupRetries; retries++ {
		if retries > 0 {
			time.Sleep(failureInterval)
		}
		// check if the cluster is healthy
		cmd := exec.Command("etcdctl",
			fmt.Sprintf("--endpoints=[%s]", endpoints),
			"--cacert="+etcdCACert,
			"--cert="+etcdCert,
			"--key="+etcdKey,
			"endpoint", "health")
		data, err := cmd.CombinedOutput()

		if strings.Contains(string(data), "unhealthy") {
			log.WithFields(log.Fields{
				"error": err,
				"data":  string(data),
			}).Warn("Checking member health failed from etcd member")
			continue
		}

		cmd = exec.Command("etcdctl",
			fmt.Sprintf("--endpoints=[%s]", endpoints),
			"--cacert="+etcdCACert,
			"--cert="+etcdCert,
			"--key="+etcdKey,
			"snapshot", "save", backupDir)

		startTime := time.Now()
		data, err = cmd.CombinedOutput()
		endTime := time.Now()

		if err != nil {
			log.WithFields(log.Fields{
				"attempt": retries + 1,
				"error":   err,
				"data":    string(data),
			}).Warn("Backup failed")
			continue
		}
		log.WithFields(log.Fields{
			"name":    bc.Name,
			"runtime": endTime.Sub(startTime),
		}).Info("Created backup")

		if bc.Enabled {
			switch bc.Provider {
			case "s3":
				s3sp, err := s3provider.NewStorageProvider(bc)
				if err != nil {
					return err
				}
				return s3sp.Upload(bc, backupDir)
			case "oss":
				s3sp, err := s3provider.NewStorageProvider(bc)
				if err != nil {
					return err
				}
				return s3sp.Upload(bc, backupDir)
			case "gcs":
				log.Info("gcs storage provider is not implemented")
				return nil
			default:
				return fmt.Errorf("invalid name of storage provider: %s", bc.Provider)
			}
		}
		break
	}
	return nil
}

func DeleteBackups(backupTime time.Time, retentionPeriod time.Duration) {
	files, err := ioutil.ReadDir(backupBaseDir)
	if err != nil {
		log.WithFields(log.Fields{
			"dir":   backupBaseDir,
			"error": err,
		}).Warn("Can't read backup directory")
	}

	cutoffTime := backupTime.Add(retentionPeriod * -1)

	for _, file := range files {
		if file.IsDir() {
			log.WithFields(log.Fields{
				"name": file.Name(),
			}).Warn("Ignored directory, expecting file")
			continue
		}

		backupTime, err2 := time.Parse(time.RFC3339, strings.Split(file.Name(), "_")[0])
		if err2 != nil {
			log.WithFields(log.Fields{
				"name":  file.Name(),
				"error": err2,
			}).Warn("Couldn't parse backup")

		} else if backupTime.Before(cutoffTime) {
			_ = DeleteBackup(file)
		}
	}
}

func DeleteBackup(file os.FileInfo) error {
	toDelete := fmt.Sprintf("%s/%s", backupBaseDir, file.Name())

	cmd := exec.Command("rm", "-r", toDelete)

	startTime := time.Now()
	err2 := cmd.Run()
	endTime := time.Now()

	if err2 != nil {
		log.WithFields(log.Fields{
			"name":  file.Name(),
			"error": err2,
		}).Warn("Delete backup failed")
		return err2
	}
	log.WithFields(log.Fields{
		"name":    file.Name(),
		"runtime": endTime.Sub(startTime),
	}).Info("Deleted backup")
	return nil
}

func DeleteRemoteBackups(backupTime time.Time, retentionPeriod time.Duration, bc *types.BackupOpts) {
	log.WithFields(log.Fields{
		"retention": retentionPeriod,
	}).Info("Invoking delete s3 backup files")
	switch bc.Provider {
	case "s3":
		s3sp, err := s3provider.NewStorageProvider(bc)
		if err != nil {
			return
		}
		s3sp.Remove(backupTime, retentionPeriod, bc)
		return
	case "oss":
		s3sp, err := s3provider.NewStorageProvider(bc)
		if err != nil {
			return
		}
		s3sp.Remove(backupTime, retentionPeriod, bc)
		return
	case "gcs":
		log.Info("gcs storage provider is not implemented")
		return
	default:
		log.Errorf("Invalid s3 service provider %s", bc.Provider)
	}
}

func DownloadBackupAction(c *cli.Context) error {
	log.Info("Initializing Download Backups")
	SetLoggingLevel(c.Bool("debug"))
	if c.Bool("s3-backup") {
		return DownloadS3Backup(c)
	}
	return DownloadLocalBackup(c)
}

func DownloadS3Backup(c *cli.Context) error {
	bc := &types.BackupOpts{
		Name:       c.String("name"),
		Endpoint:   c.String("s3-endpoint"),
		AccessKey:  c.String("s3-accessKey"),
		SecretKey:  c.String("s3-secretKey"),
		BucketName: c.String("s3-bucketName"),
		Region:     c.String("s3-region"),
		Provider:   c.String("s3-provider"),
	}

	if len(bc.Name) == 0 {
		return fmt.Errorf("empty file name")
	}

	log.Infof("Invoking downloading backup files: %s", bc.Name)
	switch bc.Provider {
	case "s3":
		s3sp, err := s3provider.NewStorageProvider(bc)
		if err != nil {
			return err
		}
		return s3sp.Download(bc, backupBaseDir)
	case "oss":
		s3sp, err := s3provider.NewStorageProvider(bc)
		if err != nil {
			return err
		}
		return s3sp.Download(bc, backupBaseDir)
	case "gcs":
		log.Info("gcs storage provider is not implemented")
		return nil
	default:
		return fmt.Errorf("invalid name of storage provider: %s", bc.Provider)
	}
	return nil
}

func DownloadLocalBackup(c *cli.Context) error {
	snapshot := path.Base(c.String("name"))
	endpoint := c.String("local-endpoint")
	if snapshot == "." || snapshot == "/" {
		return fmt.Errorf("snapshot name is required")
	}
	if len(endpoint) == 0 {
		return fmt.Errorf("local-endpoint is required")
	}
	certs, err := getCertsFromCli(c)
	if err != nil {
		return err
	}
	tlsConfig, err := setupTLSConfig(certs, false)
	if err != nil {
		return err
	}
	client := http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}

	snapshotFile, err := os.Create(fmt.Sprintf("%s/%s", backupBaseDir, snapshot))
	if err != nil {
		return err
	}
	defer snapshotFile.Close()
	log.Infof("Invoking downloading backup files: %s", snapshot)
	resp, err := client.Get(fmt.Sprintf("https://%s:%s/%s", endpoint, ServerPort, snapshot))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if _, err := io.Copy(snapshotFile, resp.Body); err != nil {
		return err
	}
	log.Infof("Successfully download %s from %s ", snapshot, endpoint)
	return nil
}

func DeleteNamedBackups(retentionPeriod time.Duration, prefix string) error {
	files, err := ioutil.ReadDir(backupBaseDir)
	if err != nil {
		log.WithFields(log.Fields{
			"dir":   backupBaseDir,
			"error": err,
		}).Warn("Can't read backup directory")
		return err
	}
	cutoffTime := time.Now().Add(retentionPeriod * -1)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), prefix) && file.ModTime().Before(cutoffTime) {
			if err = DeleteBackup(file); err != nil {
				return err
			}
		}
	}
	return nil
}

func getNamePrefix(name string) string {
	re := regexp.MustCompile("^c-[a-z0-9].*?-")
	m := re.FindStringSubmatch(name)
	if len(m) == 0 {
		return ""
	}
	return m[0]
}

func ServeBackupAction(c *cli.Context) error {
	snapshot := path.Base(c.String("name"))

	if snapshot == "." || snapshot == "/" {
		return fmt.Errorf("snapshot name is required")
	}
	certs, err := getCertsFromCli(c)
	if err != nil {
		return err
	}
	tlsConfig, err := setupTLSConfig(certs, true)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr:      fmt.Sprintf("0.0.0.0:%s", ServerPort),
		TLSConfig: tlsConfig,
	}

	http.HandleFunc(fmt.Sprintf("/%s", snapshot), func(response http.ResponseWriter, request *http.Request) {
		http.ServeFile(response, request, fmt.Sprintf("%s/%s", backupBaseDir, snapshot))
	})
	return httpServer.ListenAndServeTLS(certs["cert"], certs["key"])
}

func getCertsFromCli(c *cli.Context) (map[string]string, error) {
	caCert := c.String("cacert")
	cert := c.String("cert")
	key := c.String("key")
	if len(cert) == 0 || len(caCert) == 0 || len(key) == 0 {
		return nil, fmt.Errorf("cacert, cert and key are required")
	}

	return map[string]string{"cacert": caCert, "cert": cert, "key": key}, nil
}

func setupTLSConfig(certs map[string]string, isServer bool) (*tls.Config, error) {
	caCertPem, err := ioutil.ReadFile(certs["cacert"])
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{}
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCertPem)
	if isServer {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConfig.ClientCAs = certPool
		tlsConfig.MinVersion = tls.VersionTLS12
	} else { // client config
		x509Pair, err := tls.LoadX509KeyPair(certs["cert"], certs["key"])
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{x509Pair}
		tlsConfig.RootCAs = certPool
		// This is to avoid IP SAN errors.
		tlsConfig.InsecureSkipVerify = true
	}

	tlsConfig.BuildNameToCertificate()
	return tlsConfig, nil
}

// TickerContext implements context time ticker func
func TickerContext(ctx context.Context, duration time.Duration) <-chan time.Time {
	ticker := time.NewTicker(duration)
	go func() {
		<-ctx.Done()
		ticker.Stop()
	}()
	return ticker.C
}
