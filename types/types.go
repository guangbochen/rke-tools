package types

import "time"

type BackupOpts struct {
	Enabled    bool
	Name       string
	Endpoint   string
	AccessKey  string
	SecretKey  string
	BucketName string
	Region     string
	Provider   string
}

// Backup defines the interface that each driver plugin should implement
type StorageProvider interface {
	// Upload uploads the backup file
	Upload(opts *BackupOpts, filePath string) error

	// Remove removes the backup file
	Remove(backupTime time.Time, retentionPeriod time.Duration, opts *BackupOpts) error

	// Download download the backup file from remote server
	Download(opts *BackupOpts, backupBaseDir string) error
}
